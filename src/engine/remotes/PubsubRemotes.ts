import { MeldLocal, MeldRemotes, OperationMessage, Revup, Snapshot } from '../index';
import {
  BehaviorSubject, defer, EMPTY, firstValueFrom, from, identity, Observable, Observer, of,
  onErrorResumeNext, Subject as Source, Subscription
} from 'rxjs';
import { TreeClock } from '../clocks';
import { generate as uuid } from 'short-uuid';
import { Request, Response } from '../ControlMessage';
import { completed, Future, MsgPack, Stopwatch, toJSON } from '../util';
import {
  concatMap, delay, finalize, first, map, materialize, reduce, tap, timeout, toArray
} from 'rxjs/operators';
import { MeldError, MeldErrorStatus } from '../MeldError';
import { AbstractMeld } from '../AbstractMeld';
import { MeldConfig, shortId } from '../../index';
import { JsonNotification, NotifyParams, ReplyParams, SendParams } from './PubsubParams';

/**
 * A sub-publisher, used to temporarily address unicast messages to one peer clone. Sub-publishers
 * are created on the demand of the {@link PubsubRemotes} base class, and are closed when no longer
 * required. During its lifetime, the sub-publisher will only address one other clone.
 * @see PubsubRemotes.sender
 * @see PubsubRemotes.replier
 * @see PubsubRemotes.notifier
 */
export interface SubPub {
  /**
   * Context-dependent identity of this sub-publisher, see usages for more information.
   */
  readonly id: string;
  /**
   * Unicast a message to the peer addressed by this sub-publisher.
   * @param msg the message payload to send
   */
  publish(msg: Buffer): Promise<unknown>;
  /**
   * Optional tidy-up of this sub-publisher.
   */
  close?(): void;
}

/** A m-ld ack is a reply to a reply with a null body */
const ACK = null;
type ACK = typeof ACK;

/**
 * An abstract implementation of {@link MeldRemotes}, providing a common base for publish/subscribe
 * message transports able to broadcast moderate-sized binary payloads between nodes.
 *
 * In addition to message broadcast, the subclass must also be able to provide:
 * - A set of peer clone identities which are currently reachable ('present')
 * - Unicasting of a single message or a stream of messages to _one_ other identified peer clone
 *
 * These functions may make use of the publish/subscribe service if it has such capability, or by
 * any other means, as required.
 *
 * A subclass must:
 * 1. Implement the abstract methods to broadcast, unicast, and obtain peer presence
 * 1. Call the protected methods prefixed with "`on`" (`onConnect`, `onDisconnect`, `onNotify`,
 * `onPresenceChange`, `onOperation`, `onReply`, `onSent`) when the relevant transport state
 * change happens, or a message arrives
 */
export abstract class PubsubRemotes extends AbstractMeld implements MeldRemotes {
  private readonly localClone = new BehaviorSubject<MeldLocal | null>(null);
  private readonly replyResolvers: {
    [messageId: string]: {
      resolve: (res: Response | ACK) => void,
      readyToAck?: PromiseLike<void>
    }
  } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [subPubId: string]: Observer<Buffer> } = {};
  private readonly sendTimeout: number;
  private readonly activity: Set<PromiseLike<void>> = new Set;
  /**
   * This is separate to liveness because decided liveness requires presence, which is discovered
   * after connection. Connected is not equivalent to decided-liveness.
   */
  private connected = new BehaviorSubject<boolean>(false);

  protected constructor(config: MeldConfig) {
    super(config);
    this.sendTimeout = config.networkTimeout ?? 5000;
  }

  setLocal(clone: MeldLocal | null): void {
    if (clone == null) {
      if (this.clone != null)
        this.cloneLive(false)
          .then(() => this.clone = null)
          .catch(this.warnError);
    } else if (this.clone == null) {
      this.clone = clone;
      // Start sending updates from the local clone to the remotes
      clone.operations.subscribe({
        next: async msg => {
          // If we are not connected, we just ignore updates.
          // They will be replayed from the clone's journal on re-connection.
          if (this.connected.value) {
            try {
              // Operation received from the local clone. Relay to the domain
              await this.publishOperation(msg.encode());
              // When done, mark the message as delivered
              msg.delivered.resolve();
            } catch (err) {
              // Failed to send an update while (probably) connected
              this.log.warn(err);
              // Operation delivery is guaranteed at-least-once. So, if it
              // fails, something catastrophic must have happened. Signal
              // failure of this service and allow the clone to deal with it.
              this.closeSafely(err);
            }
          }
        },
        // Local m-ld clone has stopped. It will no longer accept messages.
        complete: () => this.closeSafely(),
        // Local m-ld clone has stopped unexpectedly.
        // The application will already know, so just shut down gracefully.
        error: err => this.closeSafely(err)
      });
      // When the clone comes live, join the presence on this domain if we can
      clone.live.subscribe(live => {
        if (live != null)
          this.cloneLive(live).catch(this.warnError);
      });
    } else if (clone != this.clone) {
      throw new Error(`${this.id}: Local clone cannot change`);
    }
  }

  /**
   * Broadcasts a operation message to all clones on the domain, not including the local one
   * (messages **must not** be echoed).
   */
  protected abstract publishOperation(msg: Buffer): Promise<unknown>;

  /**
   * Retrieve a set of peer clone identities which are 'live' for recovery collaboration. A
   * returned identity may be used to establish a unicast channel via {@link sender} or
   * {@link notifier}.
   *
   * Note that while the return is an observable, the full set should be available quickly,
   * comfortably with a single network timeout.
   * @see MeldLocal.id
   * @see Meld.live
   */
  protected abstract present(): Observable<string>;

  /**
   * Indicate to other peer clones that the local clone is 'live' for recovery collaboration, or
   * has gone away.
   * @param present whether the local clone is available to other clones for recovery collaboration
   * @see Meld.live
   */
  protected abstract setPresent(present: boolean): Promise<unknown>;

  /**
   * Allocate a new {@link SubPub sub-publisher} to address another clone with a stream of
   * messages, via a notification 'channel'. Multiple channels may be allocated at the same time to
   * the same recipient.
   *
   * The {@link SubPub.id} of the returned object **must** equal the {@link NotifyParams.channelId}
   * of the parameter. TODO: refactor, make this warning unnecessary
   * @param params details of the sender, intended recipient, and channel
   */
  protected abstract notifier(params: NotifyParams): SubPub | Promise<SubPub>;

  /**
   * Allocate a new {@link SubPub sub-publisher} to address another clone with a single message,
   * which may incur a {@link replier reply}.
   *
   * The {@link SubPub.id} of the returned object **must** equal the {@link SendParams.toId}
   * of the parameter. TODO: refactor, make this warning unnecessary
   * @param params details of the sender, intended recipient and message identity
   */
  protected abstract sender(params: SendParams): SubPub | Promise<SubPub>;

  /**
   * Allocate a new {@link SubPub sub-publisher} to reply to a {@link sender sent} message.
   *
   * The {@link SubPub.id} of the returned object **must** equal the {@link SendParams.toId}
   * of the parameter. TODO: refactor, make this warning unnecessary
   * @param params details of the replier, sender, sent message identity and reply message identity
   */
  protected abstract replier(params: ReplyParams): SubPub | Promise<SubPub>;

  async close(err?: any) {
    if (err)
      this.log.info('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');
    // This finalises the #updates, thereby notifying the clone
    super.close(err);
    // Wait until all activities have finalised
    await Promise.all(this.activity); // TODO unit test this
  }

  private closeSafely(err?: any) {
    this.close(err).catch(this.warnError);
  }

  async newClock(): Promise<TreeClock> {
    const sw = new Stopwatch('clock', shortId(4));
    const { res } = await this.send<Response.NewClock>(new Request.NewClock, { sw });
    sw.stop();
    return res.clock;
  }

  async snapshot(): Promise<Snapshot> {
    const readyToAck = new Future;
    const sw = new Stopwatch('snapshot', shortId(4));
    const { res, fromId } = await this.send<Response.Snapshot>(
      new Request.Snapshot, { readyToAck, sw });
    sw.next('consume');
    // Subscribe in parallel (subscription can be slow)
    const [data, updates] = await Promise.all([
      this.consume(fromId, res.dataAddress, MsgPack.decode, 'failIfSlow'),
      this.consume(fromId, res.updatesAddress, OperationMessage.decode)
    ]);
    sw.stop();
    return {
      gwc: res.gwc, data: defer(() => {
        // Ack the response to start the streams
        readyToAck.resolve();
        return data;
      }), updates
    };
  }

  async revupFrom(time: TreeClock): Promise<Revup | undefined> {
    const readyToAck = new Future;
    const sw = new Stopwatch('revup', shortId(4));
    const { res, fromId: from } = await this.send<Response.Revup>(new Request.Revup(time), {
      // Try everyone until we find someone who can revup
      readyToAck, check: res => res.gwc != null, sw
    });
    if (res.gwc != null) {
      sw.next('consume');
      const updates = await this.consume(
        from, res.updatesAddress, OperationMessage.decode, 'failIfSlow');
      sw.stop();
      return {
        gwc: res.gwc, updates: defer(() => {
          // Ack the response to start the streams
          readyToAck.resolve();
          return updates;
        })
      };
    } // else return undefined
    sw.stop();
  }

  /**
   * Call from the subclass when the transport is connected. Generally this indicates that the
   * network is available and the publish/subscribe service has been reached.
   */
  protected async onConnect() {
    this.connected.next(true);
    if (this.clone != null && this.clone.live.value === true)
      return this.cloneLive(true);
  }

  /**
   * Call from the subclass when the transport is disconnected. Generally this indicates that the
   * network is unavailable or the publish/subscribe service is not reachable.
   */
  protected onDisconnect() {
    this.connected.next(false);
    this.setLive(null);
  }

  /**
   * Call from the subclass when the set of present peer clone identities has changed, for example
   * because a peer has become present. This **must** also be called after connection, when the
   * presence set becomes available and {@link present} is callable without error.
   */
  protected onPresenceChange() {
    // Don't process a presence change until connected
    this.connected.pipe(first(identity), tap(() => {
      // If there is more than just me present, we are live
      this.present()
        .pipe(reduce((live, id) => live || id !== this.id, false))
        .subscribe(live => this.setLive(live));
    })).subscribe();
  }

  /**
   * Call from the subclass when an operation message has arrived.
   * @param payload the operation message payload
   */
  protected onOperation(payload: Buffer) {
    const update = OperationMessage.decode(payload);
    if (update)
      this.nextOperation(update);
    else
      // This is extremely bad - may indicate a bad actor
      this.closeSafely(new MeldError('Bad update'));
  }

  /**
   * Call from the subclass when a message has been sent intended for the local clone.
   * @param payload the sent message payload
   * @param sentParams details of the sender, recipient (always the local clone
   *   {@link MeldLocal.id identity}, and message identity
   * @see sender
   */
  protected async onSent(payload: Buffer, sentParams: SendParams) {
    // Ignore control messages before we have a clone
    const replyRejected = (err: any) => {
      this.reply(sentParams, new Response.Rejected(asMeldErrorStatus(err)))
        .catch(this.warnError);
      throw err;
    };
    if (this.clone) {
      // Keep track of long-running activity so that we can shut down cleanly
      const active = this.active();
      const sw = new Stopwatch('reply', shortId(4));
      try {
        const req = Request.fromJson(MsgPack.decode(payload));
        if (req instanceof Request.NewClock) {
          sw.next('clock');
          const clock = await this.clone.newClock().catch(replyRejected);
          sw.lap.next('send');
          await this.replyClock(sentParams, clock);
        } else if (req instanceof Request.Snapshot) {
          sw.next('snapshot');
          const snapshot = await this.clone.snapshot().catch(replyRejected);
          sw.lap.next('send');
          await this.replySnapshot(sentParams, snapshot);
        } else if (req instanceof Request.Revup) {
          sw.next('revup');
          const revup = await this.clone.revupFrom(req.time).catch(replyRejected);
          sw.lap.next('send');
          await this.replyRevup(sentParams, revup);
        }
      } catch (err) {
        // Rejection will already have been caught with replyRejected
        this.log.warn(err);
      } finally {
        sw.stop();
        active.resolve();
      }
    }
  }

  /**
   * Call from the subclass when a reply arrives to a message {@link sender sent} by us.
   * @param payload the reply message payload
   * @param replyParams details of the sender, recipient (always the local clone
   *   {@link MeldLocal.id identity}, original sent message identity and reply message identity
   * @see replier
   */
  protected async onReply(payload: Buffer, replyParams: ReplyParams) {
    if (replyParams.sentMessageId in this.replyResolvers) {
      try {
        const { resolve, readyToAck } = this.replyResolvers[replyParams.sentMessageId];
        const json = MsgPack.decode(payload);
        resolve(json != null ? Response.fromJson(json) : null);
        if (readyToAck != null) {
          await readyToAck;
          await this.reply(replyParams, ACK);
        }
      } catch (err) {
        this.log.warn(err);
      }
    }
  }

  /**
   * Call from the subclass when a single message arrives from a notification 'channel'.
   * @param channelId the notification channel identifier
   * @param payload the notified message payload
   * @see notifier
   */
  protected onNotify(channelId: string, payload: Buffer) {
    if (channelId in this.consuming) {
      const json = MsgPack.decode(payload);
      this.log.debug(`Notified ${Object.keys(json)[0]} on channel ${channelId}`);
      if (json.next)
        this.consuming[channelId].next(json.next);
      else if (json.complete)
        this.consuming[channelId].complete();
      else if (json.error)
        this.consuming[channelId].error(MeldError.from(json.error));
    }
  }

  private get clone() {
    return this.localClone.value;
  }

  private set clone(clone: MeldLocal | null) {
    this.localClone.next(clone);
  }

  private async cloneLive(live: boolean): Promise<unknown> {
    if (this.connected.value)
      return this.setPresent(live);
  }

  private async send<T extends Response>(
    request: Request,
    { readyToAck, check, sw }: {
      readyToAck?: PromiseLike<void>;
      check?: (res: T) => boolean;
      sw: Stopwatch;
    },
    tried: { [address: string]: Promise<{ res: T, fromId: string }> } = {},
    messageId = uuid()): Promise<{ res: T, fromId: string }> {
    sw.next('sender');
    const sender = await this.nextSender(messageId);
    if (sender == null) {
      throw new MeldError('No visible clones',
        `No-one present on ${this.domain} to send message to`);
    } else if (sender.id in tried) {
      // If we have already tried this address, we've tried everyone; return
      // whatever the last response was.
      return tried[sender.id];
    }
    this.log.debug('Sending request', messageId, request, sender.id);
    sw.next('send');
    const sent = sender
      .publish(MsgPack.encode(request.toJSON()))
      .finally(() => sender.close?.());
    tried[sender.id] = this.getResponse<T>(sent, messageId, { readyToAck })
      .then(res => ({ res, fromId: sender.id }));
    // If the publish fails, don't keep trying other addresses
    await sent;
    // If the caller doesn't like this response, try again
    return tried[sender.id].then(rtn => check == null || check(rtn.res) ? rtn :
        this.send(request, { readyToAck, check, sw }, tried, messageId),
      () => this.send(request, { readyToAck, check, sw }, tried, messageId));
  }

  private getResponse<T extends Response | ACK>(sent: Promise<unknown>, messageId: string,
    { readyToAck, allowTimeFor }: {
      readyToAck?: PromiseLike<void>,
      allowTimeFor?: Promise<unknown>
    }): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      // Three possible outcomes:
      // 1. Response times out
      const timer = this.timeout(() => {
        delete this.replyResolvers[messageId];
        this.log.debug(`Message ${messageId} timed out.`);
        reject(new Error('Send timeout exceeded.'));
      }, allowTimeFor);
      // 2. Send fails - abandon the response
      sent.catch(err => {
        delete this.replyResolvers[messageId];
        timer.unsubscribe();
        reject(err);
      });
      // 3. Response received
      this.replyResolvers[messageId] = {
        resolve: res => {
          delete this.replyResolvers[messageId];
          timer.unsubscribe();
          if (res instanceof Response.Rejected)
            reject(new MeldError(res.status));
          else
            resolve(res as T);
        },
        readyToAck
      };
    });
  }

  protected timeout(cb: () => void,
    allowTimeFor = Promise.resolve<unknown>(null)): Subscription {
    return from(allowTimeFor ?? of()).pipe(delay(this.sendTimeout)).subscribe(cb);
  }

  private active(): Future {
    const active = new Future();
    const done = active.then(() => {
      this.activity.delete(done);
    });
    this.activity.add(done);
    return active;
  }

  private async replyClock(sentParams: SendParams, clock: TreeClock) {
    await this.reply(sentParams, new Response.NewClock(clock));
  }

  private async replySnapshot(sentParams: SendParams, snapshot: Snapshot): Promise<void> {
    const { gwc: gwc, data, updates } = snapshot;
    const dataAddress = uuid(), updatesAddress = uuid();
    // Send the reply in parallel with establishing notifiers
    const replyId = uuid();
    const replied = this.reply(sentParams,
      new Response.Snapshot(gwc, dataAddress, updatesAddress), replyId);
    // Allow time for the notifiers to resolve while waiting for a reply
    const [dataNotifier, updatesNotifier] = await this.getAck(replied, replyId, Promise.all([
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: dataAddress }),
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress })
    ]));
    // Ack has been sent, start streaming the data and updates concurrently
    await Promise.all([
      this.produce(data, dataNotifier, MsgPack.encode, 'snapshot'),
      this.produce(updates, updatesNotifier, msg => msg.encode(), 'updates')
    ]);
  }

  private async replyRevup(sentParams: SendParams, revup: Revup | undefined) {
    if (revup) {
      const updatesAddress = uuid();
      const replyId = uuid();
      const replied = this.reply(sentParams,
        new Response.Revup(revup.gwc, updatesAddress), replyId);
      const notifier = await this.getAck(replied, replyId, Promise.resolve(this.notifier({
        toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress
      })));
      // Ack has been sent, start streaming the updates
      await this.produce(revup.updates, notifier, msg => msg.encode(), 'updates');
    } else if (this.clone) {
      await this.reply(sentParams, new Response.Revup(null, this.clone.id));
    }
  }

  private async consume<T>(fromId: string, channelId: string,
    datumFromPayload: (payload: Buffer) => T,
    failIfSlow?: 'failIfSlow'): Promise<Observable<T>> {
    const notifier = await this.notifier({ fromId, toId: this.id, channelId });
    const src = this.consuming[channelId] = new Source;
    const complete = () => {
      // TODO unit test this
      notifier.close?.();
      delete this.consuming[channelId];
    };
    onErrorResumeNext(src, EMPTY).subscribe({ complete });
    const consumed = src.pipe(map(datumFromPayload));
    // Rev-up and snapshot update channels are expected to be very fast, as they
    // are streamed directly from the dataset. So if there is a pause in the
    // delivery this probably indicates a failure e.g. the collaborator is dead.
    // TODO unit test this
    return failIfSlow ? consumed.pipe(timeout(this.sendTimeout)) : consumed;
  }

  private async produce<T>(data: Observable<T>, notifier: SubPub,
    datumToPayload: (datum: T) => Buffer, type: string) {
    const notifyError = (error: any) => {
      this.log.warn('Notifying error on', notifier.id, error);
      return notify({ error: toJSON(error) });
    };
    const notifyComplete = () => {
      this.log.debug(`Completed production of ${type} on ${notifier.id}`);
      return notify({ complete: true });
    };
    const notify: (notification: JsonNotification) => Promise<unknown> = notification =>
      notifier.publish(MsgPack.encode(notification))
        // If notifications fail due to channel death, the recipient will find
        // out from the network so here we make best efforts to notify an error
        // and then give up.
        .catch(err => notifyError(err));
    return completed(data.pipe(
      map(datumToPayload),
      materialize(),
      concatMap(notification => {
        switch (notification.kind) {
          case 'N':
            return notify({ next: notification.value });
          case 'E':
            return notifyError(notification.error);
          case 'C':
            return notifyComplete();
        }
      }),
      finalize(() => notifier.close?.())));
  }

  private async reply(
    { fromId: toId, messageId: sentMessageId }: SendParams,
    res: Response | ACK, messageId = uuid()): Promise<unknown> {
    const replier = await this.replier({ fromId: this.id, toId, messageId, sentMessageId });
    this.log.debug('Replying response', messageId, 'to', sentMessageId, res, replier.id);
    return replier
      .publish(MsgPack.encode(res == null ? null : res.toJSON()))
      .finally(() => replier.close?.());
  }

  private async getAck<T>(
    replied: Promise<unknown>, messageId: string, expectAfter: Promise<T>): Promise<T> {
    return this.getResponse<ACK>(replied, messageId, { allowTimeFor: expectAfter })
      .then(() => expectAfter); // This just gets the return value
  }

  private async nextSender(messageId: string): Promise<SubPub | null> {
    const present = await firstValueFrom(this.present().pipe(toArray()));
    if (present.every(id => this.recentlySentTo.has(id)))
      this.recentlySentTo.clear();

    this.recentlySentTo.add(this.id);
    const toId = present.filter(id => !this.recentlySentTo.has(id))[0];
    if (toId != null) {
      this.recentlySentTo.add(toId);
      return this.sender({ fromId: this.id, toId, messageId });
    }
    return null;
  }
}

function asMeldErrorStatus(err: any): MeldErrorStatus {
  return err instanceof MeldError ? err.status : MeldErrorStatus['Request rejected'];
}