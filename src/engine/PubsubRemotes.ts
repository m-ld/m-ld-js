import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, Revup } from '.';
import {
  Observable, Subject as Source, BehaviorSubject, identity, defer,
  Observer, Subscription, from, of, EMPTY, onErrorResumeNext
} from 'rxjs';
import { TreeClock } from './clocks';
import { generate as uuid } from 'short-uuid';
import { Response, Request } from './ControlMessage';
import { MsgPack, Future, toJson, Stopwatch } from './util';
import {
  finalize, reduce, toArray, first, concatMap, materialize, timeout, delay, map
} from 'rxjs/operators';
import { MeldError, MeldErrorStatus } from './MeldError';
import { AbstractMeld } from './AbstractMeld';
import { MeldConfig, shortId } from '..';
import { Triple } from './quads';

// @see org.m_ld.json.MeldJacksonModule.NotificationDeserializer
export interface JsonNotification {
  next?: any;
  complete?: true;
  error?: any;
}

/** Parameters for sending, receiving and notifying a peer */
export interface PeerParams {
  toId: string;
  fromId: string;
}

/** Parameters for sending a single message */
export interface SendParams extends PeerParams {
  messageId: string;
}

/** Parameters for replying to a message */
export interface ReplyParams extends SendParams {
  sentMessageId: string;
}

/** Parameters for multiple messages on some channel */
export interface NotifyParams extends PeerParams {
  channelId: string;
}

export interface SubPub {
  readonly id: string;
  publish(msg: Buffer): Promise<unknown>;
  close(): void;
}

/** A m-ld ack is a reply to a reply with a null body */
type ACK = null;
const ACK = null;

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
   * This is separate to liveness because decided liveness requires presence,
   * which happens after connection. Connected =/=> decided liveness.
   */
  private connected = new BehaviorSubject<boolean>(false);

  constructor(config: MeldConfig) {
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
      clone.updates.subscribe({
        next: async msg => {
          // If we are not connected, we just ignore updates.
          // They will be replayed from the clone's journal on re-connection.
          if (this.connected.value) {
            try {
              // Delta received from the local clone. Relay to the domain
              await this.publishDelta(msg.encode());
              // When done, mark the message as delivered
              msg.delivered.resolve();
            } catch (err) {
              // Failed to send an update while (probably) connected
              this.log.warn(err);
              // Delta delivery is guaranteed at-least-once. So, if it fails,
              // something catastrophic must have happened. Signal failure of
              // this service and allow the clone to deal with it.
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

  protected abstract setPresent(present: boolean): Promise<unknown>;

  /**
   * Publishes a delta message with at-least-once guaranteed delivery.
   */
  protected abstract publishDelta(msg: Buffer): Promise<unknown>;

  protected abstract present(): Observable<string>;

  protected abstract notifier(params: NotifyParams): SubPub | Promise<SubPub>;

  protected abstract sender(params: SendParams): SubPub | Promise<SubPub>;

  protected abstract replier(params: ReplyParams): SubPub | Promise<SubPub>;

  async close(err?: any) {
    if (err)
      this.log.info('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');
    // This finalises the #updates, thereby notifying the clone
    super.close(err);
    // Wait until the clone has closed
    await this.localClone.pipe(first(null)).toPromise();
    // Wait until all activity have finalised
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
    const [quads, updates] = await Promise.all([
      this.consume(fromId, res.quadsAddress, this.triplesFromBuffer, 'failIfSlow'),
      this.consume(fromId, res.updatesAddress, DeltaMessage.decode)
    ]);
    sw.stop();
    return {
      lastTime: res.lastTime, quads: defer(() => {
        // Ack the response to start the streams
        readyToAck.resolve();
        return quads;
      }), updates
    };
  }

  private triplesFromBuffer = (payload: Buffer) =>
    this.requireClone().encoding.triplesFromJson(MsgPack.decode(payload))

  async revupFrom(time: TreeClock): Promise<Revup | undefined> {
    const readyToAck = new Future;
    const sw = new Stopwatch('revup', shortId(4));
    const { res, fromId: from } = await this.send<Response.Revup>(new Request.Revup(time), {
      // Try everyone until we find someone who can revup
      readyToAck, check: res => res.lastTime != null, sw
    });
    if (res.lastTime != null) {
      sw.next('consume');
      const updates = await this.consume(
        from, res.updatesAddress, DeltaMessage.decode, 'failIfSlow');
      sw.stop();
      return {
        lastTime: res.lastTime, updates: defer(() => {
          // Ack the response to start the streams
          readyToAck.resolve();
          return updates;
        })
      };
    } // else return undefined
    sw.stop();
  }

  protected async onConnect() {
    this.connected.next(true);
    if (this.clone != null && this.clone.live.value === true)
      return this.cloneLive(true);
  }

  protected onDisconnect() {
    this.connected.next(false);
    this.setLive(null);
  }

  protected onPresenceChange() {
    // Don't process a presence change until connected
    this.connected.pipe(first(identity)).toPromise().then(() => {
      // If there is more than just me present, we are live
      this.present()
        .pipe(reduce((live, id) => live || id !== this.id, false))
        .subscribe(live => this.setLive(live));
    });
  }

  protected onRemoteUpdate(payload: Buffer) {
    const update = DeltaMessage.decode(payload);
    if (update)
      this.nextUpdate(update);
    else
      // This is extremely bad - may indicate a bad actor
      this.closeSafely(new MeldError('Bad update'));
  }

  protected async onSent(payload: Buffer, sentParams: SendParams) {
    // Ignore control messages before we have a clone
    const replyRejected = (err: any) => {
      this.reply(sentParams, new Response.Rejected(asMeldErrorStatus(err)))
        .catch(this.warnError);
      throw err;
    }
    if (this.clone) {
      // Keep track of long-running activity so that we can shut down cleanly
      const active = this.active();
      const sw = new Stopwatch('reply', shortId(4));
      try {
        const req = Request.fromJson(MsgPack.decode(payload));
        if (req instanceof Request.NewClock) {
          sw.next('clock');
          const clock = await this.clone.newClock().catch(replyRejected);
          sw.lap.next('send')
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

  protected onNotify(channelId: string, payload: Buffer) {
    if (channelId in this.consuming) {
      const json = MsgPack.decode(payload);
      this.log.debug(`Notified on channel ${channelId}:`, json);
      if (json.next)
        this.consuming[channelId].next(json.next);
      else if (json.complete)
        this.consuming[channelId].complete();
      else if (json.error)
        this.consuming[channelId].error(MeldError.from(json.error));
    }
  }

  private requireClone() {
    const clone = this.clone;
    if (clone == null)
      throw new Error('No local clone');
    return clone;
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
      .publish(MsgPack.encode(request.toJson()))
      .finally(() => sender.close());
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
    const done = active.then(() => { this.activity.delete(done); });
    this.activity.add(done);
    return active;
  }

  private async replyClock(sentParams: SendParams, clock: TreeClock) {
    await this.reply(sentParams, new Response.NewClock(clock));
  }

  private async replySnapshot(sentParams: SendParams, snapshot: Snapshot): Promise<void> {
    const { lastTime, quads, updates } = snapshot;
    const quadsAddress = uuid(), updatesAddress = uuid();
    // Send the reply in parallel with establishing notifiers
    const replyId = uuid();
    const replied = this.reply(sentParams,
      new Response.Snapshot(lastTime, quadsAddress, updatesAddress), replyId);
    // Allow time for the notifiers to resolve while waiting for a reply
    const [quadsNotifier, updatesNotifier] = await this.getAck(replied, replyId, Promise.all([
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: quadsAddress }),
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress })
    ]));
    // Ack has been sent, start streaming the data and updates concurrently
    await Promise.all([
      this.produce(quads, quadsNotifier, this.bufferFromTriples, 'snapshot'),
      this.produce(updates, updatesNotifier, msg => msg.encode(), 'updates')
    ]);
  }

  private bufferFromTriples = (triples: Triple[]) =>
    MsgPack.encode(this.requireClone().encoding.jsonFromTriples(triples));

  private async replyRevup(sentParams: SendParams, revup: Revup | undefined) {
    if (revup) {
      const updatesAddress = uuid();
      const replyId = uuid();
      const replied = this.reply(sentParams,
        new Response.Revup(revup.lastTime, updatesAddress), replyId);
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
      notifier.close();
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
      return notify({ error: toJson(error) });
    }
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
    return data.pipe(
      map(datumToPayload),
      materialize(),
      concatMap(notification => {
        switch (notification.kind) {
          case 'N': return notify({ next: notification.value });
          case 'E': return notifyError(notification.error);
          case 'C': return notifyComplete();
        }
      }),
      finalize(() => notifier.close()))
      .toPromise();
  }

  private async reply(
    { fromId: toId, messageId: sentMessageId }: SendParams,
    res: Response | ACK, messageId = uuid()): Promise<unknown> {
    const replier = await this.replier({ fromId: this.id, toId, messageId, sentMessageId });
    this.log.debug('Replying response', messageId, 'to', sentMessageId, res, replier.id);
    return replier
      .publish(MsgPack.encode(res == null ? null : res.toJson()))
      .finally(() => replier.close());
  }

  private async getAck<T>(
    replied: Promise<unknown>, messageId: string, expectAfter: Promise<T>): Promise<T> {
    return this.getResponse<ACK>(replied, messageId, { allowTimeFor: expectAfter })
      .then(() => expectAfter); // This just gets the return value
  }

  private async nextSender(messageId: string): Promise<SubPub | null> {
    const present = await this.present().pipe(toArray()).toPromise();
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