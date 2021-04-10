import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, Revup } from '.';
import { Observable, Subject as Source, BehaviorSubject, identity, defer, Observer } from 'rxjs';
import { TreeClock } from './clocks';
import { generate as uuid } from 'short-uuid';
import { Response, Request } from './ControlMessage';
import { MsgPack, Future, toJson, Stopwatch } from './util';
import {
  finalize, mergeMap, reduce, toArray, first, concatMap, materialize, timeout, share
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

export interface SubPubsub extends SubPub {
  subscribe(): Promise<unknown>;
}

export abstract class PubsubRemotes extends AbstractMeld implements MeldRemotes {
  private readonly localClone = new BehaviorSubject<MeldLocal | null>(null);
  private readonly replyResolvers: {
    [messageId: string]: [(res: Response | null) => void, PromiseLike<void> | null]
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

  protected abstract notifier(params: NotifyParams): SubPubsub | Promise<SubPubsub>;

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
    const ack = new Future;
    const sw = new Stopwatch('snapshot', shortId(4));
    const { res, from } = await this.send<Response.Snapshot>(new Request.Snapshot, { ack, sw });
    sw.next('consume');
    // Subscribe in parallel (subscription can be slow)
    const [quads, updates] = await Promise.all([
      this.consume(from, res.quadsAddress, this.triplesFromBuffer, 'failIfSlow'),
      this.consume(from, res.updatesAddress, DeltaMessage.decode)
    ]);
    sw.stop();
    return {
      lastTime: res.lastTime, quads: defer(() => {
        // Ack the response to start the streams
        ack.resolve();
        return quads;
      }), updates
    };
  }

  private triplesFromBuffer = (payload: Buffer) =>
    this.requireClone().encoding.triplesFromJson(MsgPack.decode(payload))

  async revupFrom(time: TreeClock): Promise<Revup | undefined> {
    const ack = new Future;
    const sw = new Stopwatch('revup', shortId(4));
    const { res, from } = await this.send<Response.Revup>(new Request.Revup(time), {
      // Try everyone until we find someone who can revup
      ack, check: res => res.lastTime != null, sw
    });
    if (res.lastTime != null) {
      sw.next('consume');
      const updates = await this.consume(
        from, res.updatesAddress, DeltaMessage.decode, 'failIfSlow');
      sw.stop();
      return {
        lastTime: res.lastTime, updates: defer(() => {
          // Ack the response to start the streams
          ack.resolve();
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
        const [resolve, ack] = this.replyResolvers[replyParams.sentMessageId];
        const json = MsgPack.decode(payload);
        resolve(json != null ? Response.fromJson(json) : null);
        if (ack) { // A m-ld ack is a reply to a reply with a null body
          await ack;
          await this.reply(replyParams, null);
        }
      } catch (err) {
        this.log.warn(err);
      }
    }
  }

  protected onNotify(subPubId: string, payload: Buffer) {
    if (subPubId in this.consuming) {
      const json = MsgPack.decode(payload);
      if (json.next)
        this.consuming[subPubId].next(json.next);
      else if (json.complete)
        this.consuming[subPubId].complete();
      else if (json.error)
        this.consuming[subPubId].error(MeldError.from(json.error));
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
    { ack, check, sw }: {
      ack?: PromiseLike<void>;
      check?: (res: T) => boolean;
      sw: Stopwatch;
    },
    tried: { [address: string]: PromiseLike<{ res: T, from: string }> } = {},
    messageId: string = uuid()): Promise<{ res: T, from: string }> {
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
    tried[sender.id] = this.getResponse<T>(sent, messageId, ack ?? null)
      .then(res => ({ res, from: sender.id }));
    // If the publish fails, don't keep trying other addresses
    await sent;
    // If the caller doesn't like this response, try again
    return tried[sender.id].then(rtn => check == null || check(rtn.res) ? rtn :
      this.send(request, { ack, check, sw }, tried, messageId),
      () => this.send(request, { ack, check, sw }, tried, messageId));
  }

  private getResponse<T extends Response | null>(
    sent: Promise<unknown>,
    messageId: string,
    ack: PromiseLike<void> | null): PromiseLike<T> {
    const response = new Future<T>();
    // Three possible outcomes:
    // 1. Response times out
    const timer = this.timeout(() => {
      delete this.replyResolvers[messageId];
      this.log.debug(`Message ${messageId} timed out.`)
      response.reject(new Error('Send timeout exceeded.'));
    });
    // 2. Send fails - abandon the response
    sent.catch(err => {
      delete this.replyResolvers[messageId];
      clearTimeout(timer);
      response.reject(err);
    });
    // 3. Response received
    this.replyResolvers[messageId] = [res => {
      delete this.replyResolvers[messageId];
      clearTimeout(timer);
      if (res instanceof Response.Rejected)
        response.reject(new MeldError(res.status));
      else
        response.resolve(res as T);
    }, ack];
    return response;
  }

  protected timeout(callback: (...args: any[]) => void) {
    return setTimeout(callback, this.sendTimeout);
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
    const [, quadsNotifier, updatesNotifier] = await Promise.all([
      this.reply(sentParams, new Response.Snapshot(
        lastTime, quadsAddress, updatesAddress), 'expectAck'),
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: quadsAddress }),
      this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress })
    ]);
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
      const [, notifier] = await Promise.all([
        this.reply(sentParams, new Response.Revup(revup.lastTime, updatesAddress), 'expectAck'),
        this.notifier({ toId: sentParams.fromId, fromId: this.id, channelId: updatesAddress })
      ]);
      // Ack has been sent, start streaming the updates
      await this.produce(revup.updates, notifier, msg => msg.encode(), 'updates');
    } else if (this.clone) {
      await this.reply(sentParams, new Response.Revup(null, this.clone.id));
    }
  }

  private async consume<T>(
    fromId: string,
    channelId: string,
    map: (payload: Buffer) => T | Promise<T>,
    failIfSlow?: 'failIfSlow'): Promise<Observable<T>> {
    const notifier = await this.notifier({ toId: this.id, fromId, channelId });
    const src = this.consuming[notifier.id] = new Source;
    await notifier.subscribe();
    const consumed = src.pipe(
      // Unsubscribe from the sub-channel when a complete or error arrives.
      finalize(() => {
        notifier.close();
        delete this.consuming[notifier.id];
      }),
      mergeMap(async payload => map(payload)));
    // Rev-up and snapshot update channels are expected to be very fast, as they
    // are streamed directly from the dataset. So if there is a pause in the
    // delivery this probably indicates a failure e.g. the collaborator is dead.
    // TODO unit test this
    return (failIfSlow ? consumed.pipe(timeout(this.sendTimeout)) : consumed)
      // Share so that the finalize on the Source is only done once
      .pipe(share()); // TODO unit test this
  }

  private async produce<T>(data: Observable<T>, notifier: SubPub,
    datumToPayload: (datum: T) => Promise<Buffer> | Buffer, type: string) {
    const notifyError = (error: any, final = true) => {
      this.log.warn('Notifying error on', notifier.id, error);
      return notify({ error: toJson(error) }, final);
    }
    const notifyComplete = () => {
      this.log.debug('Completed production of', type);
      return notify({ complete: true }, true);
    };
    const notify = async (notification: JsonNotification, final = false) => {
      try {
        await notifier.publish(MsgPack.encode(notification));
      } catch (error) {
        // If notifications fail due to channel death, the recipient will find
        // out from the network so here we make best efforts to notify an error
        // and then give up.
        await notifyError(error, !final); // Don't re-close if already final
      } finally {
        if (final)
          notifier.close();
      }
    }
    return data.pipe(
      // concatMap guarantees delivery ordering despite toJson promise ordering
      concatMap(async datum => await datumToPayload(datum)),
      materialize(),
      mergeMap(notification => notification.do(
        next => notify({ next }),
        error => notifyError(error),
        () => notifyComplete())))
      .toPromise();
  }

  private async reply(
    { fromId: toId, messageId: sentMessageId }: SendParams, res: Response | null, expectAck?: 'expectAck') {
    const messageId = uuid();
    const replier = await this.replier({ fromId: this.id, toId, messageId, sentMessageId });
    this.log.debug('Replying response', messageId, 'to', sentMessageId, res, replier.id);
    const replied = replier
      .publish(MsgPack.encode(res == null ? null : res.toJson()))
      .finally(() => replier.close());
    if (expectAck)
      return this.getResponse<null>(replied, messageId, null);
    else
      return replied;
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