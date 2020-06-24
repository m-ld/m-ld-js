import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, UUID } from './m-ld';
import { Observable, Subject as Source, BehaviorSubject, identity } from 'rxjs';
import { TreeClock } from './clocks';
import { generate as uuid } from 'short-uuid';
import { Response, Request } from './m-ld/ControlMessage';
import { Future, toJson } from './util';
import { finalize, flatMap, reduce, toArray, first, concatMap, materialize, timeout } from 'rxjs/operators';
import { toMeldJson, fromMeldJson } from './m-ld/MeldJson';
import { MeldError, MeldErrorStatus } from './m-ld/MeldError';
import { AbstractMeld, isOnline } from './AbstractMeld';
import { MeldConfig } from '.';

// @see org.m_ld.json.MeldJacksonModule.NotificationDeserializer
export interface JsonNotification {
  next?: any;
  complete?: true;
  error?: any;
}

export interface DirectParams {
  toId: string;
  fromId: string;
  messageId: string;
}

export interface SendParams extends DirectParams {
  address: string[];
}

export interface ReplyParams extends DirectParams {
  sentMessageId: string;
}

export interface SubPub<T> {
  readonly id: string;
  publish(msg: T): Promise<unknown>;
}

export interface SubPubsub<T> extends SubPub<T> {
  subscribe(): Promise<unknown>;
  unsubscribe(): Promise<unknown>;
}

export abstract class PubsubRemotes extends AbstractMeld implements MeldRemotes {
  private readonly domain: string;
  private readonly localClone = new BehaviorSubject<MeldLocal | null>(null);
  private readonly replyResolvers: {
    [messageId: string]: [(res: Response | null) => void, PromiseLike<void> | null]
  } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [address: string]: Source<any> } = {};
  private readonly sendTimeout: number;
  private readonly activity: Set<Promise<void>> = new Set;

  constructor(config: MeldConfig) {
    super(config['@id'], config.logLevel ?? 'info');
    this.domain = config['@domain'];
    this.sendTimeout = config.networkTimeout || 2000;
  }

  setLocal(clone: MeldLocal | null): void {
    if (clone == null) {
      if (this.clone != null)
        this.clonePresent(false)
          .then(() => this.clone = null)
          .catch(this.warnError);
    } else if (this.clone == null) {
      this.clone = clone;
      // Start sending updates from the local clone to the remotes
      clone.updates.subscribe({
        next: async msg => {
          // If we are not online, we just ignore updates.
          // They will be replayed from the clone's journal on re-connection.
          if (this.isOnline()) {
            try {
              // Delta received from the local clone. Relay to the domain
              await this.publishDelta(msg);
              // When done, mark the message as delivered
              msg.delivered.resolve();
            } catch (err) {
              // Failed to send an update while (probably) connected
              this.log.warn(err);
              // We can't allow gaps, so ensure a reconnect
              this.reconnect();
            }
          }
        },
        // Local m-ld clone has stopped. It will no longer accept messages.
        complete: () => this.close().catch(this.warnError),
        // Local m-ld clone has stopped unexpectedly.
        // The application will already know, so just shut down gracefully.
        error: err => this.close(err).catch(this.warnError)
      });
      // When the clone comes online, join the presence on this domain if we can
      clone.online.subscribe(online => {
        if (online != null)
          this.clonePresent(online).catch(this.warnError);
      });
    } else if (clone != this.clone) {
      throw new Error(`${this.id}: Local clone cannot change`);
    }
  }

  /**
   * Force a re-connect to the pubsub layer. This must be sufficiently violent
   * to lead to an unset and reset of the online flag.
   */
  protected abstract reconnect(): void;

  protected abstract clonePresent(online: boolean): Promise<unknown>;

  protected abstract publishDelta(msg: DeltaMessage): Promise<unknown>;

  protected abstract present(): Observable<string>;

  protected abstract notifier(id: string): SubPubsub<JsonNotification>;

  protected abstract sender(toId: string, messageId: string): SubPub<Request>;

  protected abstract replier(messageId: string, toId: string, sentMessageId: string): SubPub<Response | null>;

  protected abstract isEcho(json: any): boolean;

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

  async newClock(): Promise<TreeClock> {
    return (await this.send<Response.NewClock>(new Request.NewClock)).clock;
  }

  async snapshot(): Promise<Snapshot> {
    const ack = new Future;
    const res = await this.send<Response.Snapshot>(new Request.Snapshot, { ack });
    const snapshot: Snapshot = {
      lastTime: res.lastTime,
      quads: await this.consume(res.quadsAddress, fromMeldJson, 'failIfSlow'),
      tids: await this.consume<UUID[]>(res.tidsAddress, identity, 'failIfSlow'),
      lastHash: res.lastHash,
      updates: await this.consume(res.updatesAddress, deltaFromJson)
    };
    // Ack the response to start the streams
    ack.resolve();
    return snapshot;
  }

  async revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined> {
    const ack = new Future;
    const res = await this.send<Response.Revup>(new Request.Revup(time), {
      // Try everyone until we find someone who can revup
      ack, check: res => res.canRevup
    });
    if (res.canRevup) {
      const updates = await this.consume(
        res.updatesAddress, deltaFromJson, 'failIfSlow');
      // Ack the response to start the streams
      ack.resolve();
      return updates;
    } // else return undefined
  }

  protected async onConnect() {
    if (this.clone != null && await isOnline(this.clone) === true)
      this.clonePresent(true).catch(this.warnError);
  }

  protected onPresenceChange() {
    // If there is more than just me present, we are online
    this.present()
      .pipe(reduce((online, id) => online || id !== this.id, false))
      .subscribe(online => this.setOnline(online));
  }

  protected onRemoteUpdate(json: any) {
    // Ignore echoed updates
    if (!this.isEcho(json)) {
      const update = DeltaMessage.fromJson(json);
      if (update)
        this.nextUpdate(update);
      else
        // This is extremely bad - may indicate a bad actor
        this.close(new MeldError('Bad Update'));
    }
  }

  protected async onSent(json: any, sentParams: SendParams) {
    // Ignore control messages before we have a clone
    const replyRejected = (err: any) => {
      this.reply(sentParams, new Response.Rejected(asMeldErrorStatus(err)))
        .catch(this.warnError);
      throw err;
    }
    if (this.clone) {
      // Keep track of long-running activity so that we can shut down cleanly
      const active = this.active();
      try {
        const req = Request.fromJson(json);
        if (req instanceof Request.NewClock) {
          const clock = await this.clone.newClock().catch(replyRejected);
          await this.replyClock(sentParams, clock);
        } else if (req instanceof Request.Snapshot) {
          const snapshot = await this.clone.snapshot().catch(replyRejected);
          await this.replySnapshot(sentParams, snapshot);
        } else if (req instanceof Request.Revup) {
          const revup = await this.clone.revupFrom(req.time).catch(replyRejected);
          await this.replyRevup(sentParams, revup);
        }
      } catch (err) {
        // Rejection will already have been caught with replyRejected
        this.log.warn(err);
      } finally {
        active.resolve();
      }
    }
  }

  protected async onReply(json: any, replyParams: ReplyParams) {
    if (replyParams.sentMessageId in this.replyResolvers) {
      try {
        const [resolve, ack] = this.replyResolvers[replyParams.sentMessageId];
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

  protected onNotify(id: string, json: any) {
    if (id in this.consuming) {
      if (json.next)
        this.consuming[id].next(json.next);
      else if (json.complete)
        this.consuming[id].complete();
      else if (json.error)
        this.consuming[id].error(MeldError.from(json.error));
    }
  }

  private get clone() {
    return this.localClone.value;
  }

  private set clone(clone: MeldLocal | null) {
    this.localClone.next(clone);
  }

  private async send<T extends Response>(
    request: Request,
    { ack, check }: {
      ack?: PromiseLike<void>;
      check?: (res: T) => boolean;
    } = {},
    tried: { [address: string]: PromiseLike<T> } = {},
    messageId: string = uuid()): Promise<T> {
    const sender = await this.nextSender(messageId);
    if (sender == null) {
      throw new MeldError('No visible clones',
        `No-one present on ${this.domain} to send message to`);
    } else if (sender.id in tried) {
      // If we have already tried this address, we've tried everyone
      return tried[sender.id];
    }
    this.log.debug('Sending request', messageId, request, sender.id);
    // If the publish fails, don't keep trying other addresses
    await sender.publish(request);
    tried[sender.id] = this.getResponse<T>(messageId, ack ?? null);
    // If the caller doesn't like this response, try again
    return tried[sender.id].then(res => check == null || check(res) ? res :
      this.send(request, { ack, check }, tried, messageId),
      () => this.send(request, { ack, check }, tried, messageId));
  }

  private getResponse<T extends Response | null>(
    messageId: string, ack: PromiseLike<void> | null): PromiseLike<T> {
    const response = new Future<T>();
    const timer = setTimeout(() => {
      delete this.replyResolvers[messageId];
      this.log.debug(`Message ${messageId} timed out.`)
      response.reject(new Error('Send timeout exceeded.'));
    }, this.sendTimeout);
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
    const { lastTime, lastHash, quads, tids, updates } = snapshot;
    const quadsAddress = uuid(), tidsAddress = uuid(), updatesAddress = uuid();
    await this.reply(sentParams, new Response.Snapshot(
      lastTime, quadsAddress, tidsAddress, lastHash, updatesAddress
    ), 'expectAck');
    // Ack has been sent, start streaming the data and updates concurrently
    await Promise.all([
      this.produce(quads, quadsAddress, toMeldJson, 'snapshot'),
      this.produce(tids, tidsAddress, identity, 'tids'),
      this.produce(updates, updatesAddress, jsonFromDelta, 'updates')
    ]);
  }

  private async replyRevup(sentParams: SendParams,
    revup: Observable<DeltaMessage> | undefined) {
    if (revup) {
      const updatesAddress = uuid();
      await this.reply(sentParams, new Response.Revup(true, updatesAddress), 'expectAck');
      // Ack has been sent, start streaming the updates
      await this.produce(revup, updatesAddress, jsonFromDelta, 'updates');
    } else if (this.clone) {
      await this.reply(sentParams, new Response.Revup(false, this.clone.id));
    }
  }

  private async consume<T>(subAddress: string, map: (json: any) => T | Promise<T>, failIfSlow?: 'failIfSlow'): Promise<Observable<T>> {
    const notifier = this.notifier(subAddress);
    const src = this.consuming[notifier.id] = new Source;
    await notifier.subscribe();
    const consumed = src.pipe(
      // Unsubscribe from the sub-channel when a complete or error arrives
      finalize(() => {
        notifier.unsubscribe();
        delete this.consuming[notifier.id];
      }),
      flatMap(async json => map(json)));
    // Rev-up and snapshot update channels are expected to be very fast, as they
    // are streamed directly from the dataset. So if there is a pause in the
    // delivery this probably indicates a failure e.g. the collaborator is dead.
    // TODO unit test this
    return failIfSlow ? consumed.pipe(timeout(this.sendTimeout)) : consumed;
  }

  private produce<T>(data: Observable<T>, subAddress: string,
    datumToJson: (datum: T) => Promise<object> | T, type: string) {
    const notifier = this.notifier(subAddress);
    const notify = async (notification: JsonNotification) => {
      if (notification.error)
        this.log.warn('Notifying error on', subAddress, notification.error);
      else if (notification.complete)
        this.log.debug('Completed production of', type);
      await notifier.publish(notification)
        // If notifications fail due to MQTT death, the recipient will find out
        // from the broker so here we make best efforts to notify an error and
        // then give up.
        .catch((error: any) => notifier.publish({ error: toJson(error) }))
        .catch(this.warnError);
    }
    return data.pipe(
      // concatMap guarantees delivery ordering despite toJson promise ordering
      concatMap(async datum => await datumToJson(datum)),
      materialize(),
      flatMap(notification => notification.do(
        next => notify({ next }),
        error => notify({ error: toJson(error) }),
        () => notify({ complete: true }))))
      .toPromise();
  }

  private async reply(
    { fromId: toId, messageId: sentMessageId }: DirectParams, res: Response | null, expectAck?: 'expectAck') {
    const messageId = uuid();
    const replier = this.replier(messageId, toId, sentMessageId);
    this.log.debug('Replying response', messageId, res, replier.id);
    await replier.publish(res);
    if (expectAck)
      return this.getResponse<null>(messageId, null);
  }

  private async nextSender(messageId: string): Promise<SubPub<Request> | null> {
    const present = await this.present().pipe(toArray()).toPromise();
    if (present.every(id => this.recentlySentTo.has(id)))
      this.recentlySentTo.clear();

    this.recentlySentTo.add(this.id);
    const toId = present.filter(id => !this.recentlySentTo.has(id))[0];
    if (toId != null) {
      this.recentlySentTo.add(toId);
      return this.sender(toId, messageId);
    }
    return null;
  }
}

function asMeldErrorStatus(err: any): MeldErrorStatus {
  return err instanceof MeldError ? err.status : MeldErrorStatus['Request rejected'];
}

function deltaFromJson(json: any): DeltaMessage {
  const delta = DeltaMessage.fromJson(json);
  if (delta)
    return delta;
  else
    throw new Error('No time in message');
}

async function jsonFromDelta(msg: DeltaMessage): Promise<any> {
  return msg.toJson();
}