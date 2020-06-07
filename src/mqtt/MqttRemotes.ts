import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, UUID } from '../m-ld';
import { Observable, Subject as Source, BehaviorSubject, identity } from 'rxjs';
import { TreeClock } from '../clocks';
import { AsyncMqttClient, IClientOptions, ISubscriptionMap, connect as defaultConnect } from 'async-mqtt';
import { generate as uuid } from 'short-uuid';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, SendParams, ReplyParams, DirectParams } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { Response, Request, Hello } from '../m-ld/ControlMessage';
import { Future, jsonFrom, toJson } from '../util';
import { finalize, flatMap, reduce, toArray, first, concatMap, materialize, timeout } from 'rxjs/operators';
import { toMeldJson, fromMeldJson } from '../m-ld/MeldJson';
import { LogLevelDesc } from 'loglevel';
import { MeldError, MeldErrorStatus } from '../m-ld/MeldError';
import { AbstractMeld, isOnline } from '../AbstractMeld';

interface DomainParams extends TopicParams { domain: string; }
const OPERATIONS_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'operations']);
const CONTROL_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'control']);
const REGISTRY_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'registry']);

// @see org.m_ld.json.MeldJacksonModule.NotificationDeserializer
interface JsonNotification {
  next?: any;
  complete?: true;
  error?: any;
}

const CHANNEL_ID_HEADER = '__channel.id';

export type MeldMqttOpts = Omit<IClientOptions, 'will' | 'clientId'> &
  ({ hostname: string } | { host: string, port: number }) & { sendTimeout?: number, logLevel?: LogLevelDesc }

export class MqttRemotes extends AbstractMeld implements MeldRemotes {
  private readonly mqtt: AsyncMqttClient;
  private readonly localClone = new BehaviorSubject<MeldLocal | null>(null);
  private readonly operationsTopic: MqttTopic<DomainParams>;
  private readonly controlTopic: MqttTopic<DomainParams>;
  private readonly registryTopic: MqttTopic<DomainParams>;
  private readonly sentTopic: MqttTopic<SendParams>;
  private readonly replyTopic: MqttTopic<ReplyParams>;
  private readonly presence: MqttPresence;
  private readonly replyResolvers: {
    [messageId: string]: [(res: Response | null) => void, PromiseLike<void> | null]
  } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [address: string]: Source<any> } = {};
  isGenesis: Future<boolean> = new Future;
  private readonly sendTimeout: number;
  private readonly activity: Set<Promise<void>> = new Set;

  constructor(domain: string, id: string, opts: MeldMqttOpts,
    connect: (opts: IClientOptions) => AsyncMqttClient = defaultConnect) {
    super(id, opts.logLevel ?? 'info');

    this.sendTimeout = opts.sendTimeout || 2000;
    this.operationsTopic = OPERATIONS_TOPIC.with({ domain });
    this.controlTopic = CONTROL_TOPIC.with({ domain });
    this.registryTopic = REGISTRY_TOPIC.with({ domain });
    // We only listen for control requests
    this.sentTopic = SEND_TOPIC.with({ toId: this.id, address: this.controlTopic.path });
    this.replyTopic = REPLY_TOPIC.with({ toId: this.id });
    this.mqtt = connect({ ...opts, clientId: id, will: MqttPresence.will(domain, id) });
    this.presence = new MqttPresence(this.mqtt, domain, id);

    // Set up listeners
    this.presence.on('change', () => {
      // If there is more than just me present, we are online
      this.presence.present(this.controlTopic.address)
        .pipe(reduce((online, id) => online || id !== this.id, false))
        .subscribe(online => this.setOnline(online));
    });

    this.mqtt.on('message', (topic, payload) => this.onMessage(topic, payload));

    // When MQTT.js receives an error just log - it will try to reconnect
    this.mqtt.on('error', this.warnError);
    this.presence.on('error', this.warnError);

    // MQTT.js 'close' event signals a disconnect - definitely offline.
    this.mqtt.on('close', () => this.setOnline(null));

    this.mqtt.on('connect', async () => {
      try {
        // Subscribe as required
        const subscriptions: ISubscriptionMap = {
          ...this.presence.subscriptions,
          [this.operationsTopic.address]: { qos: 1 },
          [this.controlTopic.address]: { qos: 1 },
          [this.registryTopic.address]: { qos: 1 },
          [this.sentTopic.address]: { qos: 0 },
          [this.replyTopic.address]: { qos: 0 }
        };
        const grants = await this.mqtt.subscribe(subscriptions);
        if (!grants.every(grant => subscriptions[grant.topic].qos == grant.qos))
          throw new Error('Requested QoS was not granted');
        // We don't have to wait for the presence to initialise
        this.presence.initialise().catch(this.warnError);
        // Tell the world that we will be a clone on this domain
        await this.mqtt.publish(this.registryTopic.address,
          JSON.stringify({ id: this.id } as Hello), { qos: 1, retain: true });
        if (this.clone != null && await isOnline(this.clone) === true)
          this.clonePresent(true).catch(this.warnError);
      } catch (err) {
        if (this.mqtt.connected)
          this.close(err); // This is a catastrophe, can't bootstrap
        else
          this.log.debug(err); // We disconnected during bootstrap
      }
    });
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
              await this.mqtt.publish(
                this.operationsTopic.address, JSON.stringify({
                  ...msg.toJson(),
                  [CHANNEL_ID_HEADER]: this.id
                }), { qos: 1 });
              // When done, mark the message as delivered
              msg.delivered.resolve();
            } catch (err) {
              // Failed to send an update while (probably) connected
              this.log.warn(err);
              // We can't allow gaps, so ensure a reconnect
              this.mqtt.reconnect();
            }
          }
        },
        // Local m-ld clone has stopped. It will no longer accept messages.
        complete: () => this.close(),
        // Local m-ld clone has stopped unexpectedly.
        // The application will already know, so just shut down gracefully.
        error: err => this.close(err)
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

  async close(err?: any) {
    if (err)
      this.log.info('Shutting down due to', err);
    else
      this.log.info('Shutting down normally');
    // This finalises the #updates, thereby notifying the clone
    super.close(err);
    try {
      // Wait until the clone has closed
      await this.localClone.pipe(first(null)).toPromise();
      // Wait until all activity have finalised
      await Promise.all(this.activity); // TODO unit test this
      await this.mqtt.end();
    } catch (err) {
      this.log.warn(err);
    }
  }

  async newClock(): Promise<TreeClock> {
    const isGenesis = await this.isGenesis;
    return isGenesis ? Promise.resolve(TreeClock.GENESIS) :
      (await this.send<Response.NewClock>(new Request.NewClock)).clock;
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

  private get clone() {
    return this.localClone.value;
  }

  private set clone(clone: MeldLocal | null) {
    this.localClone.next(clone);
  }

  private async clonePresent(online: boolean) {
    if (this.mqtt.connected) {
      if (online)
        return this.presence.join(this.id, this.controlTopic.address);
      else
        return this.presence.leave(this.id);
    }
  }

  private async consume<T>(subAddress: string, map: (json: any) => T | Promise<T>, failIfSlow?: 'failIfSlow'): Promise<Observable<T>> {
    const address = this.controlSubAddress(subAddress);
    const src = this.consuming[address] = new Source;
    await this.mqtt.subscribe(address, { qos: 1 });
    const consumed = src.pipe(
      // Unsubscribe from the sub-channel when a complete or error arrives
      finalize(() => {
        this.mqtt.unsubscribe(address);
        delete this.consuming[address];
      }),
      flatMap(async json => map(json)));
    // Rev-up and snapshot update channels are expected to be very fast, as they
    // are streamed directly from the dataset. So if there is a pause in the
    // delivery this probably indicates a failure e.g. the collaborator is dead.
    // TODO unit test this
    return failIfSlow ? consumed.pipe(timeout(this.sendTimeout)) : consumed;
  }

  private controlSubAddress(address: string): string {
    return new MqttTopic(this.controlTopic.path.concat(address)).address;
  }

  private onMessage(topic: string, payload: Buffer) {
    this.operationsTopic.match(topic, () => this.onRemoteUpdate(payload));
    this.registryTopic.match(topic, () => this.onHello(payload));
    this.sentTopic.match(topic, sent => this.onSent(jsonFrom(payload), sent));
    this.replyTopic.match(topic, replied => this.onReply(jsonFrom(payload), replied));
    this.matchConsuming(topic, payload);
  }

  private onRemoteUpdate(payload: Buffer) {
    const json = jsonFrom(payload);
    // Ignore echoed updates
    if (!json[CHANNEL_ID_HEADER] || json[CHANNEL_ID_HEADER] !== this.id) {
      const update = DeltaMessage.fromJson(json);
      if (update)
        this.nextUpdate(update);
      else
        // This is extremely bad - may indicate a bad actor
        this.close(new MeldError('Bad Update'));
    }
  }

  private onHello(payload: Buffer) {
    const hello = jsonFrom(payload);
    // This is a race, since a Future will ignore a re-resolve
    if (this.id === hello.id)
      this.isGenesis.resolve(true);
    else
      this.isGenesis.resolve(false);
  }

  private matchConsuming(topic: string, payload: Buffer) {
    if (topic in this.consuming) {
      const jsonNotification = jsonFrom(payload) as JsonNotification;
      if (jsonNotification.next)
        this.consuming[topic].next(jsonNotification.next);
      else if (jsonNotification.complete)
        this.consuming[topic].complete();
      else if (jsonNotification.error)
        this.consuming[topic].error(MeldError.from(jsonNotification.error));
    }
  }

  private async send<T extends Response>(
    request: Request,
    { ack, check }: {
      ack?: PromiseLike<void>;
      check?: (res: T) => boolean;
    } = {},
    tried: { [address: string]: PromiseLike<T> } = {},
    messageId: string = uuid()): Promise<T> {
    const address = await this.nextSendAddress(messageId);
    if (!address) {
      throw new MeldError('No visible clones',
        `No-one present on ${this.controlTopic.address} to send message to`);
    } else if (address in tried) {
      // If we have already tried this address, we've tried everyone
      return tried[address];
    }
    this.log.debug('Sending request', messageId, request, address);
    // If the publish fails, don't keep trying other addresses
    await this.mqtt.publish(address, JSON.stringify(request.toJson()));
    tried[address] = this.getResponse<T>(messageId, ack ?? null);
    // If the caller doesn't like this response, try again
    return tried[address].then(res => check == null || check(res) ? res :
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

  private async onSent(json: any, sentParams: SendParams) {
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

  private produce<T>(data: Observable<T>, subAddress: string,
    datumToJson: (datum: T) => Promise<object> | T, type: string) {
    const address = this.controlSubAddress(subAddress);
    const notify = async (notification: JsonNotification) => {
      if (notification.error)
        this.log.warn('Notifying error on', subAddress, notification.error);
      else if (notification.complete)
        this.log.debug('Completed production of', type);
      await this.mqtt.publish(address, JSON.stringify(notification))
        // If notifications fail due to MQTT death, the recipient will find out
        // from the broker so here we make best efforts to notify an error and
        // then give up.
        .catch((error: any) => this.mqtt.publish(address, JSON.stringify({ error: toJson(error) })))
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
    const address = REPLY_TOPIC.with({
      messageId, fromId: this.id, toId, sentMessageId
    }).address;
    this.log.debug('Replying response', messageId, res, address);
    await this.mqtt.publish(address, JSON.stringify(res != null ? res.toJson() : null));
    if (expectAck)
      return this.getResponse<null>(messageId, null);
  }

  private async onReply(json: any, replyParams: ReplyParams) {
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

  private async nextSendAddress(messageId: string): Promise<string | null> {
    const present = await this.presence.present(this.controlTopic.address).pipe(toArray()).toPromise();
    if (present.every(id => this.recentlySentTo.has(id)))
      this.recentlySentTo.clear();

    this.recentlySentTo.add(this.id);
    const toId = present.filter(id => !this.recentlySentTo.has(id))[0];
    if (toId != null) {
      this.recentlySentTo.add(toId);
      return SEND_TOPIC.with({
        toId, fromId: this.id, messageId, address: this.controlTopic.path
      }).address;
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