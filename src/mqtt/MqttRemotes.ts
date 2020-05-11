import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal } from '../m-ld';
import { Observable, Subject as Source } from 'rxjs';
import { TreeClock } from '../clocks';
import { AsyncMqttClient, IClientOptions, ISubscriptionMap, connect as defaultConnect } from 'async-mqtt';
import { generate as uuid } from 'short-uuid';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, SendParams, ReplyParams, DirectParams } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { Response, Request, Hello } from '../m-ld/ControlMessage';
import { Future, jsonFrom } from '../util';
import { map, finalize, flatMap, reduce, toArray } from 'rxjs/operators';
import { fromTimeString, toTimeString, toMeldJson, fromMeldJson } from '../m-ld/MeldJson';
import { Hash } from '../hash';
import AsyncLock = require('async-lock');
import { LogLevelDesc } from 'loglevel';
import { MeldError, BAD_UPDATE, NONE_VISIBLE } from '../m-ld/MeldError';
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

export class MqttRemotes extends AbstractMeld<DeltaMessage> implements MeldRemotes {
  private readonly mqtt: AsyncMqttClient;
  private clone?: MeldLocal;
  private readonly operationsTopic: MqttTopic<DomainParams>;
  private readonly controlTopic: MqttTopic<DomainParams>;
  private readonly registryTopic: MqttTopic<DomainParams>;
  private readonly sentTopic: MqttTopic<SendParams>;
  private readonly replyTopic: MqttTopic<ReplyParams>;
  private readonly presence: MqttPresence;
  private readonly replyResolvers: {
    [messageId: string]: [(res: Response | null) => void, PromiseLike<null> | null]
  } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [address: string]: Source<any> } = {};
  isGenesis: Future<boolean> = new Future;
  private readonly notifyLock = new AsyncLock;
  private readonly sendTimeout: number;

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
    this.mqtt.on('error', err => this.log.warn(err));
    this.presence.on('error', err => this.log.warn(err));

    // MQTT.js 'close' event signals a disconnect - definitely offline.
    this.mqtt.on('close', () => this.setOnline(null));

    this.mqtt.on('connect', async () => {
      try {
        // Subscribe as required
        const subscriptions: ISubscriptionMap = {
          [this.operationsTopic.address]: 1,
          [this.controlTopic.address]: 1,
          [this.registryTopic.address]: 1,
          [this.sentTopic.address]: 0,
          [this.replyTopic.address]: 0
        };
        const grants = await this.mqtt.subscribe(subscriptions);
        if (!grants.every(grant => subscriptions[grant.topic] == grant.qos))
          throw new Error('Requested QoS was not granted');
        // Tell the world that we will be a clone on this domain
        await this.mqtt.publish(this.registryTopic.address,
          JSON.stringify({ id: this.id } as Hello), { qos: 1, retain: true });
        if (this.clone != null && await isOnline(this.clone) === true)
          this.clonePresent(true).catch(err => this.log.warn(err));
      } catch (err) {
        if (this.mqtt.connected)
          this.close(err); // This is a catastrophe, can't bootstrap
        else
          this.log.debug(err); // We disconnected during bootstrap
      }
    });
  }

  setLocal(clone: MeldLocal): void {
    if (this.clone == null) {
      this.clone = clone;
      // Start sending updates from the local clone to the remotes
      clone.updates.subscribe({
        next: async msg => {
          // If we are not online, we just ignore updates.
          // They will be replayed from the clone's journal on re-connection.
          if (await this.isOnline()) {
            try {
              // Delta received from the local clone. Relay to the domain
              await this.mqtt.publish(
                this.operationsTopic.address, JSON.stringify({
                  ...msg.toJson(),
                  [CHANNEL_ID_HEADER]: this.id
                }), { qos: 1 });
              // When done, mark the message as delivered
              msg.delivered();
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
          this.clonePresent(online).catch(err => this.log.warn(err));
      });
    } else if (clone != this.clone) {
      throw new Error(`${this.id}: Local clone cannot change`);
    }
  }

  async close(err?: any) {
    this.log.info('Shutting down MQTT remotes', err ? 'due to ' + err : 'normally');
    super.close(err);
    try {
      await this.clonePresent(false);
      await this.mqtt.end();
    } catch (err) {
      this.log.warn(err);
    }
  }

  async newClock(): Promise<TreeClock> {
    const isGenesis = await this.isGenesis;
    return isGenesis ? Promise.resolve(TreeClock.GENESIS) :
      (await this.send<Response.NewClock>(Request.NewClock.JSON)).clock;
  }

  async snapshot(): Promise<Snapshot> {
    const ack = new Future<null>();
    const res = await this.send<Response.Snapshot>(Request.Snapshot.JSON, ack);
    const snapshot: Snapshot = {
      time: res.time,
      data: (await this.consume(res.dataAddress)).pipe(flatMap(fromMeldJson)),
      lastHash: res.lastHash,
      updates: (await this.consume(res.updatesAddress)).pipe(map(deltaFromJson))
    };
    // Ack the response to start the streams
    ack.resolve(null);
    return snapshot;
  }

  revupFrom(time: TreeClock): Promise<Observable<DeltaMessage> | undefined> {
    return this.revupFromNext(time, new Set);
  }

  private async revupFromNext(time: TreeClock, tried: Set<string>): Promise<Observable<DeltaMessage> | undefined> {
    const ack = new Future<null>();
    const res = await this.send<Response.Revup>(new Request.Revup(time).toJson(), ack);
    if (res.canRevup) {
      const updates = (await this.consume(res.updatesAddress)).pipe(map(deltaFromJson));
      // Ack the response to start the streams
      ack.resolve(null);
      return updates;
    } else if (!tried.has(res.updatesAddress)) {
      tried.add(res.updatesAddress);
      // Not expecting an ack if hash not found
      return this.revupFromNext(time, tried);
    } // else return undefined
  }

  private async clonePresent(online: boolean) {
    if (this.mqtt.connected) {
      if (online)
        return this.presence.join(this.id, this.controlTopic.address);
      else
        return this.presence.leave(this.id);
    }
  }

  private async consume(subAddress: string): Promise<Observable<any>> {
    const address = this.controlSubAddress(subAddress);
    const src = this.consuming[address] = new Source;
    await this.mqtt.subscribe(address, { qos: 1 });
    return src.pipe(finalize(async () => {
      this.mqtt.unsubscribe(address);
      delete this.consuming[address];
    }));
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
        this.close(new MeldError(BAD_UPDATE));
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
        this.consuming[topic].error(new Error(jsonNotification.error));
    }
  }

  private async send<T extends Response>(json: any, ack: PromiseLike<null> | null = null): Promise<T> {
    const messageId = uuid();
    const address = await this.nextSendAddress(messageId);
    if (!address)
      throw new MeldError(NONE_VISIBLE,
        `No-one present on ${this.controlTopic.address} to send message to`);

    this.log.debug('Sending request', messageId, json);
    await this.mqtt.publish(address, JSON.stringify(json));
    return this.getResponse<T>(messageId, ack);
  }

  private getResponse<T extends Response | null>(messageId: string, ack: PromiseLike<null> | null) {
    const response = new Future<T>();
    const timer = setTimeout(() => {
      delete this.replyResolvers[messageId];
      this.log.debug(`Message ${messageId} timed out.`)
      response.reject(new Error('Send timeout exceeded.'));
    }, this.sendTimeout);
    this.replyResolvers[messageId] = [res => {
      delete this.replyResolvers[messageId];
      clearTimeout(timer);
      response.resolve(res as T);
    }, ack];
    return response;
  }

  private async onSent(json: any, sentParams: SendParams) {
    // Ignore control messages before we have a clone
    if (this.clone) {
      try {
        const req = Request.fromJson(json);
        if (req instanceof Request.NewClock) {
          await this.replyClock(sentParams, this.clone.newClock());
        } else if (req instanceof Request.Snapshot) {
          await this.replySnapshot(sentParams, this.clone.snapshot());
        } else if (req instanceof Request.Revup) {
          await this.replyRevup(sentParams, this.clone.revupFrom(req.time));
        }
      } catch (err) {
        this.log.warn(err);
      }
    }
  }

  private async replyClock(sentParams: SendParams, clock: Promise<TreeClock>) {
    this.reply(sentParams, new Response.NewClock(await clock).toJson());
  }

  private async replySnapshot(sentParams: SendParams, snapshot: Promise<Snapshot>) {
    const { time, lastHash, data, updates } = await snapshot;
    const dataAddress = uuid(), updatesAddress = uuid();
    await this.reply(sentParams, new Response.Snapshot(
      time, dataAddress, lastHash, updatesAddress
    ).toJson(), true);
    // Ack has been sent, start streaming the data and updates
    this.produce(data, dataAddress, toMeldJson, 'snapshot');
    this.produce(updates, updatesAddress, jsonFromDelta, 'updates');
  }

  private async replyRevup(sentParams: SendParams,
    willRevup: Promise<Observable<DeltaMessage> | undefined>) {
    const revup = await willRevup;
    if (revup) {
      const updatesAddress = uuid();
      await this.reply(sentParams, new Response.Revup(true, updatesAddress).toJson(), true);
      // Ack has been sent, start streaming the updates
      this.produce(revup, updatesAddress, jsonFromDelta, 'updates');
    } else if (this.clone) {
      this.reply(sentParams, new Response.Revup(false, this.clone.id).toJson());
    }
  }

  private produce<T>(data: Observable<T>, subAddress: string,
    toJson: (datum: T) => Promise<any>, type: 'snapshot' | 'updates') {

    const address = this.controlSubAddress(subAddress);
    // If notifications fail due to MQTT death, the recipient will find out from the broker
    // so here we make best efforts to notify an error and then give up.
    const logError = (err: any) => this.log.warn(err);
    const subs = data.subscribe({
      next: datum => this.notify(address, toJson(datum).then(json => ({ next: json })))
        .catch(error => {
          // All our productions are guaranteed delivery
          this.notify(address, Promise.resolve({ error })).catch(logError);
          subs.unsubscribe();
        }),
      complete: () => this.notify(address, Promise.resolve({ complete: true }))
        .then(() => this.log.debug('Completed production of', type))
        .catch(logError),
      error: error => this.notify(address, Promise.resolve({ error }))
        .catch(logError)
    });
  }

  private async notify(address: string, notification: Promise<JsonNotification>): Promise<unknown> {
    // Use of a lock guarantees delivery ordering despite promise ordering
    return this.notifyLock.acquire(address, async () =>
      this.mqtt.publish(address, JSON.stringify(await notification)));
  }

  private async reply({ fromId: toId, messageId: sentMessageId }: DirectParams, json: any, expectAck?: boolean) {
    const messageId = uuid();
    await this.mqtt.publish(REPLY_TOPIC.with({
      messageId, fromId: this.id, toId, sentMessageId
    }).address, JSON.stringify(json));
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