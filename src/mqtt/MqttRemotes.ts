import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal } from '../m-ld';
import { Observable, Subject as Source } from 'rxjs';
import { TreeClock } from '../clocks';
import { AsyncMqttClient, IClientOptions, ISubscriptionMap } from 'async-mqtt';
import { generate as uuid } from 'short-uuid';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, SendParams, ReplyParams, DirectParams } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { Response, Request } from '../m-ld/ControlMessage';
import { Future, jsonFrom } from '../util';
import { map, finalize, flatMap } from 'rxjs/operators';
import { fromRDF, toRDF } from 'jsonld';
import { Triple } from 'rdf-js';
import { fromTimeString, toTimeString } from '../m-ld/JsonDelta';
import { Hash } from '../hash';

export type MqttRemotesOptions = Omit<IClientOptions, 'clientId'>;

interface DomainParams extends TopicParams { domain: string; }
const OPERATIONS_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'operations']);
const CONTROL_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'control']);

// @see org.m_ld.json.MeldJacksonModule.NotificationDeserializer
interface JsonNotification {
  next?: any;
  complete?: true;
  error?: any;
}

export class MqttRemotes implements MeldRemotes {
  private readonly id: string;
  private readonly initialised: Future<void>;
  private clone?: MeldLocal;
  private readonly remoteUpdates: Source<DeltaMessage> = new Source;
  private readonly operationsTopic: MqttTopic<DomainParams>;
  private readonly controlTopic: MqttTopic<DomainParams>;
  private readonly sentTopic: MqttTopic<SendParams>;
  private readonly replyTopic: MqttTopic<ReplyParams>;
  private readonly presence: MqttPresence;
  private readonly replyResolvers: {
    [messageId: string]: [(res: Response) => void, PromiseLike<null> | null]
  } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [address: string]: Source<any> } = {};

  constructor(domain: string, private readonly mqtt: AsyncMqttClient) {
    if (!mqtt.options.clientId)
      throw new Error('MQTT for m-ld remotes must have a client Id');

    this.id = mqtt.options.clientId;
    this.initialised = new Future;
    this.presence = new MqttPresence(domain);
    this.operationsTopic = OPERATIONS_TOPIC.with({ domain });
    this.controlTopic = CONTROL_TOPIC.with({ domain });
    // We only listen for control requests
    this.sentTopic = SEND_TOPIC.with({ toId: this.id, address: this.controlTopic.path });
    this.replyTopic = REPLY_TOPIC.with({ toId: this.id });

    mqtt.on('connect', async () => {
      try { // Set up listeners
        mqtt.on('message', (topic, payload) => {
          this.presence.onMessage(topic, payload);
          this.onMessage(topic, payload);
        });

        // When MQTT dies irrecoverably, tell the clone (it will shut down)
        mqtt.on('error', err => this.remoteUpdates.error(err));

        // Subscribe as required
        const subscriptions: ISubscriptionMap = { ...this.presence.subscriptions };
        subscriptions[this.operationsTopic.address] = 1;
        subscriptions[this.sentTopic.address] = 0;
        subscriptions[this.replyTopic.address] = 0;
        const grants = await mqtt.subscribe(subscriptions);
        if (!grants.every(grant => subscriptions[grant.topic] == grant.qos))
          throw new Error('Requested QoS was not granted');

        await this.presence.join(mqtt, this.id, this.id, this.controlTopic.address);
        this.initialised.resolve();
      } catch (err) {
        this.initialised.reject(err);
      }
    });
  }

  async initialise(): Promise<void> {
    await this.initialised;
  }

  get updates(): Observable<DeltaMessage> {
    return this.remoteUpdates;
  }

  connect(clone: MeldLocal): void {
    this.clone = clone;
    clone.updates.subscribe({
      next: async msg => {
        try {
          // Delta received from the local clone. Relay to the domain
          await this.mqtt.publish(
            this.operationsTopic.address, JSON.stringify(msg));
          // When done, mark the message as delivered
          msg.delivered();
        } catch (err) {
          // Failure to send an update is catastrophic - signal death to the clone
          this.remoteUpdates.error(err);
        }
      },
      // Local m-ld clone has stopped normally. It will no longer accept messages
      complete: () => this.close(),
      // Local m-ld clone has stopped unexpectedly.
      // The application will already know, so just shut down gracefully.
      error: err => this.close(err)
    });
  }

  private close(err?: any) {
    console.log('Shutting down MQTT remote proxy ' + err ? 'due to ' + err : 'normally');
    this.presence.leave(this.mqtt, this.id, this.id);
    this.mqtt.end();
  }

  async newClock(): Promise<TreeClock> {
    const res = await this.send<Response.NewClock>(Request.NewClock.JSON);
    return res.clock;
  }

  async snapshot(): Promise<Snapshot> {
    const ack = new Future<null>();
    const res = await this.send<Response.Snapshot>(Request.Snapshot.JSON, ack);
    const snapshot: Snapshot = {
      time: res.time,
      data: (await this.consume(res.dataAddress)).pipe(flatMap(triplesFromJson)),
      lastHash: res.lastHash,
      updates: (await this.consume(res.updatesAddress)).pipe(map(deltaFromJson))
    };
    // Ack the response to start the streams
    ack.resolve(null);
    return snapshot;
  }

  revupFrom(lastHash: Hash): Promise<Observable<DeltaMessage> | undefined> {
    return this.revupFromNext(lastHash, new Set);
  }

  private async revupFromNext(lastHash: Hash, tried: Set<string>): Promise<Observable<DeltaMessage> | undefined> {
    const ack = new Future<null>();
    const res = await this.send<Response.Revup>(Request.Revup.toJson({ lastHash }), ack);
    if (res.hashFound) {
      const updates = (await this.consume(res.updatesAddress)).pipe(map(deltaFromJson));
      // Ack the response to start the streams
      ack.resolve(null);
      return updates;
    } else if (!tried.has(res.updatesAddress)) {
      tried.add(res.updatesAddress);
      // Not expecting an ack if hash not found
      return this.revupFromNext(lastHash, tried);
    } // else return undefined
  }

  private async consume<T>(subAddress: string): Promise<Observable<any>> {
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
    this.operationsTopic.match(topic, () =>
      this.remoteUpdates.next(DeltaMessage.fromJson(jsonFrom(payload))));
    this.sentTopic.match(topic, sent =>
      this.onSent(jsonFrom(payload), sent));
    this.replyTopic.match(topic, replied =>
      this.onReply(jsonFrom(payload), replied));
    this.matchConsuming(topic, payload);
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
    await this.mqtt.publish(
      this.nextSendAddress(messageId),
      JSON.stringify(json));
    return new Promise<T>((resolve: (t: T) => void, reject) => {
      // TODO: reject on timeout
      this.replyResolvers[messageId] = [resolve, ack];
    })
  }

  private onSent(json: any, sentParams: SendParams) {
    // Ignore control messages before we have a clone
    if (this.clone) {
      const req = Request.fromJson(json);
      if (Request.isNewClock(req)) {
        this.replyClock(sentParams, this.clone.newClock());
      } else if (Request.isSnapshot(req)) {
        this.replySnapshot(sentParams, this.clone.snapshot());
      } else if (Request.isRevup(req)) {
        this.replyRevup(sentParams, this.clone.revupFrom(req.lastHash));
      }
    }
  }

  private async replyClock(sentParams: SendParams, clock: Promise<TreeClock>) {
    this.reply(sentParams, Response.NewClock.toJson({ clock: await clock }));
  }

  private async replySnapshot(sentParams: SendParams, snapshot: Promise<Snapshot>) {
    const { time, lastHash, data, updates } = await snapshot;
    const dataAddress = uuid(), updatesAddress = uuid();
    await this.reply(sentParams, Response.Snapshot.toJson({
      time, dataAddress, lastHash, updatesAddress
    }), true);
    // Ack has been sent, start streaming the data and updates
    this.produce(data, dataAddress, jsonFromTriples);
    this.produce(updates, updatesAddress, jsonFromDelta);
  }

  private async replyRevup(sentParams: SendParams,
    willRevup: Promise<Observable<DeltaMessage> | undefined>) {
    const revup = await willRevup;
    if (revup) {
      const updatesAddress = uuid();
      await this.reply(sentParams, Response.Revup.toJson({
        hashFound: true, updatesAddress
      }), true);
      // Ack has been sent, start streaming the updates
      this.produce(revup, updatesAddress, jsonFromDelta);
    } else if (this.clone) {
      this.reply(sentParams, Response.Revup.toJson({
        hashFound: false, updatesAddress: this.clone.id
      }));
    }
  }

  private produce<T>(data: Observable<T>, subAddress: string, toJson: (datum: T) => Promise<any>) {
    const address = this.controlSubAddress(subAddress);
    const subs = data.subscribe({
      next: datum => toJson(datum)
        .then(json => this.notify(address, { next: json }))
        .catch(error => {
          // All our productions are guaranteed delivery
          this.notify(address, { error });
          subs.unsubscribe();
        }),
      complete: () => this.notify(address, { complete: true }),
      error: error => this.notify(address, { error })
    });
  }

  private async notify(address: string, notification: JsonNotification): Promise<unknown> {
    return this.mqtt.publish(address, JSON.stringify(notification));
  }

  private async reply({ fromId: toId, messageId: sentMessageId }: DirectParams, json: any, expectAck?: boolean) {
    const messageId = uuid();
    await this.mqtt.publish(REPLY_TOPIC.with({
      messageId, fromId: this.id, toId, sentMessageId
    }).address, JSON.stringify(json));
    if (expectAck) {
      return new Promise<void>((resolve: () => void, reject) => {
        // TODO: reject on timeout
        this.replyResolvers[messageId] = [resolve, null];
      })
    }
  }

  private onReply(json: any, replyParams: ReplyParams) {
    if (replyParams.sentMessageId in this.replyResolvers) {
      const [resolve, ack] = this.replyResolvers[replyParams.sentMessageId];
      resolve(Response.fromJson(json));
      if (ack) // A m-ld ack is a reply to a reply with a null body
        ack.then(() => this.reply(replyParams, null));
    }
  }

  private nextSendAddress(messageId: string): string {
    const present = Array.from(this.presence.present(this.controlTopic.address));
    if (present.every(id => this.recentlySentTo.has(id)))
      this.recentlySentTo.clear();

    this.recentlySentTo.add(this.id);
    const toId = present.filter(id => !this.recentlySentTo.has(id))[0];
    if (toId) {
      this.recentlySentTo.add(toId);
      return SEND_TOPIC.with({
        toId, fromId: this.id, messageId, address: this.controlTopic.path
      }).address;
    } else {
      throw new Error(`No-one present on ${this.controlTopic.address} to send message to`);
    }
  }
}

async function triplesFromJson(json: any): Promise<Triple[]> {
  return await toRDF(json) as Triple[];
}

async function jsonFromTriples(triples: Triple[]): Promise<any> {
  return await fromRDF(triples);
}

function deltaFromJson(json: any): DeltaMessage {
  const time = fromTimeString(json.time);
  if (time)
    return { time, data: json.data };
  else
    throw new Error('No time in message');
}

async function jsonFromDelta(msg: DeltaMessage): Promise<any> {
  return { time: toTimeString(msg.time), data: msg.data };
}