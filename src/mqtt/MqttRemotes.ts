import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, JsonDelta } from '../m-ld';
import { Observable, Subject as Source } from 'rxjs';
import { TreeClock } from '../clocks';
import { connectAsync, AsyncMqttClient, IClientOptions, ISubscriptionMap } from 'async-mqtt';
import { generate as uuid } from 'short-uuid';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, SentParams as SendParams, ReplyParams } from './MqttTopic';
import { TopicParams, AT_LEAST_ONCE, AT_MOST_ONCE } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { Response, Request } from '../m-ld/ControlMessage';
import { Future, jsonFrom } from '../util';
import { map, finalize, flatMap } from 'rxjs/operators';
import { fromRDF, toRDF } from 'jsonld';
import { Triple } from 'rdf-js';
import { fromTimeString, toTimeString } from '../m-ld/JsonDelta';
import { JsonLd } from 'jsonld/jsonld-spec';

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
  private readonly mqtt: Promise<AsyncMqttClient>;
  private readonly id: string;
  private readonly clone: Future<MeldLocal>;
  private readonly remoteUpdates: Source<DeltaMessage> = new Source;
  private readonly operationsTopic: MqttTopic<DomainParams>;
  private readonly controlTopic: MqttTopic<DomainParams>;
  private readonly sentTopic: MqttTopic<SendParams>;
  private readonly replyTopic: MqttTopic<ReplyParams>;
  private readonly presence: MqttPresence;
  private readonly replyResolvers: { [messageId: string]: [(res: Response) => void, PromiseLike<null> | null] } = {};
  private readonly recentlySentTo: Set<string> = new Set;
  private readonly consuming: { [address: string]: Source<any> }

  constructor(domain: string, opts: MqttRemotesOptions) {
    this.id = uuid();
    this.presence = new MqttPresence(domain);
    this.operationsTopic = OPERATIONS_TOPIC.with({ domain });
    this.controlTopic = CONTROL_TOPIC.with({ domain });
    // We only listen for control requests
    this.sentTopic = SEND_TOPIC.with({ toId: this.id, address: this.controlTopic.path });
    this.replyTopic = REPLY_TOPIC.with({ toId: this.id });

    this.mqtt = connectAsync({ ...opts, clientId: this.id }).then(async mqtt => {
      // Set up listeners
      mqtt.on('message', (topic, payload) => {
        this.presence.onMessage(topic, payload);
        this.onMessage(topic, payload);
      });

      // When MQTT dies irrecoverably, tell the clone (it will shut down)
      mqtt.on('error', err => this.remoteUpdates.error(err));

      // Subscribe as required
      const subscriptions: ISubscriptionMap = { ...this.presence.subscriptions };
      subscriptions[this.operationsTopic.address] = AT_LEAST_ONCE;
      subscriptions[this.sentTopic.address] = AT_MOST_ONCE;
      subscriptions[this.replyTopic.address] = AT_MOST_ONCE;
      const grants = await mqtt.subscribe(subscriptions);
      if (!grants.every(grant => subscriptions[grant.topic] == grant.qos))
        throw new Error('Requested QoS was not granted');

      await this.presence.join(mqtt, this.id, this.id, this.controlTopic.address);

      return mqtt;
    });
  }

  get updates(): Observable<DeltaMessage> {
    return this.remoteUpdates;
  }

  connect(clone: MeldLocal): void {
    this.clone.resolve(clone);
    clone.updates.subscribe({
      next: async msg => {
        // Delta received from the local clone. Relay to the domain
        await (await this.mqtt).publish(this.operationsTopic.address, JSON.stringify(msg));
        // When done, mark the message as delivered
        msg.delivered();
      },
      // Local m-ld clone has stopped normally. It will no longer accept messages
      complete: () => this.shutdown(),
      // Local m-ld clone has stopped unexpectedly.
      // The application will already know, so just shut down gracefully.
      error: err => this.shutdown(err)
    });
  }

  private async shutdown(err?: any) {
    console.log('Shutting down MQTT remote proxy ' + err ? 'due to ' + err : 'normally');
    this.presence.leave(await this.mqtt, this.id, this.id);
    (await this.mqtt).end();
  }

  async newClock(): Promise<TreeClock> {
    const res = await this.send<Response.NewClock>(Request.NewClock.json);
    return res.clock;
  }

  async snapshot(): Promise<Snapshot> {
    const ack = new Future<null>();
    const res = await this.send<Response.Snapshot>(Request.Snapshot.json, ack);
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

  revupFrom(): Promise<Observable<DeltaMessage> | undefined> {
    throw new Error('Method not implemented.');
  }

  private async consume<T>(address: string): Promise<Observable<any>> {
    const fullAddress = this.controlSubAddress(address);
    const src = this.consuming[fullAddress] = new Source;
    (await this.mqtt).subscribe(fullAddress, { qos: AT_LEAST_ONCE });
    return src.pipe(finalize(async () => {
      (await this.mqtt).unsubscribe(fullAddress);
      delete this.consuming[fullAddress];
    }));
  }

  private controlSubAddress(address: string): string {
    return new MqttTopic(this.controlTopic.path.concat(address)).address;
  }

  private onMessage(topic: string, payload: Buffer) {
    this.operationsTopic.match(topic, () =>
      this.remoteUpdates.next(jsonFrom(payload) as DeltaMessage));
    this.sentTopic.match(topic, sent =>
      this.onSent(jsonFrom(payload), sent));
    this.replyTopic.match(topic, replied =>
      this.onReply(jsonFrom(payload), replied));
    this.matchObserving(topic, payload);
  }

  private matchObserving(topic: string, payload: Buffer) {
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
    await (await this.mqtt).publish(
      this.nextSendAddress(messageId),
      JSON.stringify(json));
    return new Promise<T>((resolve: (t: T) => void, reject) => {
      // TODO: reject on timeout
      this.replyResolvers[messageId] = [resolve, ack];
    })
  }

  private async onSent(json: any, { fromId, messageId }: SendParams) {
    const req = Request.fromJson(json);
    if (Request.isNewClock(req)) {
      const clock = await (await this.clone).newClock();
      this.reply(fromId, messageId, Response.NewClock.toJson({ clock }));
    } else if (Request.isSnapshot(req)) {
      const { time, lastHash, data, updates } = await (await this.clone).snapshot();
      const dataAddress = uuid(), updatesAddress = uuid();
      await this.reply(fromId, messageId, Response.Snapshot.toJson({
        time, dataAddress, lastHash, updatesAddress
      }), true);
      // Ack has been sent, start streaming the data and updates
      this.produce(data, dataAddress, triples => jsonFromTriples(triples));
      this.produce(updates, updatesAddress, msg => Promise.resolve(jsonFromDelta(msg)));
    }
    // TODO revup
  }

  private produce<T>(data: Observable<T>, address: string, toJson: (datum: T) => Promise<any>) {
    const subs = data.subscribe({
      next: datum => toJson(datum).then(
        json => this.notify(address, { next: json }),
        error => {
          this.notify(address, { error });
          subs.unsubscribe();
        }),
      complete: () => this.notify(address, { complete: true }),
      error: error => this.notify(address, { error })
    });
  }

  private async notify(dataAddress: string, notification: JsonNotification) {
    return (await this.mqtt).publish(this.controlSubAddress(dataAddress), JSON.stringify(notification));
  }

  private async reply(toId: string, sentMessageId: string, json: any, expectAck?: boolean) {
    const messageId = uuid();
    await (await this.mqtt).publish(REPLY_TOPIC.with({
      messageId, fromId: this.id, toId, sentMessageId
    }).address, JSON.stringify(json));
    if (expectAck) {
      return new Promise<void>((resolve: () => void, reject) => {
        // TODO: reject on timeout
        this.replyResolvers[messageId] = [resolve, null];
      })
    }
  }

  private onReply(json: any, { fromId, messageId, sentMessageId }: ReplyParams) {
    if (sentMessageId in this.replyResolvers) {
      const [resolve, ack] = this.replyResolvers[sentMessageId];
      resolve(Response.fromJson(json));
      if (ack)
        ack.then(() => this.reply(fromId, messageId, null));
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

async function jsonFromTriples(triples: Triple[]): Promise<JsonLd> {
  return await fromRDF(triples);
}

function deltaFromJson(json: any): DeltaMessage {
  const time = fromTimeString(json.time);
  if (time)
    return { time, data: json.data };
  else
    throw new Error('No time in message');
}

function jsonFromDelta(msg: DeltaMessage): any {
  return { time: toTimeString(msg.time), data: msg.data };
}