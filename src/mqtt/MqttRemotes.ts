import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, JsonDelta } from '../m-ld';
import { Observable, Subject as Source } from 'rxjs';
import { TreeClock } from '../clocks';
import { connectAsync, AsyncMqttClient, IClientOptions, ISubscriptionMap } from 'async-mqtt';
import { generate as uuid } from 'short-uuid';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, SentParams as SendParams, ReplyParams } from './MqttTopic';
import { TopicParams, AT_LEAST_ONCE, AT_MOST_ONCE } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { Response, Request } from '../m-ld/ControlMessage';

export type MqttRemotesOptions = Omit<IClientOptions, 'clientId'>;

interface DomainParams extends TopicParams { domain: string; }
const OPERATIONS_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'operations']);
const CONTROL_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'control']);

export class MqttRemotes implements MeldRemotes {
  private readonly mqtt: Promise<AsyncMqttClient>;
  private readonly id: string;
  private readonly remoteUpdates: Source<DeltaMessage> = new Source;
  private readonly operationsTopic: MqttTopic<DomainParams>;
  private readonly controlTopic: MqttTopic<DomainParams>;
  private readonly sentTopic: MqttTopic<SendParams>;
  private readonly replyTopic: MqttTopic<ReplyParams>;
  private readonly presence: MqttPresence;

  constructor(domain: string, opts: MqttRemotesOptions) {
    this.id = uuid();
    this.presence = new MqttPresence(domain);
    this.operationsTopic = OPERATIONS_TOPIC.with({ domain });
    this.controlTopic = CONTROL_TOPIC.with({ domain });
    this.sentTopic = SEND_TOPIC.with({ toId: this.id });
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
    return (await this.send<Response.NewClock>(Request.NewClock.json)).clock;
  }

  snapshot(): Promise<Snapshot> {
    throw new Error('Method not implemented.');
  }

  revupFrom(): Promise<Observable<DeltaMessage> | undefined> {
    throw new Error('Method not implemented.');
  }

  private readonly replyResolvers: { [key: string]: (res: Response) => void } = {};

  private async send<T extends Response>(json: any) {
    const messageId = uuid();
    await (await this.mqtt).publish(
      this.nextSendAddress(messageId),
      JSON.stringify(json));
    return new Promise<T>((resolve: (t: T) => void, reject) => {
      // TODO: reject on timeout
      this.replyResolvers[messageId] = resolve;
    })
  }

  private readonly recentlySentTo: Set<string> = new Set;

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

  private onMessage(topic: string, payload: Buffer) {
    this.operationsTopic.match(topic, () =>
      this.remoteUpdates.next(JSON.parse(payload.toString()) as DeltaMessage));
    this.sentTopic.match(topic, sent =>
      this.onSent(JSON.parse(payload.toString()), sent));
    this.replyTopic.match(topic, replied =>
      this.onReply(JSON.parse(payload.toString()), replied));
  }

  private onSent(json: any, { fromId, messageId, address }: SendParams) {
    // TODO
  }

  private onReply(json: any, { fromId, messageId, sentMessageId }: ReplyParams) {
    if (sentMessageId in this.replyResolvers) {
      this.replyResolvers[sentMessageId](Response.fromJson(json));
    }
  }
}
