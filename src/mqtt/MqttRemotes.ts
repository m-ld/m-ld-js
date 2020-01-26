import { Snapshot, DeltaMessage, MeldRemotes, MeldLocal, JsonDelta } from '../m-ld';
import { Observable, Subject as Source } from 'rxjs';
import { TreeClock } from '../clocks';
import { connectAsync, AsyncMqttClient, IClientOptions, ISubscriptionMap } from 'async-mqtt';
import { generate as uuid } from 'short-uuid';
import { MqttTopic, SENT_TOPIC, REPLY_TOPIC, SentParams, ReplyParams } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';

const AT_MOST_ONCE = 0, AT_LEAST_ONCE = 1;

export type MqttRemotesOptions = Omit<IClientOptions, 'clientId'>;

interface DomainParams extends TopicParams {
  domain: string;
}

const OPERATIONS_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'operations']);
const CONTROL_TOPIC = new MqttTopic<DomainParams>([{ '+': 'domain' }, 'control']);
const PRESENCE_TOPIC = new MqttTopic<DomainParams & {
  clientConsumer: string[];
}>(['__presence', { '+': 'domain' }, { '#': 'clientConsumer' }]);

export class MqttRemotes implements MeldRemotes {
  private readonly mqtt: Promise<AsyncMqttClient>;
  private readonly clientId: string;
  private readonly remoteUpdates: Source<DeltaMessage> = new Source;

  constructor(domain: string, opts: MqttRemotesOptions) {
    const toId = this.clientId = uuid();
    const presenceTopic = PRESENCE_TOPIC.with({ domain });
    const operationsTopic = OPERATIONS_TOPIC.with({ domain });
    const controlTopic = CONTROL_TOPIC.with({ domain });
    const sentTopic = SENT_TOPIC.with({ toId });
    const replyTopic = REPLY_TOPIC.with({ toId });

    this.mqtt = connectAsync({ ...opts, clientId: toId }).then(async mqtt => {
      // Set up listeners
      mqtt.on('message', (topic, payload) => {
        presenceTopic.match(topic, presence =>
          this.onPresence(payload.toString(), ...presence.clientConsumer));
        operationsTopic.match(topic, () =>
          this.remoteUpdates.next(JSON.parse(payload.toString()) as DeltaMessage));
        sentTopic.match(topic, sent =>
          this.onSent(JSON.parse(payload.toString()), sent));
        replyTopic.match(topic, replied =>
          this.onReply(JSON.parse(payload.toString()), replied));
      });

      // Subscribe as required
      const subscriptions: ISubscriptionMap = {};
      subscriptions[presenceTopic.address] = AT_LEAST_ONCE;
      subscriptions[operationsTopic.address] = AT_LEAST_ONCE;
      subscriptions[sentTopic.address] = AT_MOST_ONCE;
      subscriptions[replyTopic.address] = AT_MOST_ONCE;
      const grants = await mqtt.subscribe(subscriptions);
      if (!grants.every(grant => subscriptions[grant.topic] == grant.qos))
        throw new Error('Requested QoS was not granted');

      // Publish our presence on the control topic
      await mqtt.publish(presenceTopic.with({
        clientConsumer: [this.clientId, 'control']
      }).address, controlTopic.address);

      return mqtt;
    });
  }

  get updates(): Observable<DeltaMessage> {
    return this.remoteUpdates;
  }

  connect(clone: MeldLocal): void {
    throw new Error('Method not implemented.');
  }

  newClock(): Promise<TreeClock> {
    throw new Error('Method not implemented.');
  }

  snapshot(): Promise<Snapshot> {
    throw new Error('Method not implemented.');
  }

  revupFrom(): Promise<Observable<DeltaMessage> | undefined> {
    throw new Error('Method not implemented.');
  }

  onPresence(address: string, clientId?: string, consumerId?: string) {
    throw new Error('Method not implemented.');
  }

  onSent(msg: any/*TODO*/, { fromId, messageId, address }: SentParams) {
    throw new Error('Method not implemented.');
  }

  onReply(msg: any/*TODO*/, { fromId, messageId, sentMessageId }: ReplyParams) {
    throw new Error('Method not implemented.');
  }
}
