import { TopicParams, matches } from 'mqtt-pattern';
import { MqttTopic } from './MqttTopic';
import { ISubscriptionMap, AsyncMqttClient, IClientOptions, IClientPublishOptions } from 'async-mqtt';


interface PresenceParams extends TopicParams {
  domain: string;
  clientConsumer: string[];
}
const PRESENCE_TOPIC = new MqttTopic<PresenceParams>(
  ['__presence', { '+': 'domain' }, { '#': 'clientConsumer' }]);
const PRESENCE_OPTS: Required<Pick<IClientPublishOptions, 'qos' | 'retain'>> = { qos: 1, retain: true };

export class MqttPresence {
  private readonly presenceTopic: MqttTopic<PresenceParams>;
  private readonly presentMap: { [clientId: string]: { [consumerId: string]: string } } = {};

  constructor(domain: string, private readonly clientId: string) {
    this.presenceTopic = PRESENCE_TOPIC.with({ domain });
  }

  get will(): IClientOptions['will'] {
    return {
      ...PRESENCE_OPTS,
      topic: this.presenceTopic.with({ clientConsumer: [this.clientId] }).address,
      payload: '-'
    };
  }

  get subscriptions(): ISubscriptionMap {
    const subscriptions: ISubscriptionMap = {};
    subscriptions[this.presenceTopic.address] = 1;
    return subscriptions;
  }

  async join(mqtt: AsyncMqttClient, consumerId: string, address: string) {
    await mqtt.publish(this.presenceTopic.with({
      clientConsumer: [this.clientId, consumerId]
    }).address, address, PRESENCE_OPTS);
  }

  async leave(mqtt: AsyncMqttClient, consumerId?: string) {
    await mqtt.publish(this.presenceTopic.with({
      clientConsumer: consumerId ? [this.clientId, consumerId] : [this.clientId]
    }).address, '-', PRESENCE_OPTS);
  }

  present(address: string): Set<string> {
    const rtn = new Set<string>();
    Object.keys(this.presentMap).forEach(clientId => {
      Object.keys(this.presentMap[clientId]).forEach(consumerId => {
        if (matches(this.presentMap[clientId][consumerId], address))
          rtn.add(consumerId);
      });
    });
    return rtn;
  }

  onMessage(topic: string, payload: Buffer) {
    this.presenceTopic.match(topic, presence => {
      const address = payload.toString(),
        [clientId, consumerId] = presence.clientConsumer;
      if (address === '-') {
        if (consumerId && this.presentMap[clientId]) {
          delete this.presentMap[clientId][consumerId];
          if (!Object.keys(this.presentMap[clientId]).length)
            delete this.presentMap[clientId];
        } else if (!consumerId) {
          delete this.presentMap[clientId];
        }
      } else if (consumerId) {
        this.ensureClientPresence(clientId)[consumerId] = address;
      }
    });
  }

  private ensureClientPresence(clientId: string) {
    return this.presentMap[clientId] || (this.presentMap[clientId] = {});
  }
}
