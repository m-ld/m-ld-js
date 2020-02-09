import { TopicParams, matches } from 'mqtt-pattern';
import { MqttTopic } from './MqttTopic';
import { ISubscriptionMap, AsyncMqttClient } from 'async-mqtt';


interface PresenceParams extends TopicParams {
  domain: string;
  clientConsumer: string[];
}
const PRESENCE_TOPIC = new MqttTopic<PresenceParams>(
  ['__presence', { '+': 'domain' }, { '#': 'clientConsumer' }]);

export class MqttPresence {
  private readonly presenceTopic: MqttTopic<PresenceParams>;
  private readonly presentMap: { [clientId: string]: { [consumerId: string]: string } } = {};

  constructor(domain: string) {
    this.presenceTopic = PRESENCE_TOPIC.with({ domain });
  }

  get subscriptions(): ISubscriptionMap {
    const subscriptions: ISubscriptionMap = {};
    subscriptions[this.presenceTopic.address] = 1;
    return subscriptions;
  }

  async join(mqtt: AsyncMqttClient, clientId: string, consumerId: string, address: string) {
    await mqtt.publish(this.presenceTopic.with({
      clientConsumer: [clientId, consumerId]
    }).address, address);
  }

  async leave(mqtt: AsyncMqttClient, clientId: string, consumerId?: string) {
    await mqtt.publish(this.presenceTopic.with({
      clientConsumer: consumerId ? [clientId, consumerId] : [clientId]
    }).address, '-');
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
