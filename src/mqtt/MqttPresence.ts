import { TopicParams, matches } from 'mqtt-pattern';
import { MqttTopic } from './MqttTopic';
import { ISubscriptionMap, AsyncMqttClient, IClientOptions, IClientPublishOptions } from 'async-mqtt';
import { jsonFrom } from '../util';


interface PresenceParams extends TopicParams {
  domain: string;
  client: string;
}
const PRESENCE_TOPIC = new MqttTopic<PresenceParams>(
  ['__presence', { '+': 'domain' }, { '+': 'client' }]);
const PRESENCE_OPTS: Required<Pick<IClientPublishOptions, 'qos' | 'retain'>> =
  { qos: 1, retain: true };
/**
 * Leave payload is empty, so that it's not retained
 * @see http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349265
 */
const LEAVE_PAYLOAD = '';

export class MqttPresence {
  private readonly domainTopic: MqttTopic<PresenceParams>;
  private readonly clientTopic: MqttTopic<PresenceParams>;
  private readonly presence: { [clientId: string]: { [consumerId: string]: string } } = {};

  constructor(domain: string, private readonly clientId: string) {
    this.domainTopic = PRESENCE_TOPIC.with({ domain });
    this.clientTopic = this.domainTopic.with({ client: clientId });
  }

  get will(): IClientOptions['will'] {
    return {
      ...PRESENCE_OPTS,
      topic: this.clientTopic.address,
      payload: LEAVE_PAYLOAD
    };
  }

  get subscriptions(): ISubscriptionMap {
    const subscriptions: ISubscriptionMap = {};
    subscriptions[this.domainTopic.address] = 1;
    return subscriptions;
  }

  async join(mqtt: AsyncMqttClient, consumerId: string, address: string) {
    const myConsumers = this.presence[this.clientId] || (this.presence[this.clientId] = {});
    myConsumers[consumerId] = address;
    await mqtt.publish(this.clientTopic.address, JSON.stringify(myConsumers), PRESENCE_OPTS);
  }

  async leave(mqtt: AsyncMqttClient, consumerId: string) {
    this.left(this.clientId, consumerId);
    const myConsumers = this.presence[this.clientId];
    await mqtt.publish(this.clientTopic.address,
      myConsumers ? JSON.stringify(myConsumers) : LEAVE_PAYLOAD, PRESENCE_OPTS);
  }

  *present(address: string): IterableIterator<string> {
    for (let clientId in this.presence) {
      for (let consumerId in this.presence[clientId]) {
        if (matches(this.presence[clientId][consumerId], address))
          yield consumerId;
      };
    }
  }

  onMessage(topic: string, payload: Buffer): boolean {
    const presence = this.domainTopic.match(topic);
    if (presence) {
      if (payload.toString() === LEAVE_PAYLOAD) {
        this.left(presence.client);
      } else {
        this.presence[presence.client] = jsonFrom(payload);
      }
      return true;
    }
    return false;
  }

  private left(clientId: string, consumerId?: string) {
    if (consumerId && this.presence[clientId]) {
      delete this.presence[clientId][consumerId];
      if (!Object.keys(this.presence[clientId]).length)
        delete this.presence[clientId];
    } else if (!consumerId) {
      delete this.presence[clientId];
    }
  }
}
