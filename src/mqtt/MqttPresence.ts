import { matches, TopicParams } from 'mqtt-pattern';
import { MqttTopic } from './MqttTopic';
import {
  AsyncMqttClient, IClientOptions, IClientPublishOptions, ISubscriptionMap
} from 'async-mqtt';
import { Future, getIdLogger, inflate } from '../engine/util';
import { EventEmitter } from 'events';
import { Observable } from 'rxjs';
import { Logger, LogLevelDesc } from 'loglevel';

interface PresenceParams extends TopicParams {
  domain: string;
  client: string;
}

const PRESENCE_TOPIC = new MqttTopic<PresenceParams>(
  '__presence', { '+': 'domain' }, { '+': 'client' });
const PRESENCE_OPTS: Required<Pick<IClientPublishOptions, 'qos' | 'retain'>> =
  { qos: 1, retain: true };
/**
 * Leave payload is empty, so that it's not retained
 * @see http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349265
 */
const LEAVE_PAYLOAD = '';
/**
 * Used to ensure that retained presence messages are processed
 */
const GHOST_PAYLOAD = '-';

export class MqttPresence extends EventEmitter {
  private readonly clientTopic: MqttTopic<PresenceParams>;
  private readonly domainTopic: MqttTopic<PresenceParams>;
  private readonly presence: { [clientId: string]: { [consumerId: string]: string } } = {};
  private readonly ready = new Future;
  private readonly log: Logger;

  constructor(
    private readonly mqtt: AsyncMqttClient,
    domain: string,
    private readonly clientId: string,
    logLevel: LogLevelDesc = 'info') {
    super();
    this.log = getIdLogger(this.constructor, clientId, logLevel);

    this.domainTopic = PRESENCE_TOPIC.with({ domain });
    this.clientTopic = this.domainTopic.with({ client: clientId });

    mqtt.on('close', () =>
      Object.keys(this.presence).forEach(clientId => delete this.presence[clientId]));

    mqtt.on('message', (topic, payload) => {
      this.domainTopic.match(topic, presence => {
        const payloadStr = payload.toString();
        if (payloadStr === GHOST_PAYLOAD) {
          if (presence.client === this.clientId)
            this.ready.resolve();
        } else if (payloadStr === LEAVE_PAYLOAD) {
          this.log.debug('Has left:', presence.client);
          this.left(presence.client);
        } else {
          this.log.debug('Has arrived:', presence.client);
          this.presence[presence.client] = JSON.parse(payloadStr);
        }
        if (!this.ready.pending)
          this.emit('change');
      });
    });
  }

  // Do not subscribe; MQTT.js seems to allow only one concurrent subscription
  get subscriptions(): ISubscriptionMap {
    return { [this.domainTopic.address]: { qos: 1 } };
  }

  initialise(): Promise<unknown> {
    return this.mqtt.publish(this.clientTopic.address, GHOST_PAYLOAD, { qos: 1 });
  }

  static will(domain: string, client: string): IClientOptions['will'] {
    return {
      ...PRESENCE_OPTS,
      topic: PRESENCE_TOPIC.with({ domain, client }).address,
      payload: LEAVE_PAYLOAD
    };
  }

  on(event: 'change', listener: () => void): this {
    return super.on(event, listener);
  }

  async join(consumerId: string, address: string): Promise<unknown> {
    if (this.ready.pending) // Convenience for tests: makes this method sync
      await this.ready;
    const myConsumers = this.presence[this.clientId] || (this.presence[this.clientId] = {});
    myConsumers[consumerId] = address;
    return this.mqtt.publish(this.clientTopic.address, JSON.stringify(myConsumers), PRESENCE_OPTS);
  }

  async leave(consumerId?: string) {
    this.left(this.clientId, consumerId);
    const myConsumers = this.presence[this.clientId];
    await this.mqtt.publish(this.clientTopic.address,
      myConsumers ? JSON.stringify(myConsumers) : LEAVE_PAYLOAD, PRESENCE_OPTS);
  }

  present(address: string): Observable<string> {
    return inflate(this.ready, () =>
      new Observable<string>(subs => {
        for (let clientId in this.presence) {
          for (let consumerId in this.presence[clientId]) {
            if (matches(this.presence[clientId][consumerId], address))
              subs.next(consumerId);
          }
        }
        subs.complete();
      }));
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
