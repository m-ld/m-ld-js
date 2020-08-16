import { TopicParams, matches } from 'mqtt-pattern';
import { MqttTopic } from './MqttTopic';
import { AsyncMqttClient, IClientOptions, IClientPublishOptions, ISubscriptionMap } from 'async-mqtt';
import { jsonFrom } from '../engine/util';
import { EventEmitter } from 'events';
import { BehaviorSubject, identity, Observable } from 'rxjs';
import { first, filter, flatMap } from 'rxjs/operators';

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
const GHOST_PAYLOAD = '-';

export class MqttPresence extends EventEmitter {
  private readonly clientTopic: MqttTopic<PresenceParams>;
  private readonly domainTopic: MqttTopic<PresenceParams>;
  private readonly presence: { [clientId: string]: { [consumerId: string]: string } } = {};
  private readonly ready = new BehaviorSubject<boolean>(false);
  private ghosts = 0;

  constructor(private readonly mqtt: AsyncMqttClient, domain: string, private readonly clientId: string) {
    super();

    this.domainTopic = PRESENCE_TOPIC.with({ domain });
    this.clientTopic = this.domainTopic.with({ client: clientId });

    mqtt.on('close', () => {
      Object.keys(this.presence).forEach(clientId => delete this.presence[clientId]);
      this.ready.next(false);
    });

    mqtt.on('message', (topic, payload) => {
      this.domainTopic.match(topic, presence => {
        this.ready.pipe(first()).subscribe(ready => {
          if (payload.toString() === GHOST_PAYLOAD) {
            if (presence.client === this.clientId && --this.ghosts === 0) {
              this.ready.next(true);
              this.emit('change');
            } else if (!ready && presence.client < this.clientId) {
              // Someone else is also bootstrapping. Try to avoid a race by trying again
              this.publishGhost().catch(this.errored);
            }
          } else {
            if (payload.toString() === LEAVE_PAYLOAD) {
              this.left(presence.client);
            } else {
              this.presence[presence.client] = jsonFrom(payload);
            }
            if (ready)
              this.emit('change');
          }
        });
      });
    });
  }

  // Do not subscribe; MQTT.js seems to allow only one concurrent subscription
  get subscriptions(): ISubscriptionMap {
    return { [this.domainTopic.address]: { qos: 1 } };
  }

  async initialise() {
    this.publishGhost();
  }

  static will(domain: string, client: string): IClientOptions['will'] {
    return {
      ...PRESENCE_OPTS,
      topic: PRESENCE_TOPIC.with({ domain, client }).address,
      payload: LEAVE_PAYLOAD
    };
  }

  on(event: 'error', listener: (err: any) => void): this;
  on(event: 'change', listener: () => void): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  join(consumerId: string, address: string): Promise<void> {
    // Wait until we are ready
    return this.ready.pipe(filter(identity), first(), flatMap(async () => {
      const myConsumers = this.presence[this.clientId] || (this.presence[this.clientId] = {});
      myConsumers[consumerId] = address;
      await this.mqtt.publish(this.clientTopic.address, JSON.stringify(myConsumers), PRESENCE_OPTS);
    })).toPromise();
  }

  async leave(consumerId?: string) {
    this.left(this.clientId, consumerId);
    const myConsumers = this.presence[this.clientId];
    await this.mqtt.publish(this.clientTopic.address,
      myConsumers ? JSON.stringify(myConsumers) : LEAVE_PAYLOAD, PRESENCE_OPTS);
  }

  present(address: string): Observable<string> {
    return this.ready.pipe(filter(identity), first(), flatMap(() =>
      new Observable<string>(subs => {
        for (let clientId in this.presence) {
          for (let consumerId in this.presence[clientId]) {
            if (matches(this.presence[clientId][consumerId], address))
              subs.next(consumerId);
          }
        }
        subs.complete();
      })));
  }

  private async publishGhost() {
    this.ghosts++;
    await this.mqtt.publish(this.clientTopic.address, GHOST_PAYLOAD, { qos: 1 });
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

  private errored = (err: any) => this.emit('error', err);
}
