/**
 * [[include:mqtt-remotes.md]]
 * @packageDocumentation
 */
import { Observable } from 'rxjs';
import { AsyncMqttClient, IClientOptions, ISubscriptionMap, connect as defaultConnect } from 'async-mqtt';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, SendParams, } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { MeldConfig } from '..';
import { ReplyParams, PubsubRemotes, SubPubsub, SubPub } from '../engine/PubsubRemotes';

export interface MeldMqttConfig extends MeldConfig {
  mqtt?: Omit<IClientOptions, 'will' | 'clientId'> & ({ hostname: string } | { host: string, port: number })
}

interface DomainParams extends TopicParams { domain: string; }
interface NoEchoParams extends DomainParams { clientId: string; }
interface NotifyParams extends DomainParams { id: string; }
const OPERATIONS_TOPIC = new MqttTopic<NoEchoParams>({ '+': 'domain' }, 'operations', { '+': 'clientId' });
const CONTROL_TOPIC = new MqttTopic<DomainParams>({ '+': 'domain' }, 'control');
const NOTIFY_TOPIC = new MqttTopic<NotifyParams>({ '+': 'domain' }, 'control', { '+': 'id' });

export class MqttRemotes extends PubsubRemotes {
  private readonly mqtt: AsyncMqttClient;
  private readonly operationsTopic: MqttTopic<NoEchoParams>;
  private readonly controlTopic: MqttTopic<DomainParams>;
  private readonly notifyTopic: MqttTopic<NotifyParams>;
  private readonly sentTopic: MqttTopic<SendParams & TopicParams>;
  private readonly replyTopic: MqttTopic<ReplyParams & TopicParams>;
  private readonly presence: MqttPresence;

  constructor(config: MeldMqttConfig,
    connect: (opts: IClientOptions) => AsyncMqttClient = defaultConnect) {
    super(config);

    const id = config['@id'], domain = config['@domain'];
    this.operationsTopic = OPERATIONS_TOPIC.with({ domain });
    this.controlTopic = CONTROL_TOPIC.with({ domain });
    this.notifyTopic = NOTIFY_TOPIC.with({ domain });
    // We only listen for control requests
    this.sentTopic = SEND_TOPIC.with({ toId: this.id, address: this.controlTopic.path });
    this.replyTopic = REPLY_TOPIC.with({ toId: this.id });
    this.mqtt = connect({ ...config.mqtt, clientId: id, will: MqttPresence.will(domain, id) });
    this.presence = new MqttPresence(this.mqtt, domain, id, config.logLevel);

    // Set up listeners
    this.presence.on('change', () => this.onPresenceChange());
    this.mqtt.on('message', (topic, payload) => {
      this.operationsTopic.match(topic, params => {
        if (params.clientId !== this.id) // Prevent echo
          this.onRemoteUpdate(payload);
      });
      this.sentTopic.match(topic, sent => this.onSent(payload, sent));
      this.replyTopic.match(topic, replied => this.onReply(payload, replied));
      this.notifyTopic.match(topic, notify => this.onNotify(notify.id, payload));
    });

    // When MQTT.js receives an error just log - it will try to reconnect
    this.mqtt.on('error', this.warnError);

    // MQTT.js 'close' event signals a disconnect - definitely offline.
    this.mqtt.on('close', () => this.onDisconnect());
    this.mqtt.on('connect', () => this.onConnect());
  }

  async close(err?: any) {
    await super.close(err);
    await this.mqtt.end();
  }

  protected async onConnect() {
    try {
      const subscriptions: ISubscriptionMap = {
        ...this.presence.subscriptions,
        [this.operationsTopic.address]: { qos: 1 },
        [this.controlTopic.address]: { qos: 1 },
        [this.sentTopic.address]: { qos: 0 },
        [this.replyTopic.address]: { qos: 0 }
      };
      const grants = await this.mqtt.subscribe(subscriptions);
      if (!grants.every(grant => subscriptions[grant.topic].qos == grant.qos))
        throw new Error('Requested QoS was not granted');
      // We don't have to wait for the presence to initialise
      this.presence.initialise().catch(this.warnError);
      await super.onConnect();
    }
    catch (err) {
      if (this.mqtt.connected)
        this.close(err); // This is a catastrophe, can't bootstrap
      else
        this.log.debug(err);
    }
  }

  protected async setPresent(present: boolean) {
    if (present)
      return this.presence.join(this.id, this.controlTopic.address);
    else
      return this.presence.leave(this.id);
  }

  protected publishDelta(msg: Buffer): Promise<unknown> {
    return this.mqtt.publish(
      // Client Id is included to prevent echo
      this.operationsTopic.with({ clientId: this.id }).address, msg, { qos: 1 });
  }

  protected present(): Observable<string> {
    return this.presence.present(this.controlTopic.address);
  }

  protected notifier(_toId: string, id: string): SubPubsub {
    const address = this.notifyTopic.with({ id }).address;
    return {
      id,
      publish: notification => this.mqtt.publish(address, notification),
      subscribe: () => this.mqtt.subscribe(address, { qos: 1 }),
      unsubscribe: () => this.mqtt.unsubscribe(address)
    };
  }

  protected sender(toId: string, messageId: string): SubPub {
    const address = SEND_TOPIC.with({
      toId, fromId: this.id, messageId, address: this.controlTopic.path
    }).address;
    return {
      id: address,
      publish: request => this.mqtt.publish(address, request)
    };
  }

  protected replier(toId: string, messageId: string, sentMessageId: string): SubPub {
    const address = REPLY_TOPIC.with({
      messageId, fromId: this.id, toId, sentMessageId
    }).address;
    return {
      id: address,
      publish: res => this.mqtt.publish(address, res)
    };
  }
}
