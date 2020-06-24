import { DeltaMessage } from '../m-ld';
import { Observable } from 'rxjs';
import { AsyncMqttClient, IClientOptions, ISubscriptionMap, connect as defaultConnect } from 'async-mqtt';
import { MqttTopic, SEND_TOPIC, REPLY_TOPIC, } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { Response, Request } from '../m-ld/ControlMessage';
import { jsonFrom } from '../util';
import { MeldConfig } from '..';
import { SendParams, ReplyParams, PubsubRemotes, SubPubsub, JsonNotification, SubPub } from '../PubsubRemotes';

export interface MeldMqttConfig extends MeldConfig {
  /**
   * Options required for the MQTT driver. These must not include the `will` and
   * `clientId` options as these are generated internally. They must include a
   * `hostname` _or_ a `host` and `port`.
   * @see https://www.npmjs.com/package/mqtt#client
   */
  mqttOpts?: Omit<IClientOptions, 'will' | 'clientId'> & ({ hostname: string } | { host: string, port: number })
}

interface DomainParams extends TopicParams { domain: string; }
interface NotifyParams extends DomainParams { id: string; }
const OPERATIONS_TOPIC = new MqttTopic<DomainParams>({ '+': 'domain' }, 'operations');
const CONTROL_TOPIC = new MqttTopic<DomainParams>({ '+': 'domain' }, 'control');
const NOTIFY_TOPIC = new MqttTopic<NotifyParams>({ '+': 'domain' }, 'control', { '+': 'id' });
const CHANNEL_ID_HEADER = '__channel.id';

export class MqttRemotes extends PubsubRemotes {
  private readonly mqtt: AsyncMqttClient;
  private readonly operationsTopic: MqttTopic<DomainParams>;
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
    this.mqtt = connect({ ...config.mqttOpts, clientId: id, will: MqttPresence.will(domain, id) });
    this.presence = new MqttPresence(this.mqtt, domain, id);

    // Set up listeners
    this.presence.on('change', () => this.onPresenceChange());
    this.mqtt.on('message', (topic, payload) => {
      this.operationsTopic.match(topic, () => this.onRemoteUpdate(jsonFrom(payload)));
      this.sentTopic.match(topic, sent => this.onSent(jsonFrom(payload), sent));
      this.replyTopic.match(topic, replied => this.onReply(jsonFrom(payload), replied));
      this.notifyTopic.match(topic, notify => this.onNotify(notify.id, jsonFrom(payload)));
    });

    // When MQTT.js receives an error just log - it will try to reconnect
    this.mqtt.on('error', this.warnError);
    this.presence.on('error', this.warnError);

    // MQTT.js 'close' event signals a disconnect - definitely offline.
    this.mqtt.on('close', () => this.setOnline(null));
    this.mqtt.on('connect', () => this.onConnect());
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

  protected reconnect(): void {
    this.mqtt.reconnect();
  }

  protected async clonePresent(online: boolean) {
    if (this.mqtt.connected) {
      if (online)
        return this.presence.join(this.id, this.controlTopic.address);
      else
        return this.presence.leave(this.id);
    }
  }

  protected publishDelta(msg: DeltaMessage): Promise<unknown> {
    return this.mqtt.publish(
      this.operationsTopic.address, JSON.stringify({
        ...msg.toJson(),
        [CHANNEL_ID_HEADER]: this.id
      }), { qos: 1 });
  }

  protected present(): Observable<string> {
    return this.presence.present(this.controlTopic.address);
  }

  protected notifier(id: string): SubPubsub<JsonNotification> {
    const address = this.notifyTopic.with({ id }).address;
    return {
      id,
      publish: notification => this.mqtt.publish(address, JSON.stringify(notification)),
      subscribe: () => this.mqtt.subscribe(address, { qos: 1 }),
      unsubscribe: () => this.mqtt.unsubscribe(address)
    };
  }

  protected sender(toId: string, messageId: string): SubPub<Request> {
    const address = SEND_TOPIC.with({
      toId, fromId: this.id, messageId, address: this.controlTopic.path
    }).address;
    return {
      id: address,
      publish: request => this.mqtt.publish(address, JSON.stringify(request.toJson()))
    };
  }

  protected replier(messageId: string, toId: string, sentMessageId: string): SubPub<Response | null> {
    const address = REPLY_TOPIC.with({
      messageId, fromId: this.id, toId, sentMessageId
    }).address;
    return {
      id: address,
      publish: res => this.mqtt.publish(address, JSON.stringify(res != null ? res.toJson() : null))
    };
  }

  protected isEcho(json: any): boolean {
    return json[CHANNEL_ID_HEADER] && json[CHANNEL_ID_HEADER] === this.id;
  }

  async close(err?: any) {
    await super.close(err);
    await this.mqtt.end();
  }
}
