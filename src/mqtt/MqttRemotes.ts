/**
 * [[include:mqtt-remotes.md]]
 * @module MqttRemotes
 * @internal
 */
import { Observable } from 'rxjs';
import {
  AsyncMqttClient, connect as defaultConnect, IClientOptions, ISubscriptionMap
} from 'async-mqtt';
import { MqttTopic, REPLY_TOPIC, SEND_TOPIC, SendAddressParams } from './MqttTopic';
import { TopicParams } from 'mqtt-pattern';
import { MqttPresence } from './MqttPresence';
import { MeldExtensions } from '../api';
import { NotifyParams, PubsubRemotes, ReplyParams, SendParams, SubPub } from '../engine/remotes';
import { MeldConfig } from '../config';

export interface MeldMqttConfig extends MeldConfig {
  mqtt?: Omit<IClientOptions, 'will' | 'clientId'> &
    ({ hostname: string } | { host: string, port: number })
}

interface DomainTopicParams extends TopicParams {
  domain: string;
}

interface NoEchoTopicParams extends DomainTopicParams {
  clientId: string;
}

interface NotifyTopicParams extends DomainTopicParams {
  channelId: string;
}

const OPERATIONS_TOPIC =
  new MqttTopic<NoEchoTopicParams>({ '+': 'domain' }, 'operations', { '+': 'clientId' });
const CONTROL_TOPIC =
  new MqttTopic<DomainTopicParams>({ '+': 'domain' }, 'control');
const NOTIFY_TOPIC =
  new MqttTopic<NotifyTopicParams>({ '+': 'domain' }, 'control', { '+': 'channelId' });

export class MqttRemotes extends PubsubRemotes {
  private readonly mqtt: AsyncMqttClient;
  private readonly operationsTopic: MqttTopic<NoEchoTopicParams>;
  private readonly controlTopic: MqttTopic<DomainTopicParams>;
  private readonly notifyTopic: MqttTopic<NotifyTopicParams>;
  private readonly sentTopic: MqttTopic<SendAddressParams & TopicParams>;
  private readonly replyTopic: MqttTopic<ReplyParams & TopicParams>;
  private readonly presence: MqttPresence;

  constructor(
    config: MeldMqttConfig,
    extensions: () => Promise<MeldExtensions>,
    connect = defaultConnect
  ) {
    super(config, extensions);

    const { id, domain } = this;
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
          this.onOperation(payload);
      });
      this.sentTopic.match(topic, sent => this.onSent(payload, sent));
      this.replyTopic.match(topic, replied => this.onReply(payload, replied));
      this.notifyTopic.match(topic, notify => this.onNotify(notify.channelId, payload));
    });

    // When MQTT.js receives an error just log - it will try to reconnect
    this.mqtt.on('error', this.warnError);

    // MQTT.js 'close' event signals a disconnect - definitely offline.
    this.mqtt.on('close', () => this.onDisconnect());
    this.mqtt.on('connect', () => this.onConnect());
  }

  async close(err?: any) {
    try {
      await super.close(err);
      await this.mqtt.end();
    } catch (e) {
      this.warnError(e);
    }
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
        // noinspection ExceptionCaughtLocallyJS
        throw new Error('Requested QoS was not granted');
      // We don't have to wait for the presence to initialise
      this.presence.initialise().catch(this.warnError);
      await super.onConnect();
    } catch (err) {
      if (this.mqtt.connected)
        this.close(err).catch(this.warnError); // This is a catastrophe, can't bootstrap
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

  protected publishOperation(msg: Buffer): Promise<unknown> {
    return this.mqtt.publish(
      // Client Id is included to prevent echo
      this.operationsTopic.with({ clientId: this.id }).address, msg, { qos: 1 });
  }

  protected present(): Observable<string> {
    return this.presence.present(this.controlTopic.address);
  }

  protected async notifier({ channelId, toId }: NotifyParams): Promise<SubPub> {
    const address = this.notifyTopic.with({ channelId }).address;
    if (toId === this.id)
      await this.mqtt.subscribe(address, { qos: 1 });
    return {
      id: channelId,
      publish: notification => this.mqtt.publish(address, notification),
      close: () => this.mqtt.unsubscribe(address).catch(this.warnError)
    };
  }

  protected sender(params: SendParams): SubPub {
    const address = SEND_TOPIC.with({
      ...params, address: this.controlTopic.path
    }).address;
    return {
      id: address,
      publish: request => this.mqtt.publish(address, request)
    };
  }

  protected replier(params: ReplyParams): SubPub {
    const address = REPLY_TOPIC.with({ ...params }).address;
    return {
      id: address,
      publish: res => this.mqtt.publish(address, res)
    };
  }
}
