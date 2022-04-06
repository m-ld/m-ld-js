/**
 * [[include:ably-remotes.md]]
 * @module AblyRemotes
 * @internal
 */
import * as Ably from 'ably';
import { MeldExtensions } from '../api';
import {
  NotifyParams, PeerParams, PubsubRemotes, ReplyParams, SendParams, SubPub
} from '../engine/remotes';
import { Observable } from 'rxjs';
import { filter, map } from 'rxjs/operators';
import { AblyTraffic, AblyTrafficConfig } from './AblyTraffic';
import { inflateFrom } from '../engine/util';
import { MeldConfig } from '../config';

export interface AblyMeldConfig extends
  Omit<Ably.Types.ClientOptions, 'echoMessages' | 'clientId'>,
  AblyTrafficConfig {
}

export interface MeldAblyConfig extends MeldConfig {
  ably: AblyMeldConfig;
}

export const ablyConnect =
  (opts: Ably.Types.ClientOptions) => new Ably.Realtime.Promise(opts);

interface SendTypeParams extends SendParams {type: '__send';}
interface ReplyTypeParams extends ReplyParams {type: '__reply';}
interface NotifyTypeParams extends NotifyParams { type: '__notify' }
interface SignalTypeParams extends PeerParams { type: '__signal'; channelId: string; }
type PeerTypeParams = SendTypeParams | ReplyTypeParams | NotifyTypeParams | SignalTypeParams;

export class AblyRemotes extends PubsubRemotes {
  private readonly client: Ably.Types.RealtimePromise;
  private readonly opsChannel: Ably.Types.RealtimeChannelPromise;
  private readonly directChannel: Ably.Types.RealtimeChannelPromise;
  private readonly traffic: AblyTraffic;
  private readonly subscribed: Promise<unknown>;

  constructor(
    config: MeldAblyConfig,
    extensions: () => Promise<MeldExtensions>,
    connect = ablyConnect
  ) {
    super(config, extensions);
    this.client = connect({ ...config.ably, echoMessages: false, clientId: config['@id'] });
    this.opsChannel = this.channel('operations');
    this.traffic = new AblyTraffic(config.ably);
    // Direct channel that is specific to us, for sending and replying to
    // requests and receiving notifications
    this.directChannel = this.channel(config['@id']);
    // Ensure we are fully subscribed before we make any presence claims
    this.subscribed = Promise.all([
      this.traffic.subscribe(this.opsChannel, data => this.onOperation(data)),
      this.opsChannel.presence.subscribe(() => this.onPresenceChange()),
      this.traffic.subscribe(this.directChannel, this.onDirectMessage)
    ]).catch(err => this.close(err));
    // Ably does not notify if no-one around, so check presence once subscribed
    this.subscribed.then(() => this.onPresenceChange());
    // Note that we wait for subscription before claiming to be connected.
    // This is so we don't miss messages that are immediately sent to us.
    // https://support.ably.com/support/solutions/articles/3000067435
    this.client.connection.on('connected', () =>
      this.subscribed.then(() => this.onConnect()).catch(this.warnError));
    // Ably has connection recovery with no message loss for 2min. During that
    // time we treat the remotes as live. After that, the connection becomes
    // suspended and we are offline.
    this.client.connection.on(['suspended', 'failed', 'closing'], () => this.onDisconnect());
  }

  async close(err?: any) {
    await super.close(err);
    this.client.connection.close();
  }

  protected channel(id: string) {
    // https://www.ably.io/documentation/realtime/channels#channel-namespaces
    return this.client.channels.get(`${this.domain}:${id}`);
  }

  private onDirectMessage = async (data: any, name: string, clientId: string) => {
    try {
      // Message name is concatenated type:id:sentMessageId, where id is type-specific
      const [type, id, sentMessageId] = name.split(':');
      const params = { fromId: clientId, toId: this.id };
      switch (type) {
        case '__send':
          await this.onSent(data, { messageId: id, ...params });
          break;
        case '__reply':
          await this.onReply(data, { messageId: id, sentMessageId, ...params });
          break;
        case '__notify':
          this.onNotify(id, data);
          break;
        case '__signal':
          this.onSignal(id, clientId, data);
      }
    } catch (err) {
      this.warnError(err);
    }
  };

  private static fromParams(params: PeerTypeParams): { msgName: string, subPubId: string } {
    switch (params.type) {
      case '__send':
        return {
          subPubId: params.toId,
          msgName: `__send:${params.messageId}`
        };
      case '__reply':
        return {
          subPubId: params.toId,
          msgName: `__reply:${params.messageId}:${params.sentMessageId}`
        };
      case '__notify':
        return {
          subPubId: params.channelId,
          msgName: `__notify:${params.channelId}`
        };
      case '__signal':
        return {
          subPubId: params.channelId,
          msgName: `__signal:${params.channelId}`
        };
    }
  }

  protected setPresent(present: boolean): Promise<unknown> {
    if (present)
      return this.opsChannel.presence.update('__live');
    else
      return this.opsChannel.presence.leave();
  }

  protected publishOperation(msg: Buffer): Promise<unknown> {
    return this.traffic.publish(this.opsChannel, '__op', msg);
  }

  protected present(): Observable<string> {
    return inflateFrom(this.opsChannel.presence.get()).pipe(
      filter(present => present.data === '__live'),
      map(present => present.clientId));
  }

  protected async notifier(params: NotifyParams): Promise<SubPub> {
    // Try to create a peer-to-peer notifier
    return (await this.peerSubPub(params)) ??
      this.directSubPub({ type: '__notify', ...params });
  }

  protected sender(params: SendParams): SubPub {
    return this.directSubPub({ type: '__send', ...params });
  }

  protected replier(params: ReplyParams): SubPub {
    return this.directSubPub({ type: '__reply', ...params });
  }

  private directSubPub(params: PeerTypeParams): SubPub {
    const channel = this.channel(params.toId);
    const { subPubId, msgName } = AblyRemotes.fromParams(params);
    return {
      id: subPubId,
      publish: msg => this.duplexPublish(channel, msgName, msg)
    };
  }

  /** "duplex" because expecting a response */
  private async duplexPublish(
    channel: Ably.Types.RealtimeChannelPromise,
    name: string,
    msg: Buffer | object
  ): Promise<unknown> {
    // Ensure we are subscribed before sending anything, or we won't get the reply
    await this.subscribed;
    return this.traffic.publish(channel, name, msg);
  }

  protected async peerSubPub(params: NotifyParams): Promise<SubPub | undefined> {
    // For outbound notifications, preempt a target who expects to peer
    if (params.fromId === this.id)
      await this.signal(params.toId, params.channelId, { unavailable: true });
    return undefined;
  }

  protected signal(peerId: string, channelId: string, data: object) {
    const { msgName } = AblyRemotes.fromParams({
      type: '__signal', channelId, fromId: this.id, toId: peerId
    });
    return this.duplexPublish(this.channel(peerId), msgName, data);
  }

  protected onSignal(channelId: string, fromId: string, data: object) {
    // If someone is saying they are unavailable for peering, that's OK with us
    if (!('unavailable' in data))
      // Otherwise someone is trying to peer with us, but we can't
      this.signal(fromId, channelId, { unavailable: true }).catch(this.warnError);
  }
}

