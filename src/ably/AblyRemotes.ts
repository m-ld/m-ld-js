/**
 * [[include:ably-remotes.md]]
 * @packageDocumentation
 * @internal
 */
import * as Ably from 'ably';
import { MeldConfig } from '..';
import { PubsubRemotes, SubPubsub, SubPub, DirectParams, ReplyParams } from '../engine/PubsubRemotes';
import { Observable, from, identity } from 'rxjs';
import { flatMap, filter, map } from 'rxjs/operators';
import { AblyTraffic, AblyTrafficConfig } from './AblyTraffic';

export interface AblyMeldConfig extends
  Omit<Ably.Types.ClientOptions, 'echoMessages' | 'clientId'>,
  AblyTrafficConfig {
}

export interface MeldAblyConfig extends MeldConfig {
  ably: AblyMeldConfig;
}

interface SendTypeParams extends DirectParams { type: '__send'; }
interface ReplyTypeParams extends ReplyParams { type: '__reply'; }
interface NotifyTypeParams { type: '__notify'; toId: string; id: string; }
type DirectTypeParams = SendTypeParams | ReplyTypeParams | NotifyTypeParams;

export class AblyRemotes extends PubsubRemotes {
  private readonly client: Ably.Types.RealtimePromise;
  private readonly operations: Ably.Types.RealtimeChannelPromise;
  private readonly direct: Ably.Types.RealtimeChannelPromise;
  private readonly traffic: AblyTraffic;
  private readonly subscribed: Promise<unknown>;

  constructor(config: MeldAblyConfig,
    connect: (opts: Ably.Types.ClientOptions) => Ably.Types.RealtimePromise
      = opts => new Ably.Realtime.Promise(opts)) {
    super(config);
    this.client = connect({ ...config.ably, echoMessages: false, clientId: config['@id'] });
    this.operations = this.client.channels.get(this.channelName('operations'));
    this.traffic = new AblyTraffic(config.ably);
    // Direct channel that is specific to us, for sending and replying to
    // requests and receiving notifications
    this.direct = this.client.channels.get(this.channelName(config['@id']));
    // Ensure we are fully subscribed before we make any presence claims
    this.subscribed = Promise.all([
      this.traffic.subscribe(this.operations, data => this.onRemoteUpdate(data)),
      this.operations.presence.subscribe(() => this.onPresenceChange()),
      this.traffic.subscribe(this.direct, this.onDirectMessage)
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

  private channelName(id: string) {
    // https://www.ably.io/documentation/realtime/channels#channel-namespaces
    return `${this.meldEncoding.domain}:${id}`;
  }

  private onDirectMessage = async (data: any, name: string, clientId: string) => {
    try {
      const params = this.getParams(name, clientId);
      switch (params?.type) {
        case '__send':
          await this.onSent(data, params);
          break;
        case '__reply':
          await this.onReply(data, params);
          break;
        case '__notify':
          this.onNotify(params.id, data);
      }
    } catch (err) {
      this.warnError(err);
    }
  };

  protected setPresent(present: boolean): Promise<unknown> {
    if (present)
      return this.operations.presence.update('__live');
    else
      return this.operations.presence.leave();
  }

  protected publishDelta(msg: Buffer): Promise<unknown> {
    return this.traffic.publish(this.operations, '__delta', msg);
  }

  protected present(): Observable<string> {
    return from(this.operations.presence.get()).pipe(
      flatMap(identity), // flatten the array of presence messages
      filter(present => present.data === '__live'),
      map(present => present.clientId));
  }

  protected notifier(toId: string, id: string): SubPubsub {
    return this.directSubPub({ type: '__notify', toId, id });
  }

  protected sender(toId: string, messageId: string): SubPub {
    return this.directSubPub({ type: '__send', toId, messageId, fromId: this.id });
  }

  protected replier(toId: string, messageId: string, sentMessageId: string): SubPub {
    return this.directSubPub({ type: '__reply', toId, messageId, sentMessageId, fromId: this.id });
  }

  private getParams(name: string, clientId: string): DirectTypeParams | undefined {
    // Message name is concatenated type:id:sentMessageId, where id is type-specific info
    const [type, id, sentMessageId] = name.split(':');
    const params = { fromId: clientId, toId: this.id };
    switch (type) {
      case '__send': return { type, messageId: id, ...params };
      case '__reply': return { type, messageId: id, sentMessageId, ...params };
      case '__notify': return { type, id, ...params };
    }
  }

  private directSubPub(params: DirectTypeParams): SubPubsub {
    const channelName = this.channelName(params.toId);
    const channel = this.client.channels.get(channelName);
    const [id, name] = (function () {
      switch (params.type) {
        case '__send': return [params.toId, `__send:${params.messageId}`];
        case '__reply': return [params.toId, `__reply:${params.messageId}:${params.sentMessageId}`];
        case '__notify': return [params.id, `__notify:${params.id}`];
      }
    })();
    return {
      // Ensure we are subscribed before sending anything, or we won't get the reply
      id, publish: msg => this.subscribed.then(() => this.traffic.publish(channel, name, msg)),
      // Never need to subscribe since we are just using our direct channel
      subscribe: async () => null, unsubscribe: async () => null
    };
  }
}