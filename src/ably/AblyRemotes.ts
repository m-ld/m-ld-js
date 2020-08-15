/**
 * [[include:ably-remotes.md]]
 * @packageDocumentation
 * @internal
 */
import * as Ably from 'ably';
import { MeldConfig } from '..';
import { PubsubRemotes, SubPubsub, SubPub, DirectParams, ReplyParams } from '../PubsubRemotes';
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

export class AblyRemotes extends PubsubRemotes {
  private readonly client: Ably.Types.RealtimePromise;
  private readonly operations: Ably.Types.RealtimeChannelPromise;
  private readonly traffic: AblyTraffic;

  constructor(config: MeldAblyConfig,
    connect: (opts: Ably.Types.ClientOptions) => Ably.Types.RealtimePromise
      = opts => new Ably.Realtime.Promise(opts)) {
    super(config);
    this.client = connect({ ...config.ably, echoMessages: false, clientId: config['@id'] });
    this.operations = this.client.channels.get(this.channelName('operations'));
    this.traffic = new AblyTraffic(config.ably);
    // Ensure we are fully subscribed before we make any presence claims
    const subscribed = Promise.all([
      this.traffic.subscribe(
        this.operations, data => this.onRemoteUpdate(data)),
      this.operations.presence
        .subscribe(() => this.onPresenceChange())
        .then(() => this.onPresenceChange()), // Ably does not notify if no-one around
      // Direct channel that is specific to us, for sending and replying to
      // requests
      this.traffic.subscribe(
        this.client.channels.get(this.channelName(config['@id'])), this.onDirectMessage)
    ]).catch(err => this.close(err));

    // Note that we wait for subscription before claiming to be connected.
    // This is so we don't miss messages that are immediately sent to us.
    // https://support.ably.com/support/solutions/articles/3000067435
    this.client.connection.on('connected', () =>
      subscribed.then(() => this.onConnect()).catch(this.warnError));
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
    return `${this.meldJson.domain}:${id}`;
  }

  private onDirectMessage = async (data: any, name: string, clientId: string) => {
    try {
      // Message name is concatenated type:messageId:sentMessageId, where id is type-specific info
      const params = this.getParams(name, clientId);
      switch (params?.type) {
        case '__send':
          await this.onSent(data, params);
          break;
        case '__reply':
          await this.onReply(data, params);
      }
    } catch (err) {
      this.warnError(err);
    }
  };

  protected reconnect(): void {
    throw new Error('Method not implemented.'); // TODO
  }

  protected setPresent(present: boolean): Promise<unknown> {
    if (present)
      return this.operations.presence.update('__live');
    else
      return this.operations.presence.leave();
  }

  protected publishDelta(msg: object): Promise<unknown> {
    return this.traffic.publish(this.operations, '__delta', msg);
  }

  protected present(): Observable<string> {
    return from(this.operations.presence.get()).pipe(
      flatMap(identity), // flatten the array of presence messages
      filter(present => present.data === '__live'),
      map(present => present.clientId));
  }

  protected notifier(id: string): SubPubsub {
    const channel = this.client.channels.get(this.channelName(id));
    return {
      id, publish: notification => this.traffic.publish(channel, '__notify', notification),
      subscribe: () => this.traffic.subscribe(channel, data => this.onNotify(id, data)),
      unsubscribe: async () => channel.unsubscribe()
    };
  }

  protected sender(toId: string, messageId: string): SubPub {
    return this.getSubPub({ type: '__send', toId, messageId });
  }

  protected replier(toId: string, messageId: string, sentMessageId: string): SubPub {
    return this.getSubPub({ type: '__reply', toId, messageId, sentMessageId });
  }

  protected isEcho(): boolean {
    return false; // Echo is disabled in the config
  }

  private getParams(name: string, clientId: string): SendTypeParams | ReplyTypeParams | undefined {
    const [type, messageId, sentMessageId] = name.split(':');
    const params = { fromId: clientId, toId: this.id };
    switch (type) {
      case '__send': return { type, messageId, ...params }
      case '__reply': return { type, messageId, sentMessageId, ...params }
    }
  }

  private getSubPub(name: Omit<SendTypeParams, 'fromId'> | Omit<ReplyTypeParams, 'fromId'>): SubPub {
    const channelName = this.channelName(name.toId);
    const channel = this.client.channels.get(channelName);
    return {
      id: channelName,
      publish: msg => this.traffic.publish(channel, name.type === '__send' ?
        `__send:${name.messageId}` :
        `__reply:${name.messageId}:${name.sentMessageId}`, msg)
    };
  }
}