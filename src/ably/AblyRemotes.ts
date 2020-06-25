import * as Ably from 'ably';
import { MeldConfig } from '..';
import { PubsubRemotes, SubPubsub, SubPub, DirectParams, ReplyParams } from '../PubsubRemotes';
import { Observable, from, identity } from 'rxjs';
import { flatMap, filter, map } from 'rxjs/operators';

export interface MeldAblyConfig extends MeldConfig {
  ably: Omit<Ably.Types.ClientOptions, 'echoMessages' | 'clientId'>;
}

interface SendTypeParams extends DirectParams { type: '__send'; }
interface ReplyTypeParams extends ReplyParams { type: '__reply'; }

export class AblyRemotes extends PubsubRemotes {
  private readonly client: Ably.Types.RealtimePromise;
  private readonly channel: Ably.Types.RealtimeChannelPromise;

  constructor(config: MeldAblyConfig) {
    super(config);
    this.client = new Ably.Realtime.Promise({
      ...config.ably, echoMessages: false, clientId: config['@id']
    });
    this.channel = this.client.channels.get(this.channelName('operations'));
    this.channel.subscribe(message => this.onRemoteUpdate(message.data));
    this.channel.presence
      .subscribe(() => this.onPresenceChange())
      .then(() => this.onPresenceChange()); // Ably does not notify if no-one around
    // Direct channel that is specific to us, for sending and replying to requests
    this.client.channels.get(this.channelName(config['@id']))
      .subscribe(message => this.onDirectMessage(message).catch(this.warnError));
    // Ably has connection recovery with no message loss for 2min. During that
    // time we treat the remotes as online. After that, the connection becomes
    // suspended and we are offline.
    this.client.connection.on('connected', () => this.onConnect().catch(this.warnError));
    this.client.connection.on(['suspended', 'failed', 'closing'], () => this.onDisconnect());
  }

  async close(err?: any) {
    await super.close(err);
    this.client.connection.close();
  }

  private channelName(id: string) {
    // https://www.ably.io/documentation/realtime/channels#channel-namespaces
    return `${this.domain}:${id}`;
  }

  private async onDirectMessage(message: Ably.Types.Message): Promise<void> {
    // Message name is concatenated type:messageId:sentMessageId, where id is type-specific info
    const params = this.getParams(message);
    switch (params?.type) {
      case '__send': return this.onSent(message.data, params);
      case '__reply': return this.onReply(message.data, params);
    }
  }

  protected reconnect(): void {
    throw new Error('Method not implemented.'); // TODO
  }

  protected setPresent(present: boolean): Promise<unknown> {
    if (present)
      return this.channel.presence.update('__online');
    else
      return this.channel.presence.leave();
  }

  protected publishDelta(msg: object): Promise<unknown> {
    return this.channel.publish('__delta', msg);
  }

  protected present(): Observable<string> {
    return from(this.channel.presence.get()).pipe(
      flatMap(identity), // flatten the array of presence messages
      filter(present => present.data === '__online'),
      map(present => present.clientId));
  }

  protected notifier(id: string): SubPubsub {
    const channel = this.client.channels.get(this.channelName(id));
    return {
      id, publish: notification => channel.publish('__notify', notification),
      subscribe: () => channel.subscribe(message => this.onNotify(id, message.data)),
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

  private getParams(message: Ably.Types.Message): SendTypeParams | ReplyTypeParams | undefined {
    const [type, messageId, sentMessageId] = message.name.split(':');
    const params = { fromId: message.clientId, toId: this.id };
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
      publish: msg => channel.publish(name.type === '__send' ?
        `__send:${name.messageId}` :
        `__reply:${name.messageId}:${name.sentMessageId}`, msg)
    };
  }
}