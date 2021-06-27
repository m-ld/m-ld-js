/**
 * [[include:socketio-remotes.md]]
 * @module IoRemotes
 * @internal
 */
import type { NotifyParams, PeerParams, ReplyParams, SendParams } from '../engine/remotes';
import { PubsubRemotes, SubPub } from '../engine/remotes';
import { from, Observable } from 'rxjs';
import { io, Socket, SocketOptions } from 'socket.io-client';
import type { MeldConfig } from '../index';
import type { ManagerOptions } from 'socket.io-client/build/manager';

export interface MeldIoConfig extends MeldConfig {
  io?: {
    uri: string;
    opts?: Partial<ManagerOptions & SocketOptions>;
  };
}

export class IoRemotes extends PubsubRemotes {
  private readonly socket: Socket;

  constructor(config: MeldIoConfig, connect = io) {
    super(config);
    const opts = config.io?.opts;
    const optsToUse: Partial<ManagerOptions> = {
      ...opts, query: {
        ...opts?.query,
        '@id': config['@id'],
        '@domain': config['@domain']
      }
    };
    this.socket = config.io != null ?
      connect(config.io.uri, optsToUse) : connect(optsToUse);
    this.socket
      .on('connect', () => this.onConnect())
      .on('disconnect', () => this.onDisconnect())
      .on('presence', () => this.onPresenceChange())
      .on('operation', (payload: Buffer) => this.onRemoteUpdate(payload))
      .on('send', (params: SendParams, msg: Buffer) => this.onSent(msg, params))
      .on('reply', (params: ReplyParams, msg: Buffer) => this.onReply(msg, params))
      .on('notify', (params: NotifyParams, msg: Buffer) => this.onNotify(params.channelId, msg));
  }

  protected present(): Observable<string> {
    return new Observable(subs => {
      this.socket.emit('presence', (err: string | null, present: string[]) => {
        if (err)
          subs.error(err);
        else if (!subs.closed)
          from(present).subscribe(subs);
      });
    });
  }

  protected async setPresent(present: boolean): Promise<void> {
    this.socket.emit('present', present);
  }

  protected async publishOperation(msg: Buffer): Promise<void> {
    this.socket.emit('operation', msg);
  }

  protected sender(params: SendParams): SubPub {
    return this.subPub(params.toId, params, 'send');
  }

  protected replier(params: ReplyParams): SubPub {
    return this.subPub(params.toId, params, 'reply');
  }

  protected notifier(params: NotifyParams): SubPub {
    // All the notification params are sent with every message, to save the
    // server having to maintain state for the channel
    return this.subPub(params.channelId, params, 'notify');
  }

  private subPub(id: string, params: PeerParams, ev: string): SubPub {
    return {
      id,
      publish: msg => new Promise<void>((resolve, reject) => {
        // See ./server/index.ts#SubPubHandler
        this.socket.emit(ev, params, msg, (err: string | null) => err ? reject(err) : resolve());
      })
    };
  }
}