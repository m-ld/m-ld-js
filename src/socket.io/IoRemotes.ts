/**
 * [[include:socketio-remotes.md]]
 * @module IoRemotes
 * @internal
 */
import type { NotifyParams, PeerParams, ReplyParams, SendParams } from '../engine/remotes';
import { PubsubRemotes, SubPub } from '../engine/remotes';
import { Observable } from 'rxjs';
import { io, ManagerOptions, Socket, SocketOptions } from 'socket.io-client';
import { MeldExtensions } from '../api';
import { inflateFrom } from '../engine/util';
import { MeldConfig } from '../config';

export interface MeldIoConfig extends MeldConfig {
  io?: {
    uri: string;
    opts?: Partial<ManagerOptions & SocketOptions>;
  };
}

export class IoRemotes extends PubsubRemotes {
  private readonly socket: Socket;

  /** @type ConstructRemotes */
  constructor(
    config: MeldIoConfig,
    extensions: () => Promise<MeldExtensions>,
    connect = io
  ) {
    super(config, extensions);
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
      // includes a successful reconnection
      .on('connect', () => this.onConnect())
      // https://socket.io/docs/v4/client-socket-instance/#connect_error
      .on('connect_error', err => this.onConnectError(
        err, (<any>err)['type'] !== 'TransportError'))
      .on('reconnect_error', err => this.onConnectError(err, false)) // Already disconnected
      // reconnect_failed will only happen if reconnectionAttempts != Infinity (default)
      .on('reconnect_failed', () => this.onConnectError(
        'IO reconnect failed', true))
      // https://socket.io/docs/v4/client-socket-instance/#disconnect
      .on('disconnect', reason => reason !== 'io client disconnect' &&
        this.onConnectError(reason, reason === 'io server disconnect'))
      .on('presence', () => this.onPresenceChange())
      .on('operation', (payload: Uint8Array) => this.onOperation(payload))
      .on('send', (params: SendParams, msg: Uint8Array) => this.onSent(msg, params))
      .on('reply', (params: ReplyParams, msg: Uint8Array) => this.onReply(msg, params))
      .on('notify', (params: NotifyParams, msg: Uint8Array) =>
        this.onNotify(params.channelId, msg));
  }

  onConnectError(error: Error | string, isFatal: boolean) {
    this.log.warn(typeof error == 'string' ? error : error.message);
    if (isFatal)
      this.close(error).catch();
    else
      this.onDisconnect();
  }

  async close(err?: any): Promise<void> {
    await super.close(err);
    this.socket.close();
  }

  protected present(): Observable<string> {
    return inflateFrom(this.emitWithAck('presence'));
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
      // See ./server/index.ts#SubPubHandler
      publish: msg => this.emitWithAck(ev, params, msg)
    };
  }

  private emitWithAck<T>(ev: string, ...args: unknown[]): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.socket.timeout(this.sendTimeout).emit(ev, ...args,
        (timedOut: string | null, err: string | null, rtn: T) => {
          if (timedOut || err)
            reject(timedOut || err);
          else
            resolve(rtn);
        });
    });
  }
}