import { EventEmitter } from 'events';
import type { Namespace, RemoteSocket, Socket } from 'socket.io';
// noinspection ES6PreferShortImport because we just want the types
import type { PeerParams } from '../../engine/remotes/PubsubParams';

type Callback = (err: string | null, ...args: any[]) => void;
type SubPubHandler = (params: PeerParams, payload: Buffer, cb: Callback) => void;
type PresenceCallback = (err: string | null, present: string[]) => void;

export class IoRemotesService extends EventEmitter {
  constructor(
    private readonly ns: Namespace) {
    super();
    ns.on('connection', socket => {
      const id = queryValue(socket, '@id');
      const domain = queryValue(socket, '@domain');
      this.emit('debug', id, 'connected to', domain);
      // A clone's socket joins the domain and its own private room so we can find it later
      socket.join([domain, `${domain}/${id}`]);
      socket
        .on('presence', this.presenceHandler(domain))
        .on('present', this.presentHandler(socket, domain))
        .on('operation', this.operationHandler(socket, domain))
        .on('send', this.subPubHandler('send', domain))
        .on('reply', this.subPubHandler('reply', domain))
        .on('notify', this.subPubHandler('notify', domain));
      // Pub-sub remotes requires an immediate notification of presence
      socket.emit('presence');
    });
    const roomChanged = (room: string) => {
      const [domain, path] = room.split('/', 2);
      if (path === 'present')
        ns.in(domain).emit('presence');
    };
    ns.adapter.on('join-room', roomChanged);
    ns.adapter.on('leave-room', roomChanged);
  }

  private presentHandler(socket: Socket, domain: string) {
    return (present: boolean) => socket[present ? 'join' : 'leave'](`${domain}/present`);
  }

  private operationHandler(socket: Socket, domain: string) {
    return (payload: Buffer) => socket.broadcast.in(domain).emit('operation', payload);
  }

  private presenceHandler(domain: string) {
    return (cb: PresenceCallback) => {
      // Presence is managed as a room called 'present' (see 'present' handler)
      this.ns.in(`${domain}/present`).fetchSockets()
        .then(sockets => cb(null, sockets.map(socket => queryValue(socket, '@id'))))
        .catch(err => this.error(err, cb));
    };
  }

  private subPubHandler(ev: string, domain: string): SubPubHandler {
    return (params, payload, cb) => {
      this.emit('debug', ev, params);
      this.ns.in(`${domain}/${params.toId}`).emit(ev, params, payload);
      cb(null);
    };
  }

  private error = (err: any, cb: Callback) => {
    this.emit('error', err);
    cb(`${err}`);
  };
}

function queryValue(socket: RemoteSocket<any> | Socket, param: string): string {
  const element = socket.handshake.query[param];
  if (element == null)
    throw new Error(`Missing ${param} query parameter`);
  else
    return Array.isArray(element) ? element[0] : element;
}
