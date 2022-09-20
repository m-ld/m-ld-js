import { Logger } from 'loglevel';
import type { Instance as Peer, Options as PeerOpts, SignalData } from 'simple-peer';
import * as SimplePeer from 'simple-peer';
import { timer } from 'rxjs';
import type { NotifyParams, SubPub } from '../engine/remotes';
import { MeldConfig } from '../config';
import { getIdLogger } from '../engine/logging';

export interface MeldWrtcConfig extends MeldConfig {
  wrtc?: RTCConfiguration;
}

type Unavailable = {
  unavailable?: true;
};

export type PeerSignal = SignalData | Unavailable;

export interface PeerSignaller {
  notify: (channelId: string, payload: Buffer) => void;
  signal: (peerId: string, channelId: string, data: PeerSignal) => Promise<unknown>;
}

class UnavailableError extends Error {
  constructor(peerId: string) {
    super(`${peerId} peering unavailable`);
  }
}

export class WrtcPeering {
  private readonly id: string;
  private readonly config: RTCConfiguration;
  private readonly networkTimeout: number;
  private readonly peers: {
    [channelId: string]: { connected?: Promise<unknown>, peer?: Peer }
  } = {};
  protected readonly log: Logger;

  constructor(
    config: MeldWrtcConfig,
    private client: PeerSignaller,
    private readonly createPeer = (opts: PeerOpts) => new SimplePeer(opts),
    readonly available = SimplePeer.WEBRTC_SUPPORT
  ) {
    this.id = config['@id'];
    this.config = config.wrtc ?? {};
    this.networkTimeout = config.networkTimeout ?? 5000;
    this.log = getIdLogger(this.constructor, this.id, config.logLevel ?? 'info');
  }

  async pubSub({ toId, fromId, channelId }: NotifyParams): Promise<SubPub> {
    // Determine the notification direction, from us or to us?
    const outbound = fromId === this.id;
    const peerId = outbound ? toId : fromId;
    if (!this.available)
      this.dispose(peerId, channelId, true, 'peering unavailable.');
    const { connected, peer } =
      this.peer(peerId, channelId, outbound);
    if (peer == null)
      throw new UnavailableError(peerId);
    await connected;
    return {
      id: channelId,
      // Peer should always exist if connected resolved
      publish: async msg => peer.send(msg),
      // Wait a bit before destroying so the socket flushes
      // https://github.com/feross/simple-peer/blob/9ea1805d992a8164a42b750160ed3425f2a494f1/index.js#L580
      close: () => timer(1000).subscribe(() => peer.destroy())
    };
  }

  signal(fromId: string, channelId: string, data: PeerSignal) {
    // Note that a signal can arrive before the peer has been set up.
    if (!(channelId in this.peers) && (!this.available || isUnavailable(data))) {
      // Peering not possible. Remember this channel as unavailable.
      this.dispose(fromId, channelId, !isUnavailable(data), 'marked unavailable');
    } else {
      // Get an existing peer or create a new one
      const { peer } = this.peer(fromId, channelId);
      if (peer == null)
        this.log.debug(`Disposed channel ${channelId} peer ${fromId} received signal`);
      else if (isUnavailable(data))
        peer.destroy(new UnavailableError(fromId));
      else
        peer.signal(data);
    }
  }

  private dispose(peerId: string, channelId: string, signalUnavailable: boolean, ...msg: any[]) {
    this.log.debug(`Channel ${channelId} peer ${peerId} disposed:`, ...msg);
    // If peering has failed, the signalling channel may still be open
    if (signalUnavailable)
      this.client.signal(peerId, channelId, { unavailable: true })
        .catch(err => this.log.warn(err));
    this.peers[channelId] = {};
    this.timeout(() => this.forget(channelId));
  }

  private peer(peerId: string, channelId: string, initiator = false) {
    if (channelId in this.peers) {
      return this.peers[channelId];
    } else {
      const timer = this.timeout(() =>
        peer.destroy(new Error('connection timeout exceeded.')));
      const peer = this.createPeer({
        channelConfig: { ordered: true },
        config: this.config,
        initiator
      });
      this.log.debug(`Channel ${channelId} peer ${peerId}`, 'created.');
      const connected = new Promise((resolve, reject) => {
        peer.on('connect', resolve);
        // If an error is emitted, destruction is underway
        peer.on('error', reject);
      }).then(() => {
        this.log.debug(`Channel ${channelId} peer ${peerId}`, 'connected.');
      }).catch(err => {
        // Don't signal the error to the peer if they are unavailable
        this.dispose(peerId, channelId, !(err instanceof UnavailableError), 'errored', err);
        throw err;
      }).finally(() => timer.unsubscribe());
      peer.on('signal', (data: SignalData) =>
        this.client.signal(peerId, channelId, data)
          .catch(err => peer.destroy(err)));
      peer.on('data', (data: Buffer) =>
        this.client.notify(channelId, data));
      peer.on('close', () =>
        this.dispose(peerId, channelId, false, 'closed.'));
      return this.peers[channelId] = { connected, peer };
    }
  }

  private forget(channelId: string) {
    delete this.peers[channelId];
  }

  private timeout(cb: () => void) {
    return timer(this.networkTimeout).subscribe(cb);
  }
}

function isUnavailable(signal: PeerSignal): signal is Unavailable {
  return 'unavailable' in signal;
}