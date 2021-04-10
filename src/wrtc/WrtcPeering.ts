import { Logger } from 'loglevel';
import * as SimplePeer from 'simple-peer';
import { Instance as Peer, Options as PeerOpts, SignalData } from 'simple-peer';
import { MeldConfig } from '..';
import { SubPubsub, NotifyParams } from '../engine/PubsubRemotes';
import { Future, getIdLogger } from '../engine/util';

export type CreatePeer = (opts?: PeerOpts) => Peer;

export interface MeldWrtcConfig extends MeldConfig {
  wrtc?: RTCConfiguration;
}

export interface PeerSignal extends SignalData {
  unavailable?: true;
}

export interface PeerSignaller {
  onNotify: (subPubId: string, payload: Buffer) => void;
  signal: (peerId: string, channelId: string, data: PeerSignal) => Promise<unknown>;
}

export class WrtcPeering {
  private readonly id: string;
  private readonly config: RTCConfiguration;
  private readonly timeout: (callback: () => void) => ReturnType<typeof setTimeout>;
  private readonly peers: {
    [subPubId: string]: { connected: PromiseLike<unknown>, peer: Peer }
  } = {};
  protected readonly log: Logger;

  constructor(config: MeldWrtcConfig,
    private readonly createPeer = (opts: PeerOpts) => new SimplePeer(opts)) {
    this.id = config['@id'];
    this.config = config.wrtc ?? {};
    this.timeout = callback => setTimeout(callback, config.networkTimeout ?? 5000);
    this.log = getIdLogger(this.constructor, this.id, config.logLevel ?? 'info');
  }

  async peerSubPub({ toId, fromId, channelId }: NotifyParams,
    signaller: PeerSignaller): Promise<SubPubsub | undefined> {
    // Determine the notification direction, from us or to us?
    const outbound = fromId === this.id;
    const { connected, peer } = this.peer(signaller, outbound ? toId : fromId, channelId, outbound);
    // Wait for connection, in case the collaborator doesn't support peering
    await connected;
    return {
      id: channelId,
      publish: async msg => peer.send(msg),
      subscribe: async () => null, // Never need to subscribe, peer data is always enabled
      close: async () => peer.destroy()
    }
  }

  onSignal(fromId: string, channelId: string, data: PeerSignal, signaller: PeerSignaller) {
    // A signal can arrive before the peerSubPub has been set up
    const { peer } = this.peer(signaller, fromId, channelId);
    if (data.unavailable)
      peer.destroy(new Error(`${fromId} peering unavailable`));
    else
      peer.signal(data);
  }

  private peer(signaller: PeerSignaller, peerId: string, channelId: string, initiator = false) {
    if (channelId in this.peers) {
      return this.peers[channelId];
    } else {
      const logName = `Peer ${peerId} on channel ${channelId}`;
      const connected = new Future;
      const timer = this.timeout(() =>
        peer.destroy(new Error('connection timeout exceeded.')));
      Promise.resolve(connected)
        .then(() => this.log.debug(logName, 'connected.'))
        .catch(err => this.log.debug(logName, 'errored with', err))
        .finally(() => clearTimeout(timer));
      const peers = this.peers;
      const peer = this.createPeer({ config: this.config, initiator });
      peer.on('signal', (data: SignalData) =>
        signaller.signal(peerId, channelId, data).catch(connected.reject));
      peer.on('connect', connected.resolve);
      // If an error is emitted, destruction is underway
      peer.on('error', connected.reject);
      peer.on('data', (data: Buffer) => signaller.onNotify(channelId, data));
      peer.on('close', () => {
        this.log.debug(logName, 'closed.');
        delete peers[channelId];
      });
      return peers[channelId] = { connected, peer };
    }
  }
}