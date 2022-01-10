import { MeldWrtcConfig, PeerSignal, PeerSignaller, WrtcPeering } from '../wrtc/WrtcPeering';
import { MeldExtensions } from '../api';
import { ablyConnect, AblyRemotes, MeldAblyConfig } from './AblyRemotes';
import { NotifyParams, SubPub } from '../engine/remotes/index';

export class AblyWrtcRemotes extends AblyRemotes implements PeerSignaller {
  private readonly peering: WrtcPeering;

  constructor(
    config: MeldAblyConfig & MeldWrtcConfig,
    extensions: MeldExtensions,
    connect = ablyConnect
  ) {
    super(config, extensions, connect);
    this.peering = new WrtcPeering(config, this);
  }

  /** override to make public for PeerSignaller implementation */
  notify(channelId: string, payload: Buffer) {
    super.onNotify(channelId, payload);
  }

  /** override to make public for PeerSignaller implementation */
  signal(peerId: string, channelId: string, data: PeerSignal) {
    return super.signal(peerId, channelId, data);
  }

  protected peerSubPub(params: NotifyParams): Promise<SubPub | undefined> {
    return this.peering.pubSub(params).catch(err => {
      this.log.info(`Cannot use peer-to-peer notifier due to ${err}`);
      // Fall through to use a direct pubsub
      return undefined;
    });
  }

  protected onSignal(channelId: string, fromId: string, data: PeerSignal) {
    this.peering.signal(fromId, channelId, data);
  }
}