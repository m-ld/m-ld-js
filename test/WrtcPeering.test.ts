import { WrtcPeering } from '../src/wrtc';
import { Instance as Peer } from 'simple-peer';
import { mock, MockProxy } from 'jest-mock-extended';
import { EventEmitter } from 'events';
import { PeerSignal, PeerSignaller } from '../src/wrtc/WrtcPeering';

describe('WebRTC peering', () => {
  let peerEvents: EventEmitter;
  let peer: MockProxy<Peer> & Peer;
  let signaller: MockProxy<PeerSignaller> & PeerSignaller;
  let peering: WrtcPeering;

  beforeEach(() => {
    peerEvents = new EventEmitter;
    peer = mock<Peer>({
      on: peerEvents.on.bind(peerEvents),
      destroy: (err?: Error) => {
        // See https://github.com/feross/simple-peer/blob/master/index.js
        if (err != null)
          peerEvents.emit('error', err);
        peerEvents.emit('close');
      }
    });
    signaller = mock<PeerSignaller>();
    signaller.signal.mockResolvedValue(null);
  });

  describe('without peering available', () => {
    beforeEach(() => {
      peering = new WrtcPeering({
        '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, logLevel: 'debug'
      }, signaller, () => peer, false);
    });

    test('does not create peer notifier', async () => {
      await expect(peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      })).rejects.toThrowError();
      expect(signaller.signal).toBeCalledWith(
        'peerId', 'channelId', { unavailable: true });
    });

    test('signals unavailability if signalled', async () => {
      peering.signal('fromId', 'channelId', { type: 'offer' });
      expect(signaller.signal).toBeCalledWith(
        'fromId', 'channelId', { unavailable: true });
    });
  });

  describe('with peering available', () => {
    beforeEach(() => {
      peering = new WrtcPeering({
        '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, logLevel: 'debug'
      }, signaller, () => peer, true);
    });

    test('creates peer notifier', async () => {
      const notifierPromise = peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      });
      // Promise does not resolve until connected
      await expect(Promise.race([notifierPromise, 'pending']))
        .resolves.toBe('pending');
      // Signals are emitted before connect
      const signal: PeerSignal = { type: 'answer' };
      peerEvents.emit('signal', signal);
      expect(signaller.signal).lastCalledWith('peerId', 'channelId', signal);
      // Signals are received before connect
      peering.signal('peerId', 'channelId', signal);
      expect(peer.signal).lastCalledWith(signal);
      // Connect event resolves notifier
      peerEvents.emit('connect');
      const notifier = await notifierPromise;
      expect(notifier.id).toBe('channelId');
      // Can notify data
      const data = Buffer.from('hello');
      await notifier.publish(data);
      expect(peer.send).lastCalledWith(data);
    });

    test('peering fails and signals on error', async () => {
      const notifierPromise = peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      });
      peerEvents.emit('error', new Error('error!'));
      await expect(notifierPromise).rejects.toThrowError('error!');
      expect(signaller.signal).lastCalledWith(
        'peerId', 'channelId', { unavailable: true });
    });

    test('peering fails if peer unavailable', async () => {
      const notifierPromise = peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      });
      const signal: PeerSignal = { unavailable: true };
      peering.signal('peerId', 'channelId', signal);
      await expect(notifierPromise).rejects.toThrowError();
    });

    test('peer is created from signal', async () => {
      const signal: PeerSignal = { type: 'offer' };
      peering.signal('peerId', 'channelId', signal);
      expect(peer.signal).lastCalledWith(signal);
      // Connection can happen before getting the notifier
      peerEvents.emit('connect');
      await expect(peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      })).resolves.toBeDefined();
    });

    test('peer can be set unavailable by signal', async () => {
      const signal: PeerSignal = { unavailable: true };
      peering.signal('peerId', 'channelId', signal);
      // Telling them we're unavailable does not signal us back
      expect(signaller.signal).not.toBeCalled();
      await expect(peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      })).rejects.toThrowError();
    });

    test('peer can be closed by notifier', done => {
      const notifierPromise = peering.pubSub({
        toId: 'peerId', channelId: 'channelId', fromId: 'test'
      });
      peerEvents.emit('connect');
      notifierPromise.then(notifier => {
        peerEvents.on('close', done);
        notifier.close?.();
      });
    });
  });
});
