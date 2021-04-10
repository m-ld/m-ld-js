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
    peering = new WrtcPeering({
      '@id': 'test', '@domain': 'test.m-ld.org', genesis: true
    }, () => peer);
    signaller = mock<PeerSignaller>();
    signaller.signal.mockResolvedValue(null);
  });

  test('creates peer notifier', async () => {
    const notifierPromise = peering.peerSubPub({
      toId: 'peerId', channelId: 'channelId', fromId: 'test'
    }, signaller);
    // Promise does not resolve until connected
    await expect(Promise.race([notifierPromise, 'pending'])).resolves.toBe('pending');
    // Signals are emitted before connect
    const signal: PeerSignal = { type: 'answer' };
    peerEvents.emit('signal', signal);
    expect(signaller.signal).lastCalledWith('peerId', 'channelId', signal);
    // Signals are received before connect
    peering.onSignal('peerId', 'channelId', signal, signaller);
    expect(peer.signal).lastCalledWith(signal);
    // Connect event resolves notifier
    peerEvents.emit('connect');
    const notifier = await notifierPromise;
    expect(notifier.id).toBe('channelId');
    // Can notify data
    const data = Buffer.from('hello');
    notifier.publish(data);
    expect(peer.send).lastCalledWith(data);
  });

  test('peering fails on error', async () => {
    const notifierPromise = peering.peerSubPub({
      toId: 'peerId', channelId: 'channelId', fromId: 'test'
    }, signaller);
    peerEvents.emit('error', new Error('error!'));
    expect(notifierPromise).rejects.toThrowError('error!');
  });

  test('peering fails if peer unavailable', async () => {
    const notifierPromise = peering.peerSubPub({
      toId: 'peerId', channelId: 'channelId', fromId: 'test'
    }, signaller);
    const signal: PeerSignal = { unavailable: true };
    peering.onSignal('peerId', 'channelId', signal, signaller);
    expect(notifierPromise).rejects.toThrowError();
  });

  test('peer is created from signal', async () => {
    const signal: PeerSignal = { type: 'offer' };
    peering.onSignal('peerId', 'channelId', signal, signaller);
    expect(peer.signal).lastCalledWith(signal);
    // Connection can happen before getting the notifier
    peerEvents.emit('connect');
    expect(peering.peerSubPub({
      toId: 'peerId', channelId: 'channelId', fromId: 'test'
    }, signaller)).resolves.toBeDefined();
  });

  test('peer can be closed by notifier', async done => {
    const notifierPromise = peering.peerSubPub({
      toId: 'peerId', channelId: 'channelId', fromId: 'test'
    }, signaller);
    peerEvents.emit('connect');
    const notifier = await notifierPromise;
    peerEvents.on('close', done);
    notifier.close();
  });
});