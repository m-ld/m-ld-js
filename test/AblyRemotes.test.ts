import * as Ably from 'ably';
import { mockDeep as mock, MockProxy } from 'jest-mock-extended';
import { AblyRemotes, MeldAblyConfig } from '../src/ably';
import { comesAlive } from '../src/engine/AbstractMeld';
import { mockLocal, testOp } from './testClones';
import { BehaviorSubject, Subject as Source } from 'rxjs';
import { isArray } from '../src/engine/util';
import { TreeClock } from '../src/engine/clocks';
import { NewClockRequest, NewClockResponse } from '../src/engine/remotes/ControlMessage';
import { DeepMockProxy } from 'jest-mock-extended/lib/Mock';
import { MeldOperationMessage } from '../src/engine/MeldOperationMessage';
import { Future } from '../src/engine/Future';

/**
 * These tests use a fully mocked Ably to avoid incurring costs. The behaviour
 * of a PubsubRemotes is tested primarily by MqttRemotes.test.ts.
 */
describe('Ably remotes', () => {
  let connect: jest.Mock<MockProxy<Ably.Types.RealtimePromise>>;
  let client: DeepMockProxy<Ably.Types.RealtimePromise>;
  let operations: DeepMockProxy<Ably.Types.RealtimeChannelPromise>;
  let control: MockProxy<Ably.Types.RealtimeChannelPromise>;
  let connCallbacks: { [key: string]: Ably.Types.connectionEventCallback | undefined } = {};
  const config: MeldAblyConfig = {
    '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, ably: { token: 'token' }
  };
  const extensions = () => Promise.resolve({});
  function otherPresent() {
    const [subscriber] = operations.presence.subscribe.mock.calls[0];
    if (typeof subscriber != 'function')
      throw 'expecting subscriber function';
    setImmediate(() => {
      // The implementation relies on the presence set rather than just the
      // subscriber parameter.
      const present = mock<Ably.Types.PresenceMessage>({ clientId: 'other', data: '__live' });
      operations.presence.get.mockReturnValue(Promise.resolve([present]));
      subscriber(present);
    });
  }

  beforeEach(() => {
    client = mock<Ably.Types.RealtimePromise>();
    connect = jest.fn(() => client);

    operations = mock<Ably.Types.RealtimeChannelPromise>();
    client.channels.get.calledWith('test.m-ld.org:operations').mockReturnValue(operations);
    operations.subscribe.mockReturnValue(Promise.resolve());
    operations.publish.mockReturnValue(Promise.resolve());
    operations.presence.subscribe.mockReturnValue(Promise.resolve());
    operations.presence.get.mockReturnValue(Promise.resolve([]));

    control = mock<Ably.Types.RealtimeChannelPromise>();
    client.channels.get.calledWith('test.m-ld.org:test').mockReturnValue(control);
    control.subscribe.mockReturnValue(Promise.resolve());

    // Capture the connection event handlers
    client.connection.on.mockImplementation((events, cb) => {
      if (typeof events == 'string')
        connCallbacks[events] = cb;
      else if (isArray(events))
        events.forEach(event => connCallbacks[event] = cb);
    });
  });

  test('connects with given config', async () => {
    new AblyRemotes(config, extensions, connect);
    expect(connect).toHaveBeenCalledWith({
      ...config.ably, clientId: 'test', echoMessages: false
    });
    expect(operations.subscribe).toHaveBeenCalled();
    expect(operations.presence.subscribe).toHaveBeenCalled();
    expect(control.subscribe).toHaveBeenCalled();
  });

  test('goes offline with no-one present', async () => {
    const remotes = new AblyRemotes(config, extensions, connect);
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    // We have not supplied a presence update, per Ably behaviour
    await expect(comesAlive(remotes, false)).resolves.toBe(false);
  });

  test('responds to presence', async () => {
    const remotes = new AblyRemotes(config, extensions, connect);
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    otherPresent();
    await expect(comesAlive(remotes)).resolves.toBe(true);
  });

  test('joins presence if clone is live', async () => {
    const remotes = new AblyRemotes(config, extensions, connect);
    remotes.setLocal(mockLocal({}, [true]));
    const joined = new Future<any | undefined>();
    operations.presence.update.mockImplementation(async data => joined.resolve(data));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    await expect(joined).resolves.toBe('__live');
  });

  test('does not join presence until subscribed', async () => {
    control.subscribe.mockReturnValue(new Promise(() => { }));
    const remotes = new AblyRemotes(config, extensions, connect);
    remotes.setLocal(mockLocal({}, [true]));
    const joined = new Future<any | undefined>();
    operations.presence.update.mockImplementation(async data => joined.resolve(data));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    // Push to immediate because connected handling is async
    const now = new Promise(res => setImmediate(() => res('now')));
    await expect(Promise.race([now, joined])).resolves.toBe('now');
  });

  test('does not go live until subscribed', async () => {
    control.subscribe.mockReturnValue(new Promise(() => { }));
    const remotes = new AblyRemotes(config, extensions, connect);
    remotes.setLocal(mockLocal({}, [true]));
    const goneLive = comesAlive(remotes, false); // No presence so false
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    // Push to immediate because connected handling is async
    const now = new Promise(res => setImmediate(() => res('now')));
    await expect(Promise.race([now, goneLive])).resolves.toBe('now');
  });

  test('joins presence if clone comes live', async () => {
    const remotes = new AblyRemotes(config, extensions, connect);
    remotes.setLocal(mockLocal({}, [false, true]));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    const joined = new Future<any | undefined>();
    operations.presence.update.mockImplementation(async data => joined.resolve(data));
    await expect(joined).resolves.toBe('__live');
  });

  test('leaves presence if clone goes offline', async () => {
    const remotes = new AblyRemotes(config, extensions, connect);
    const live = new BehaviorSubject(true);
    remotes.setLocal(mockLocal({ live }));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    const left = new Future;
    operations.presence.leave.mockImplementation(async () => left.resolve());
    // Push to immediate because connected handling is async
    setImmediate(() => live.next(false));
    await expect(left).resolves.toBe(undefined);
  });

  test('publishes an operation', async () => {
    const remotes = new AblyRemotes(config, extensions, connect);
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    otherPresent();
    await comesAlive(remotes);
    const prevTime = TreeClock.GENESIS.forked().left, time = prevTime.ticked();
    const entry = MeldOperationMessage.fromOperation(prevTime.ticks, testOp(time, {}, {}), null);
    const updates = new Source<MeldOperationMessage>();
    remotes.setLocal(mockLocal({ operations: updates }));
    updates.next(entry);
    expect(operations.publish).toHaveBeenCalledWith(
      '__op', MeldOperationMessage.toBuffer(entry));
  });

  test('sends a new clock request', async () => {
    const newClock = TreeClock.GENESIS.forked().left;
    // Grab the control channel subscriber
    const remotes = new AblyRemotes(config, extensions, connect);
    remotes.setLocal(mockLocal());
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    const [subscriber] = control.subscribe.mock.calls[0];
    if (typeof subscriber != 'function')
      throw 'expecting subscriber function';
    // Set up the other clone's direct channel
    const other = mock<Ably.Types.RealtimeChannelPromise>();
    client.channels.get.calledWith('test.m-ld.org:other').mockReturnValue(other);
    other.subscribe.mockReturnValue(Promise.resolve());
    other.publish.mockImplementation((name, data) => {
      const splitName = name.split(':');
      expect(splitName[0]).toBe('__send');
      expect(data.equals(new NewClockRequest().toBuffer())).toBe(true);
      // Object assign overcomes mocking of the buffer which borks Buffer.equals
      setImmediate(() => subscriber(Object.assign(mock<Ably.Types.Message>(), {
        clientId: 'other',
        // Check that the remotes can cope with non-Buffers
        data: new Uint8Array(new NewClockResponse(newClock).toBuffer()),
        name: `__reply:reply1:${splitName[1]}`
      })));
      return Promise.resolve();
    });
    otherPresent();
    await comesAlive(remotes);
    expect((await remotes.newClock()).equals(newClock)).toBe(true);
  });

  // TODO: notification channels for snapshots and revups
});