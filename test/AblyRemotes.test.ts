import * as Ably from 'ably';
import { mockDeep as mock, MockProxy } from 'jest-mock-extended';
import { AblyRemotes, MeldAblyConfig } from '../src/ably';
import { comesOnline } from '../src/AbstractMeld';
import { DeltaMessage } from '../src/m-ld';
import { mockLocal } from './testClones';
import { NEVER, Subject as Source } from 'rxjs';
import { Future } from '../src/util';
import { TreeClock } from '../src/clocks';
import { Request, Response } from '../src/m-ld/ControlMessage';

/**
 * These tests use a fully mocked Ably to avoid incurring costs. The behaviour
 * of a PubsubRemotes is tested primarily by MqttRemotes.test.ts.
 */
describe('Ably remotes', () => {
  let connect: jest.Mock<MockProxy<Ably.Types.RealtimePromise>>;
  let client: MockProxy<Ably.Types.RealtimePromise>;
  let operations: MockProxy<Ably.Types.RealtimeChannelPromise>;
  let control: MockProxy<Ably.Types.RealtimeChannelPromise>;
  let connCallbacks: { [key: string]: Ably.Types.connectionEventCallback | undefined } = {};
  const config: MeldAblyConfig = {
    '@id': 'test', '@domain': 'test.m-ld.org', genesis: true, ably: { token: 'token' }
  };
  function otherPresent() {
    const [subscriber] = operations.presence.subscribe.mock.calls[0];
    if (typeof subscriber != 'function')
      fail();
    setImmediate(() => {
      // The implementation relies on the presence set rather than just the
      // subscriber parameter.
      const present = mock<Ably.Types.PresenceMessage>({ clientId: 'other', data: '__online' });
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
    operations.presence.subscribe.mockReturnValue(Promise.resolve());
    operations.presence.get.mockReturnValue(Promise.resolve([]));

    control = mock<Ably.Types.RealtimeChannelPromise>();
    client.channels.get.calledWith('test.m-ld.org:test').mockReturnValue(control);
    control.subscribe.mockReturnValue(Promise.resolve());

    // Capture the connection event handlers
    client.connection.on.mockImplementation((events, cb) => {
      if (typeof events == 'string')
        connCallbacks[events] = cb;
      else if (Array.isArray(events))
        events.forEach(event => connCallbacks[event] = cb);
    });
  });

  test('connects with given config', async () => {
    new AblyRemotes(config, connect);
    expect(connect).toHaveBeenCalledWith({
      ...config.ably, clientId: 'test', echoMessages: false
    });
    expect(operations.subscribe).toHaveBeenCalled();
    expect(operations.presence.subscribe).toHaveBeenCalled();
    expect(control.subscribe).toHaveBeenCalled();
  });

  test('goes offline with no-one present', async () => {
    const remotes = new AblyRemotes(config, connect);
    // We have not supplied a presence update, per Ably behaviour
    await expect(comesOnline(remotes, false)).resolves.toBe(false);
  });

  test('responds to presence', async () => {
    const remotes = new AblyRemotes(config, connect);
    otherPresent();
    await expect(comesOnline(remotes)).resolves.toBe(true);
  });

  test('joins presence if clone is online', async () => {
    const remotes = new AblyRemotes(config, connect);
    remotes.setLocal(mockLocal(NEVER, [true]));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    const joined = new Future<any | undefined>();
    operations.presence.update.mockImplementation(async data => joined.resolve(data));
    await expect(joined).resolves.toBe('__online');
  });

  test('joins presence if clone comes online', async () => {
    const remotes = new AblyRemotes(config, connect);
    remotes.setLocal(mockLocal(NEVER, [false, true]));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    const joined = new Future<any | undefined>();
    operations.presence.update.mockImplementation(async data => joined.resolve(data));
    await expect(joined).resolves.toBe('__online');
  });

  test('leaves presence if clone goes offline', async () => {
    const remotes = new AblyRemotes(config, connect);
    remotes.setLocal(mockLocal(NEVER, [true, false]));
    connCallbacks.connected?.(mock<Ably.Types.ConnectionStateChange>());
    const left = new Future;
    operations.presence.leave.mockImplementation(async () => left.resolve());
    await expect(left).resolves.toBe(undefined);
  });

  test('publishes a delta', async () => {
    const remotes = new AblyRemotes(config, connect);
    otherPresent();
    await comesOnline(remotes);
    const entry = new DeltaMessage(
      TreeClock.GENESIS.forked().left,
      { tid: 't1', insert: '{}', delete: '{}' });
    const updates = new Source<DeltaMessage>();
    remotes.setLocal(mockLocal(updates));
    updates.next(entry);
    expect(operations.publish).toHaveBeenCalledWith('__delta', entry.toJson());
  });

  test('sends a new clock request', async () => {
    const newClock = TreeClock.GENESIS.forked().left;
    // Grab the control channel subscriber
    const remotes = new AblyRemotes(config, connect);
    const [subscriber] = control.subscribe.mock.calls[0];
    if (typeof subscriber != 'function')
      return fail();
    // Set up the other clone's direct channel
    const other = mock<Ably.Types.RealtimeChannelPromise>();
    client.channels.get.calledWith('test.m-ld.org:other').mockReturnValue(other);
    other.subscribe.mockReturnValue(Promise.resolve());
    other.publish.mockImplementation((name, data) => {
      const splitName = name.split(':');
      expect(splitName[0]).toBe('__send');
      expect(data).toEqual(new Request.NewClock().toJson());
      setImmediate(() => subscriber(mock<Ably.Types.Message>({
        clientId: 'other',
        data: new Response.NewClock(newClock).toJson(),
        name: `__reply:reply1:${splitName[1]}`
      })));
      return Promise.resolve();
    });
    otherPresent();
    await comesOnline(remotes);
    expect((await remotes.newClock()).equals(newClock)).toBe(true);
  });

  // TODO: notification channels for snapshots and revups
});