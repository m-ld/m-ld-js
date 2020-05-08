import { MqttRemotes } from '../src/mqtt/MqttRemotes';
import { MockProxy } from 'jest-mock-extended';
import { AsyncMqttClient } from 'async-mqtt';
import { MeldJournalEntry } from '../src/m-ld';
import { TreeClock } from '../src/clocks';
import { Subject as Source, of } from 'rxjs';
import { mockLocal, MockMqtt, mockMqtt } from './testClones';
import { filter, first, take, toArray } from 'rxjs/operators';
import { Future } from '../src/util';
import { comesOnline, isOnline } from '../src/AbstractMeld';

describe('New MQTT remotes', () => {
  let mqtt: MockMqtt & MockProxy<AsyncMqttClient>;
  let remotes: MqttRemotes;

  beforeEach(() => {
    mqtt = mockMqtt();
    remotes = new MqttRemotes('test.m-ld.org', 'client1', { hostname: 'unused' }, () => mqtt);
  });

  test('online starts unknown', async () => {
    await expect(isOnline(remotes)).resolves.toBe(null);
  });

  test('goes offline if no other clones', async () => {
    mqtt.mockConnect();
    await expect(remotes.online.pipe(take(2), toArray()).toPromise())
      .resolves.toEqual([null, false]);
  });

  test('goes online if clone already present', async () => {
    mqtt.mockPublish(
      '__presence/test.m-ld.org/client2',
      '{"consumer2":"test.m-ld.org/control"}');
    mqtt.mockConnect();
    await expect(remotes.online.pipe(take(2), toArray()).toPromise())
      .resolves.toEqual([null, true]);
  });

  test('sets presence with local clone on connect', async () => {
    remotes.setLocal(mockLocal());
    mqtt.mockConnect();
    // Presence is joined when the remotes' online status resolves
    await comesOnline(remotes, false);
    expect(mqtt.publish).lastCalledWith(
      '__presence/test.m-ld.org/client1',
      '{"client1":"test.m-ld.org/control"}',
      { qos: 1, retain: true });
  });

  describe('when genesis', () => {
    // No more setup
    beforeEach(() => mqtt.mockConnect());

    test('subscribes to topics', () => {
      expect(mqtt.subscribe).toBeCalledWith({
        '__presence/test.m-ld.org/+': 1
      });
      expect(mqtt.subscribe).toBeCalledWith({
        'test.m-ld.org/operations': 1,
        'test.m-ld.org/control': 1,
        'test.m-ld.org/registry': 1,
        '__send/client1/+/+/test.m-ld.org/control': 0,
        '__reply/client1/+/+/+': 0
      });
      // Presence ghost message
      expect(mqtt.publish).toBeCalledWith(
        '__presence/test.m-ld.org/client1', '-',
        { qos: 1 });
      // Setting retained last joined clone (no longer genesis)
      expect(mqtt.publish).toBeCalledWith(
        'test.m-ld.org/registry',
        '{"id":"client1"}',
        { qos: 1, retain: true });
    });

    test('can get new clock', async () => {
      expect((await remotes.newClock()).equals(TreeClock.GENESIS)).toBe(true);
    });

    test('emits remote operations', async () => {
      mqtt.mockPublish('test.m-ld.org/operations', {
        time: TreeClock.GENESIS.forked().left.toJson(),
        data: { tid: 't1', insert: '{}', delete: '{}' }
      });
      await expect(new Promise((resolve) => {
        remotes.updates.subscribe({ next: resolve });
      })).resolves.toHaveProperty('data');
    });

    test('goes online if clone appears', async () => {
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await expect(remotes.online.pipe(take(3), toArray()).toPromise())
        .resolves.toEqual([null, false, true]);
    });

    test('sets presence with local clone', async () => {
      remotes.setLocal(mockLocal());
      // Presence is joined when the remotes' online status resolves
      await comesOnline(remotes, false);
      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '{"client1":"test.m-ld.org/control"}',
        { qos: 1, retain: true });
    });

    test('publishes local operations if online', async () => {
      // Set someone else's presence so we're marked online
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await comesOnline(remotes);
      const delivered = new Future<boolean>();

      const entry = {
        time: TreeClock.GENESIS.forked().left,
        data: { tid: 't1', insert: '{}', delete: '{}' },
        delivered: () => delivered.resolve(true)
      };
      const updates = new Source<MeldJournalEntry>();
      remotes.setLocal(mockLocal(updates));
      // Setting retained presence on the channel
      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '{"client1":"test.m-ld.org/control"}',
        { qos: 1, retain: true });
      updates.next(entry);
      expect(mqtt.publish).toBeCalled();
      await mqtt.lastPublish();
      await expect(delivered).resolves.toBe(true);
    });

    test('online goes unknown if mqtt closes', async () => {
      mqtt.emit('close');
      await expect(isOnline(remotes)).resolves.toBe(null);
    });

    test('closes with local clone', async () => {
      const updates = new Source<MeldJournalEntry>();
      remotes.setLocal(mockLocal(updates));
      updates.complete();

      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '',
        { qos: 1, retain: true });
    });
  });

  describe('when not genesis', () => {
    beforeEach(() => {
      // Send retained Hello (remotes already constructed & listening)
      mqtt.mockPublish('test.m-ld.org/registry', { id: 'client2' });
      mqtt.mockConnect();
    });

    test('cannot get new clock if no peers', async () => {
      try {
        await remotes.newClock();
        fail();
      } catch (error) {
        expect(error.message).toMatch(/No-one present/);
      };
    });

    test('can get clock', async () => {
      const newClock = TreeClock.GENESIS.forked().right;
      // Set presence of client2's consumer
      await mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      mqtt.mockSubscribe((topic, json) => {
        const [type, toId, fromId, messageId, domain,] = topic.split('/');
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/clock') {
          expect(toId).toBe('consumer2');
          expect(fromId).toBe('client1');
          expect(domain).toBe('test.m-ld.org');
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, {
            '@type': 'http://control.m-ld.org/response/clock',
            clock: newClock.toJson()
          });
        }
      });
      expect((await remotes.newClock()).equals(newClock)).toBe(true);
    });
  });
});
