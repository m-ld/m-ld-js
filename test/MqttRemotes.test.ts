import { MqttRemotes } from '../src/mqtt/MqttRemotes';
import { MockProxy } from 'jest-mock-extended';
import { AsyncMqttClient } from 'async-mqtt';
import { DeltaMessage } from '../src/engine';
import { TreeClock } from '../src/engine/clocks';
import { Subject as Source, of } from 'rxjs';
import { mockLocal, MockMqtt, mockMqtt } from './testClones';
import { take, toArray } from 'rxjs/operators';
import { comesAlive } from '../src/engine/AbstractMeld';
import { MeldErrorStatus } from '../src/engine/MeldError';
import { Request, Response } from '../src/engine/ControlMessage';
import { Future } from '../src/engine/util';
import { JsonNotification } from '../src/engine/PubsubRemotes';
import { MsgPack } from '../src/engine/util';

/**
 * These tests also test the abstract base class, PubsubRemotes
 */
describe('New MQTT remotes', () => {
  let mqtt: MockMqtt & MockProxy<AsyncMqttClient>;
  let remotes: MqttRemotes;

  beforeEach(() => {
    mqtt = mockMqtt();
    remotes = new MqttRemotes({
      '@id': 'client1',
      '@domain': 'test.m-ld.org',
      genesis: true, // Actually not used by MqttRemotes
      mqtt: { hostname: 'unused' }
    }, () => mqtt);
  });

  test('live starts unknown', async () => {
    expect(remotes.live.value).toBe(null);
  });

  test('goes offline if no other clones', async () => {
    mqtt.mockConnect();
    await expect(remotes.live.pipe(take(2), toArray()).toPromise())
      .resolves.toEqual([null, false]);
  });

  test('goes live if clone already present', async () => {
    mqtt.mockPublish(
      '__presence/test.m-ld.org/client2',
      '{"consumer2":"test.m-ld.org/control"}');
    mqtt.mockConnect();
    await expect(remotes.live.pipe(take(2), toArray()).toPromise())
      .resolves.toEqual([null, true]);
  });

  test('sets presence with local clone on connect', async () => {
    remotes.setLocal(mockLocal());
    mqtt.mockConnect();
    // Presence is joined when the remotes' live status resolves
    await comesAlive(remotes, false);
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
        '__presence/test.m-ld.org/+': { qos: 1 },
        'test.m-ld.org/operations/+': { qos: 1 },
        'test.m-ld.org/control': { qos: 1 },
        '__send/client1/+/+/test.m-ld.org/control': { qos: 0 },
        '__reply/client1/+/+/+': { qos: 0 }
      });
      // Presence ghost message
      expect(mqtt.publish).toBeCalledWith(
        '__presence/test.m-ld.org/client1', '-',
        { qos: 1 });
    });

    test('emits remote operations', async () => {
      mqtt.mockPublish('test.m-ld.org/operations/client2', MsgPack.encode({
        time: TreeClock.GENESIS.forked().left.toJson(), data: [0, 't1', '{}', '{}']
      }));
      await expect(new Promise((resolve) => {
        remotes.updates.subscribe({ next: resolve });
      })).resolves.toHaveProperty('data');
    });

    test('goes live if clone appears', async () => {
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await expect(remotes.live.pipe(take(3), toArray()).toPromise())
        .resolves.toEqual([null, false, true]);
    });

    test('sets presence with local clone', async () => {
      remotes.setLocal(mockLocal());
      // Presence is joined when the remotes' live status resolves
      await comesAlive(remotes, false);
      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '{"client1":"test.m-ld.org/control"}',
        { qos: 1, retain: true });
    });

    test('publishes local operations if live', async () => {
      // Set someone else's presence so we're marked live
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await comesAlive(remotes);

      const time = TreeClock.GENESIS.forked().left;
      const entry = new DeltaMessage(time.ticks,
        time.ticked(), [0, 't1', '{}', '{}']);
      const updates = new Source<DeltaMessage>();
      remotes.setLocal(mockLocal({ updates }));
      // Setting retained presence on the channel
      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '{"client1":"test.m-ld.org/control"}',
        { qos: 1, retain: true });
      updates.next(entry);
      expect(mqtt.publish).toBeCalled();
      await mqtt.lastPublish();
      await expect(entry.delivered).resolves.toBeUndefined();
    });

    test('closes if publish fails', async () => {
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await comesAlive(remotes);

      const updates = new Source<DeltaMessage>();
      remotes.setLocal(mockLocal({ updates }));

      mqtt.publish.mockReturnValue(<any>Promise.reject('Delivery failed'));

      const time = TreeClock.GENESIS.forked().left;
      updates.next(new DeltaMessage(time.ticks,
        time.ticked(), [0, 't1', '{}', '{}']));

      await expect(remotes.updates.toPromise()).rejects.toBe('Delivery failed');
    });

    test('live goes unknown if mqtt closes', async () => {
      mqtt.emit('close');
      expect(remotes.live.value).toBe(null);
    });

    test('closes with local clone', async () => {
      const updates = new Source<DeltaMessage>();
      remotes.setLocal(mockLocal({ updates }));
      updates.complete();
      remotes.setLocal(null);

      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '',
        { qos: 1, retain: true });
    });
  });

  describe('as a collaborator', () => {
    beforeEach(() => mqtt.mockConnect());

    test('can provide revup', async () => {
      // Local clone provides a rev-up on any request
      const { left: localTime, right: remoteTime } = TreeClock.GENESIS.forked();
      const revupUpdate = new DeltaMessage(remoteTime.ticks, remoteTime.ticked(), [0, 't1', '{}', '{}']);
      remotes.setLocal(mockLocal({
        revupFrom: () => Promise.resolve({ lastTime: remoteTime.ticked(), updates: of(revupUpdate) })
      }));

      // Send a rev-up request from an imaginary client2
      mqtt.mockPublish('__send/client1/client2/send1/test.m-ld.org/control',
        MsgPack.encode(new Request.Revup(localTime).toJson()));

      let updatesAddress: string, firstRevupBuffer: Buffer | null = null;
      const complete = new Future;
      mqtt.mockSubscribe((topic, payload) => {
        const [type, , , messageId] = topic.split('/');
        const json = Buffer.isBuffer(payload) ? MsgPack.decode(payload) : payload;
        if (type === '__reply' && json && json['@type'] === 'http://control.m-ld.org/response/revup') {
          // Ack the rev-up response when it arrives
          updatesAddress = (<Response.Revup>Response.fromJson(json)).updatesAddress;
          mqtt.mockPublish('__reply/client1/client2/ack1/' + messageId, MsgPack.encode(null));
        } else if (topic == `test.m-ld.org/control/${updatesAddress}`) {
          const notification = (<JsonNotification>json);
          if (notification.next != null)
            firstRevupBuffer = notification.next;
          else if (notification.complete)
            complete.resolve();
        }
      });
      await complete;
      expect(firstRevupBuffer).not.toBeNull();
      if (firstRevupBuffer != null)
        expect(revupUpdate.encode().equals(firstRevupBuffer)).toBe(true);
    });
  });

  describe('when not genesis', () => {
    beforeEach(() => mqtt.mockConnect());

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
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, fromId, messageId, domain,] = topic.split('/');
        const json = MsgPack.decode(payload);
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/clock') {
          expect(toId).toBe('consumer2');
          expect(fromId).toBe('client1');
          expect(domain).toBe('test.m-ld.org');
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, MsgPack.encode({
            '@type': 'http://control.m-ld.org/response/clock',
            clock: newClock.toJson()
          }));
        }
      });
      expect((await remotes.newClock()).equals(newClock)).toBe(true);
    });

    test('round robins for clock', async () => {
      const newClock = TreeClock.GENESIS.forked().right;
      // Set presence of client2's consumer
      await mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      await mqtt.mockPublish('__presence/test.m-ld.org/client3', '{"consumer3":"test.m-ld.org/control"}');
      let first = true;
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, , messageId] = topic.split('/');
        const json = MsgPack.decode(payload);
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/clock') {
          if (first) {
            first = false;
            mqtt.mockPublish(`__reply/client1/${toId}/reply1/` + messageId, MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/rejected',
              status: MeldErrorStatus['Request rejected']
            }));
          } else {
            mqtt.mockPublish(`__reply/client1/${toId}/reply1/` + messageId, MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/clock',
              clock: newClock.toJson()
            }));
          }
        }
      });
      expect((await remotes.newClock()).equals(newClock)).toBe(true);
    });

    test('cannot get revup of no-one present', async () => {
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left)).rejects.toThrow();
    });

    test('no revup if no collaborator', async () => {
      // Set presence of client2's consumer
      await mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, fromId, messageId, domain,] = topic.split('/');
        const json = MsgPack.decode(payload);
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/revup') {
          expect(toId).toBe('consumer2');
          expect(fromId).toBe('client1');
          expect(domain).toBe('test.m-ld.org');
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, MsgPack.encode({
            '@type': 'http://control.m-ld.org/response/revup',
            lastTime: null,
            updatesAddress: 'consumer2'
          }));
        }
      });
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left)).resolves.toBeUndefined();
    });

    test('can revup from first collaborator', async () => {
      // Set presence of client2's consumer
      await mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      mqtt.mockSubscribe((topic, payload) => {
        const [type, , , messageId] = topic.split('/');
        const json = MsgPack.decode(payload);
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/revup') {
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, MsgPack.encode({
            '@type': 'http://control.m-ld.org/response/revup',
            lastTime: TreeClock.GENESIS.forked().right.toJson(),
            updatesAddress: 'subChannel1'
          }));
        }
      });
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left)).resolves.toBeDefined();
    });

    test('can revup from second collaborator', async () => {
      // Set presence of client2's consumer
      await mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      await mqtt.mockPublish('__presence/test.m-ld.org/client3', '{"consumer3":"test.m-ld.org/control"}');
      let first = true;
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, , messageId] = topic.split('/');
        const json = MsgPack.decode(payload);
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/revup') {
          if (first) {
            first = false;
            mqtt.mockPublish(`__reply/client1/${toId}/reply1/${messageId}`, MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/revup',
              lastTime: null,
              updatesAddress: toId
            }));
          } else {
            mqtt.mockPublish(`__reply/client1/${toId}/reply2/${messageId}`, MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/revup',
              lastTime: TreeClock.GENESIS.forked().right.toJson(),
              updatesAddress: 'subChannel1'
            }));
          }
        }
      });
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left)).resolves.toBeDefined();
    });
  });
});
