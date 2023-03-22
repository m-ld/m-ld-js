// noinspection ES6MissingAwait

import { MqttRemotes } from '../src/mqtt';
import { mock, MockProxy } from 'jest-mock-extended';
import { AsyncMqttClient } from 'async-mqtt';
import { GlobalClock, TreeClock } from '../src/engine/clocks';
import { firstValueFrom, of, Subject as Source } from 'rxjs';
import { mockLocal, MockMqtt, mockMqtt, MockProcess, testOp } from './testClones';
import { take, toArray } from 'rxjs/operators';
import { comesAlive } from '../src/engine/AbstractMeld';
import { MeldErrorStatus } from '@m-ld/m-ld-spec';
import { Response, RevupRequest, RevupResponse } from '../src/engine/remotes/ControlMessage';
import * as MsgPack from '../src/engine/msgPack';
import { JsonNotification } from '../src/engine/remotes';
import { once } from 'events';
import { MeldOperationMessage } from '../src/engine/MeldOperationMessage';
import { Future } from '../src/engine/Future';

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
    }, () => Promise.resolve({}), () => mqtt);
  });

  test('live starts unknown', async () => {
    expect(remotes.live.value).toBe(null);
  });

  test('goes offline if no other clones', async () => {
    mqtt.mockConnect();
    await expect(firstValueFrom(remotes.live.pipe(take(2), toArray())))
      .resolves.toEqual([null, false]);
  });

  test('goes live if clone already present', async () => {
    mqtt.mockPublish(
      '__presence/test.m-ld.org/client2',
      '{"consumer2":"test.m-ld.org/control"}');
    mqtt.mockConnect();
    await expect(firstValueFrom(remotes.live.pipe(take(2), toArray())))
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

  test('closes when local clone removed', async () => {
    remotes.setLocal(mockLocal());
    mqtt.mockConnect();
    // Presence is joined when the remotes' live status resolves
    await comesAlive(remotes, false);

    remotes.setLocal(null);
    await once(mqtt, 'close');
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
      const time = TreeClock.GENESIS.forked().left;
      const op = MeldOperationMessage.fromOperation(time.ticks, testOp(time.ticked()), null);
      mqtt.mockPublish('test.m-ld.org/operations/client2', MeldOperationMessage.toBuffer(op));
      await expect(new Promise((resolve) => {
        remotes.operations.subscribe({ next: resolve });
      })).resolves.toHaveProperty('data');
    });

    test('emits remote operations as Uint8Array', async () => {
      const time = TreeClock.GENESIS.forked().left;
      const op = MeldOperationMessage.fromOperation(time.ticks, testOp(time.ticked()), null);
      mqtt.mockPublish('test.m-ld.org/operations/client2',
        new Uint8Array(MeldOperationMessage.toBuffer(op)));
      await expect(new Promise((resolve) => {
        remotes.operations.subscribe({ next: resolve });
      })).resolves.toHaveProperty('data');
    });

    test('goes live if clone appears', async () => {
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await expect(firstValueFrom(remotes.live.pipe(take(3), toArray())))
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

      const entry = new MockProcess(TreeClock.GENESIS.forked().left).sentOperation({}, {});
      const operations = new Source<MeldOperationMessage>();
      remotes.setLocal(mockLocal({ operations }));
      // Setting retained presence on the channel
      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '{"client1":"test.m-ld.org/control"}',
        { qos: 1, retain: true });
      operations.next(entry);
      expect(mqtt.publish).toBeCalled();
      await mqtt.lastPublish();
    });

    test('closes if publish fails', async () => {
      mqtt.mockPublish(
        '__presence/test.m-ld.org/client2',
        '{"consumer2":"test.m-ld.org/control"}');
      await comesAlive(remotes);

      const operations = new Source<MeldOperationMessage>();
      remotes.setLocal(mockLocal({ operations }));

      mqtt.publish.mockReturnValueOnce(<any>Promise.reject('Delivery failed'));

      operations.next(new MockProcess(TreeClock.GENESIS.forked().left).sentOperation({}, {}));

      await expect(firstValueFrom(remotes.operations)).rejects.toBe('Delivery failed');
    });

    test('live goes unknown if mqtt closes', async () => {
      mqtt.mockClose();
      expect(remotes.live.value).toBe(null);
    });

    test('closes with local clone', async () => {
      const operations = new Source<MeldOperationMessage>();
      remotes.setLocal(mockLocal({ operations }));
      operations.complete();
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
      const revupUpdate = new MockProcess(remoteTime).sentOperation({}, {});
      const gwc = GlobalClock.GENESIS.set(localTime).set(remoteTime);
      remotes.setLocal(mockLocal({
        revupFrom: () => Promise.resolve({
          gwc, updates: of(revupUpdate), cancel() {}
        })
      }));

      // Send a rev-up request from an imaginary client2
      mqtt.mockPublish('__send/client1/client2/send1/test.m-ld.org/control',
        new RevupRequest(localTime).toBuffer());

      let updatesAddress: string, firstRevupBuffer: Buffer | null = null;
      const complete = new Future;
      mqtt.mockSubscribe((topic, payload) => {
        const [type, , , messageId] = topic.split('/');
        const json = Buffer.isBuffer(payload) ? MsgPack.decode(payload) : payload;
        if (type === '__reply' && json?.enc &&
          MsgPack.decode(json.enc)['@type'] === 'http://control.m-ld.org/response/revup') {
          // Ack the rev-up response when it arrives
          updatesAddress =
            (<RevupResponse>Response.fromBuffer(payload)).updatesAddress;
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
        expect(MeldOperationMessage.toBuffer(revupUpdate).equals(firstRevupBuffer)).toBe(true);
    });
  });

  describe('when not genesis', () => {
    beforeEach(() => {
      remotes.setLocal(mockLocal());
      mqtt.mockConnect();
    });

    test('cannot get new clock if no peers', done => {
      remotes.newClock().then(() => { throw 'expecting error'; }, error => {
        expect(error.message).toMatch(/No-one present/);
        done();
      });
    });

    test('can get clock', async () => {
      const newClock = TreeClock.GENESIS.forked().right;
      // Set presence of client2's consumer
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, fromId, messageId, domain] = topic.split('/');
        const json = Buffer.isBuffer(payload) && MsgPack.decode(payload);
        if (type === '__send' && MsgPack.decode(json.enc)['@type'] ===
          'http://control.m-ld.org/request/clock') {
          expect(toId).toBe('consumer2');
          expect(fromId).toBe('client1');
          expect(domain).toBe('test.m-ld.org');
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, MsgPack.encode({
            enc: MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/clock',
              clock: newClock.toJSON()
            }),
            attr: null
          }));
        }
      });
      expect((await remotes.newClock()).equals(newClock)).toBe(true);
    });

    test('round robins for clock', async () => {
      const newClock = TreeClock.GENESIS.forked().right;
      // Set presence of client2's consumer
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client3', '{"consumer3":"test.m-ld.org/control"}');
      let first = true;
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, , messageId] = topic.split('/');
        const json = Buffer.isBuffer(payload) && MsgPack.decode(payload);
        if (type === '__send' && MsgPack.decode(json.enc)['@type'] ===
          'http://control.m-ld.org/request/clock') {
          if (first) {
            first = false;
            mqtt.mockPublish(`__reply/client1/${toId}/reply1/` + messageId, MsgPack.encode({
              enc: MsgPack.encode({
                '@type': 'http://control.m-ld.org/response/rejected',
                status: MeldErrorStatus['Request rejected']
              }),
              attr: null
            }));
          } else {
            mqtt.mockPublish(`__reply/client1/${toId}/reply1/` + messageId, MsgPack.encode({
              enc: MsgPack.encode({
                '@type': 'http://control.m-ld.org/response/clock',
                clock: newClock.toJSON()
              }),
              attr: null
            }));
          }
        }
      });
      expect((await remotes.newClock()).equals(newClock)).toBe(true);
    });

    test('cannot get revup of no-one present', async () => {
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left, mock())).rejects.toThrow();
    });

    test('no revup if no collaborator', async () => {
      // Set presence of client2's consumer
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, fromId, messageId, domain] = topic.split('/');
        const json = Buffer.isBuffer(payload) && MsgPack.decode(payload);
        if (type === '__send' && MsgPack.decode(json.enc)['@type'] ===
          'http://control.m-ld.org/request/revup') {
          expect(toId).toBe('consumer2');
          expect(fromId).toBe('client1');
          expect(domain).toBe('test.m-ld.org');
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, MsgPack.encode({
            enc: MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/revup',
              gwc: null, updatesAddress: 'consumer2'
            }),
            attr: null
          }));
        }
      });
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left, mock()))
        .resolves.toBeUndefined();
    });

    test('can revup from first collaborator', async () => {
      // Set presence of client2's consumer
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      mqtt.mockSubscribe((topic, payload) => {
        const [type, , , messageId] = topic.split('/');
        const json = Buffer.isBuffer(payload) && MsgPack.decode(payload);
        if (type === '__send' && MsgPack.decode(json.enc)['@type'] ===
          'http://control.m-ld.org/request/revup') {
          mqtt.mockPublish('__reply/client1/consumer2/reply1/' + messageId, MsgPack.encode({
            enc: MsgPack.encode({
              '@type': 'http://control.m-ld.org/response/revup',
              gwc: GlobalClock.GENESIS.set(TreeClock.GENESIS.forked().right).toJSON(),
              updatesAddress: 'subChannel1'
            }),
            attr: null
          }));
        }
      });
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left, mock()))
        .resolves.toBeDefined();
    });

    test('can revup from second collaborator', async () => {
      // Set presence of client2's consumer
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      await mqtt.mockPublish(
        '__presence/test.m-ld.org/client3', '{"consumer3":"test.m-ld.org/control"}');
      let first = true;
      mqtt.mockSubscribe((topic, payload) => {
        const [type, toId, , messageId] = topic.split('/');
        const json = Buffer.isBuffer(payload) && MsgPack.decode(payload);
        if (type === '__send' && MsgPack.decode(json.enc)['@type'] ===
          'http://control.m-ld.org/request/revup') {
          if (first) {
            first = false;
            mqtt.mockPublish(`__reply/client1/${toId}/reply1/${messageId}`, MsgPack.encode({
              enc: MsgPack.encode({
                '@type': 'http://control.m-ld.org/response/revup',
                gwc: null, updatesAddress: toId
              }),
              attr: null
            }));
          } else {
            mqtt.mockPublish(`__reply/client1/${toId}/reply2/${messageId}`, MsgPack.encode({
              enc: MsgPack.encode({
                '@type': 'http://control.m-ld.org/response/revup',
                gwc: GlobalClock.GENESIS.set(TreeClock.GENESIS.forked().right).toJSON(),
                updatesAddress: 'subChannel1'
              }),
              attr: null
            }));
          }
        }
      });
      await expect(remotes.revupFrom(TreeClock.GENESIS.forked().left, mock()))
        .resolves.toBeDefined();
    });
  });
});
