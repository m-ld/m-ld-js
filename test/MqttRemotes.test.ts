import { MqttRemotes } from '../src/mqtt/MqttRemotes';
import { MockProxy, mock } from 'jest-mock-extended';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { MeldJournalEntry } from '../src/m-ld';
import { TreeClock } from '../src/clocks';
import { Subject as Source } from 'rxjs';
import { mockLocal } from './testClones';

describe('New MQTT remotes', () => {
  let mqtt: AsyncMqttClient & MockProxy<AsyncMqttClient>;
  let remotes: MqttRemotes;
  const published = Promise.resolve(mock<IPublishPacket>());

  function remotePublish(topic: string, json: any) {
    return new Promise<void>((resolve) => setImmediate(() => {
      mqtt.emit('message', topic, Buffer.from(
        typeof json === 'string' ? json : JSON.stringify(json)));
      resolve();
    }));
  }

  function remoteSubscribe(subscriber: (topic: string, json: any) => void) {
    mqtt.on('message', (topic, payload) => {
      try {
        subscriber(topic, JSON.parse(payload.toString()));
      } catch (err) {
        subscriber(topic, payload.toString());
      }
    });
  }

  beforeEach(() => {
    mqtt = new EventEmitter() as AsyncMqttClient & MockProxy<AsyncMqttClient>;
    mqtt = mock<AsyncMqttClient>(mqtt);
    // jest-mock-extended typing is confused by the AsyncMqttClient overloads, hence <any>
    mqtt.subscribe.mockReturnValue(<any>Promise.resolve([]));
    mqtt.publish.mockImplementation((topic, msg) => {
      remotePublish(topic, <string>msg).then(() => published);
      return <any>published;
    });
    remotes = new MqttRemotes('test.m-ld.org', 'client1', { hostname: 'unused' }, () => mqtt);
  });

  describe('when genesis', () => {
    beforeEach(async () => {
      // No more setup
      mqtt.emit('connect');
      await remotes.initialise();
    });

    test('subscribes to topics', () => {
      expect(mqtt.subscribe).toBeCalledWith({
        '__presence/test.m-ld.org/+': 1,
        'test.m-ld.org/operations': 1,
        'test.m-ld.org/control': 1,
        'test.m-ld.org/registry': 1,
        '__send/client1/+/+/test.m-ld.org/control': 0,
        '__reply/client1/+/+/+': 0
      });
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
      remotePublish('test.m-ld.org/operations', {
        time: TreeClock.GENESIS.forked().left.toJson(),
        data: { tid: 't1', insert: '{}', delete: '{}' }
      });
      await expect(new Promise((resolve) => {
        remotes.updates.subscribe({ next: resolve });
      })).resolves.toHaveProperty('data');
    });

    test('publishes local operations', async () => {
      const entry = {
        time: TreeClock.GENESIS.forked().left,
        data: { tid: 't1', insert: '{}', delete: '{}' },
        delivered: jest.fn()
      };
      const updates = new Source<MeldJournalEntry>();
      const ready = Promise.resolve();
      remotes.setLocal(mockLocal(updates));
      remotes.localReady = true;
      // Setting retained presence on the channel
      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '{"client1":"test.m-ld.org/control"}',
        { qos: 1, retain: true });
      updates.next(entry);

      expect(mqtt.publish).toBeCalled();
      await published;
      expect(entry.delivered).toBeCalled();
    });

    test('closes with connected clone', () => {
      const updates = new Source<MeldJournalEntry>();
      remotes.setLocal(mockLocal(updates));
      remotes.localReady = true;
      updates.complete();

      expect(mqtt.publish).lastCalledWith(
        '__presence/test.m-ld.org/client1',
        '',
        { qos: 1, retain: true });
    });
  });

  describe('when not genesis', () => {
    beforeEach(async () => {
      // Send retained Hello
      remotePublish('test.m-ld.org/registry', { id: 'client2' });
      mqtt.emit('connect');
      await remotes.initialise();
    });

    test('cannot get new clock if not genesis and no peers', async () => {
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
      await remotePublish('__presence/test.m-ld.org/client2', '{"consumer2":"test.m-ld.org/control"}');
      remoteSubscribe((topic, json) => {
        const [type, toId, fromId, messageId, domain,] = topic.split('/');
        if (type === '__send' && json['@type'] === 'http://control.m-ld.org/request/clock') {
          expect(toId).toBe('consumer2');
          expect(fromId).toBe('client1');
          expect(domain).toBe('test.m-ld.org');
          remotePublish('__reply/client1/consumer2/reply1/' + messageId, {
            '@type': 'http://control.m-ld.org/response/clock',
            clock: newClock.toJson()
          });
        }
      });
      expect((await remotes.newClock()).equals(newClock)).toBe(true);
    });
  });
});
