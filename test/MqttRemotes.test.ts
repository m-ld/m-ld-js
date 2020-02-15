import { MqttRemotes } from '../src/mqtt/MqttRemotes';
import { MockProxy, mock } from 'jest-mock-extended';
import { AsyncMqttClient, IPublishPacket } from 'async-mqtt';
import { EventEmitter } from 'events';
import { MeldJournalEntry, MeldLocal } from '../src/m-ld';
import { TreeClock } from '../src/clocks';
import { Subject } from 'rxjs';

describe('New MQTT remotes', () => {
  let mqtt: AsyncMqttClient & MockProxy<AsyncMqttClient>;
  let remotes: MqttRemotes;
  const published = Promise.resolve(mock<IPublishPacket>());

  function remotePublish(topic: string, json: any) {
    return new Promise<void>((resolve) => {
      setImmediate(() => {
        mqtt.emit('message', topic, Buffer.from(
          typeof json === 'string' ? json : JSON.stringify(json)));
        resolve();
      });
    });
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

  beforeEach(async () => {
    mqtt = new EventEmitter() as AsyncMqttClient & MockProxy<AsyncMqttClient>;
    mqtt.options = { clientId: 'client1' };
    mqtt = mock<AsyncMqttClient>(mqtt);
    // jest-mock-extended typing is confused by the AsyncMqttClient overloads, hence <any>
    mqtt.subscribe.mockReturnValue(<any>Promise.resolve([]));
    mqtt.publish.mockImplementation((topic, msg) => {
      remotePublish(topic, <string>msg).then(() => published);
      return <any>published;
    });

    remotes = new MqttRemotes('test.m-ld.org', mqtt);
    mqtt.emit('connect');
    await remotes.initialise();
  });

  test('subscribes to topics', async () => {
    expect(mqtt.subscribe).toBeCalledWith({
      '__presence/test.m-ld.org/#': 1,
      'test.m-ld.org/operations': 1,
      '__send/client1/+/+/test.m-ld.org/control': 0,
      '__reply/client1/+/+/+': 0
    });
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
    const updates = new Subject<MeldJournalEntry>();
    // This weirdness is due to jest-mock-extended trying to mock arrays
    remotes.connect({ ...mock<MeldLocal>(), updates });
    updates.next(entry);

    expect(mqtt.publish).toBeCalled();
    await published;
    expect(entry.delivered).toBeCalled();
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
    await remotePublish('__presence/test.m-ld.org/client2/consumer2', 'test.m-ld.org/control');
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