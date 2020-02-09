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

  beforeEach(async () => {
    mqtt = new EventEmitter() as AsyncMqttClient & MockProxy<AsyncMqttClient>;
    mqtt.options = { clientId: 'clientId' };
    mqtt = mock<AsyncMqttClient>(mqtt);
    // jest-mock-extended typing is confused by the AsyncMqttClient overloads, hence <any>
    mqtt.subscribe.mockReturnValue(<any>Promise.resolve([]));
    mqtt.publish.mockReturnValue(<any>published);

    remotes = new MqttRemotes('test.m-ld.org', mqtt);
    mqtt.emit('connect');
    await remotes.initialise();
  });

  test('subscribes to topics', async () => {
    expect(mqtt.subscribe).toBeCalledWith({
      '__presence/test.m-ld.org/#': 1,
      'test.m-ld.org/operations': 1,
      '__send/clientId/+/+/test.m-ld.org/control': 0,
      '__reply/clientId/+/+/+': 0
    });
  });

  test('emits remote operations', async () => {
    setImmediate(() => mqtt.emit('message', 'test.m-ld.org/operations', Buffer.from(JSON.stringify({
      time: TreeClock.GENESIS.forked().left.toJson(),
      data: { tid: 't1', insert: '{}', delete: '{}' }
    }), 'utf8')));
    await expect(new Promise((resolve) => {
      remotes.updates.subscribe({ next: resolve });
    })).resolves.toHaveProperty('data');
  });

  test('publishes local operations', async () => {
    const entry = mock<MeldJournalEntry>({
      time: TreeClock.GENESIS.forked().left,
      data: { tid: 't1', insert: '{}', delete: '{}' }
    });
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
});