import { MockProxy } from 'jest-mock-extended';
import { AsyncMqttClient } from 'async-mqtt';
import { fromEvent } from 'rxjs';
import { MockMqtt, mockMqtt } from './testClones';
import { first, toArray, concatMap } from 'rxjs/operators';
import { MqttPresence } from '../src/mqtt/MqttPresence';

describe('MQTT presence', () => {
  let mqtt: MockMqtt & MockProxy<AsyncMqttClient>;
  let presence: MqttPresence;
  
  function nextChange(presence: MqttPresence) {
    return fromEvent(presence, 'change').pipe(
      concatMap(() => presence.present('address').pipe(toArray())),
      first()).toPromise();
  }

  beforeEach(() => {
    mqtt = mockMqtt();
    presence = new MqttPresence(mqtt, 'test.m-ld.org', 'client1');
  });

  test('will is expected', () => {
    expect(MqttPresence.will('test.m-ld.org', 'client1')).toEqual({
      topic: '__presence/test.m-ld.org/client1',
      payload: '', qos: 1, retain: true
    });
  });

  test('connect subscribes', () => {
    mqtt.mockConnect();
    expect(mqtt.subscribe).toBeCalledWith({ '__presence/test.m-ld.org/+': 1 });
  });

  test('gets no presence', async () => {
    mqtt.mockConnect();
    const present = presence.present('address').pipe(toArray()).toPromise();
    await expect(present).resolves.toEqual([]);
  });

  test('gets retained presence', async () => {
    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    mqtt.mockConnect();
    const present = presence.present('address').pipe(toArray()).toPromise();
    await expect(present).resolves.toEqual(['consumer2']);
  });

  test('gets all retained', async () => {
    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    mqtt.mockPublish('__presence/test.m-ld.org/client3', '{"consumer3":"address"}');
    mqtt.mockConnect();
    const present = presence.present('address').pipe(toArray()).toPromise();
    await expect(present).resolves.toEqual(['consumer2', 'consumer3']);
  });

  test('emits change when received all retained', async () => {
    const present = fromEvent(presence, 'change').pipe(
      concatMap(() => presence.present('address').pipe(toArray())));

    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    mqtt.mockPublish('__presence/test.m-ld.org/client3', '{"consumer3":"address"}');
    mqtt.mockConnect();

    await expect(present.pipe(first()).toPromise()).resolves.toEqual(['consumer2', 'consumer3']);
  });

  test('emits change when no retained', async () => {
    const present = fromEvent(presence, 'change').pipe(
      concatMap(() => presence.present('address').pipe(toArray())));

    mqtt.mockConnect();

    await expect(present.pipe(first()).toPromise()).resolves.toEqual([]);
  });

  test('emits when presence changed', async () => {
    mqtt.mockConnect();
    await expect(nextChange(presence)).resolves.toEqual([]);

    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    await expect(nextChange(presence)).resolves.toEqual(['consumer2']);
  });

  test('when racing, lower client wins', async () => {
    const presence2 = new MqttPresence(mqtt, 'test.m-ld.org', 'client2');
    mqtt.on('connect', () => {
      presence.join('consumer1', 'address');
      presence2.join('consumer2', 'address');
    });
    mqtt.mockConnect();
    await expect(Promise.all([
      nextChange(presence),
      nextChange(presence2)
    ])).resolves.toEqual([
      ['consumer1'],
      ['consumer1', 'consumer2']
    ]);
  });
});
