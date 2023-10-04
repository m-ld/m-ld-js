import { MockProxy } from 'jest-mock-extended';
import { AsyncMqttClient } from 'async-mqtt';
import { MockMqtt, mockMqtt } from './testClones';
import { MqttPresence } from '../src/mqtt/MqttPresence';
import { once } from 'events';

describe('MQTT presence', () => {
  let mqtt: MockMqtt & MockProxy<AsyncMqttClient>;
  let presence: MqttPresence;
  
  beforeEach(async () => {
    mqtt = mockMqtt();
    presence = new MqttPresence(mqtt, 'test.m-ld.org', 'client1');
  });

  test('will is expected', () => {
    expect(MqttPresence.will('test.m-ld.org', 'client1')).toEqual({
      topic: '__presence/test.m-ld.org/client1',
      payload: '', qos: 1, retain: true
    });
  });

  test('gets no presence', async () => {
    await presence.initialise();
    const present = [...presence.present('address')];
    expect(present).toEqual([]);
  });

  test('gets retained presence', async () => {
    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    await presence.initialise();
    const present = [...presence.present('address')];
    expect(present).toEqual(['consumer2']);
  });

  test('gets all retained', async () => {
    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    mqtt.mockPublish('__presence/test.m-ld.org/client3', '{"consumer3":"address"}');
    await presence.initialise();
    const present = [...presence.present('address')];
    expect(present).toEqual(['consumer2', 'consumer3']);
  });

  test('emits change when received all retained', async () => {
    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    mqtt.mockPublish('__presence/test.m-ld.org/client3', '{"consumer3":"address"}');
    presence.initialise();

    await once(presence, 'change');
    expect([...presence.present('address')]).toEqual(['consumer2', 'consumer3']);
  });

  test('emits change when no retained', async () => {
    presence.initialise();

    await once(presence, 'change');
    expect([...presence.present('address')]).toEqual([]);
  });

  test('emits when presence changed', async () => {
    presence.initialise();
    await once(presence, 'change');

    mqtt.mockPublish('__presence/test.m-ld.org/client2', '{"consumer2":"address"}');
    await once(presence, 'change');
    expect([...presence.present('address')]).toEqual(['consumer2']);
  });

  test('when racing, neither client sees the other', async () => {
    const presence2 = new MqttPresence(mqtt, 'test.m-ld.org', 'client2');
    presence.initialise();
    presence2.initialise();
    presence.join('consumer1', 'address');
    presence2.join('consumer2', 'address');
    // First change see self presence
    await expect(Promise.all([
      once(presence, 'change').then(() => [...presence.present('address')]),
      once(presence2, 'change').then(() => [...presence2.present('address')])
    ])).resolves.toEqual([['consumer1'], ['consumer2']]);
  });
});
