import { MqttTopic } from '../src/mqtt/MqttTopic';

describe('MQTT topic pattern', () => {
  test('fixed to string', () => {
    expect(new MqttTopic(['a', 'b']).address).toBe('a/b');
  });

  test('single wildcard to string', () => {
    expect(new MqttTopic<{ b: string }>(['a', { '+': 'b' }]).address).toBe('a/+');
  });

  test('multi wildcard to string', () => {
    expect(new MqttTopic<{ b: string[] }>(['a', { '#': 'b' }]).address).toBe('a/#');
  });

  test('mixed wildcards to string', () => {
    expect(new MqttTopic<{ a: string, b: string[] }>([{ '+': 'a' }, { '#': 'b' }]).address).toBe('+/#');
  });

  test('single wildcard with value', () => {
    expect(new MqttTopic<{ b: string }>(['a', { '+': 'b' }])
      .with({ b: 'b' }).address).toBe('a/b');
  });

  test('multi wildcard with value', () => {
    expect(new MqttTopic<{ b: string[] }>(['a', { '#': 'b' }])
      .with({ b: ['b', 'c'] }).address).toBe('a/b/c');
  });

  test('mixed wildcards with values', () => {
    expect(new MqttTopic<{ a: string, b: string[] }>([{ '+': 'a' }, { '#': 'b' }])
      .with({ a: 'a', b: ['b', 'c'] }).address).toBe('a/b/c');
  });

  test('mixed wildcards with partial multi values', () => {
    expect(new MqttTopic<{ a: string, b: string[] }>([{ '+': 'a' }, { '#': 'b' }])
      .with({ a: 'a' }).address).toBe('a/#');
  });

  test('mixed wildcards with partial single values', () => {
    expect(new MqttTopic<{ a: string, b: string[] }>([{ '+': 'a' }, { '#': 'b' }])
      .with({ b: ['b', 'c'] }).address).toBe('+/b/c');
  });

  test('no match', () => {
    expect(new MqttTopic<{ b: string }>(['a', { '+': 'b' }])
      .match('c/b')).toBeNull();
  });

  test('single wildcard match', () => {
    expect(new MqttTopic<{ b: string }>(['a', { '+': 'b' }])
      .match('a/b')).toEqual({ b: 'b' });
  });

  test('multi wildcard match', () => {
    expect(new MqttTopic<{ b: string[] }>(['a', { '#': 'b' }])
      .match('a/b/c')).toEqual({ b: ['b', 'c'] });
  });

  test('mixed wildcards match', () => {
    expect(new MqttTopic<{ a: string, b: string[] }>([{ '+': 'a' }, { '#': 'b' }])
      .match('a/b/c')).toEqual({ a: 'a', b: ['b', 'c'] });
  });
});