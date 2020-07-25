import { AblyTraffic } from '../src/ably/AblyTraffic';
import { mock } from 'jest-mock-extended';
import { Types } from 'ably';
import { TickableScheduler } from './testUtil';

describe('Ably traffic shaping', () => {
  test('subscribes', async () => {
    const channel = mock<Types.RealtimeChannelPromise>();
    channel.subscribe.mockReturnValue(Promise.resolve());
    const handler = () => { };
    await expect(new AblyTraffic({})
      .subscribe(channel, handler))
      .resolves.toBeUndefined();
    expect(channel.subscribe).toBeCalled();
  });

  test('publishes immediately if not shaping', async () => {
    const channel = mock<Types.RealtimeChannelPromise>();
    channel.publish.mockReturnValue(Promise.resolve());
    await expect(new AblyTraffic({})
      .publish(channel, 'message', {}))
      .resolves.toBeUndefined();
    expect(channel.publish).toBeCalledWith('message', {});
  });

  test('publishes one immediately even if rate limiting', async () => {
    const channel = mock<Types.RealtimeChannelPromise>();
    channel.publish.mockReturnValue(Promise.resolve());
    await expect(new AblyTraffic({ maxRate: 1 })
      .publish(channel, 'message', {}))
      .resolves.toBeUndefined();
    expect(channel.publish).toBeCalledWith('message', {});
  });

  test('publishes another after tick if rate limiting', async () => {
    const channel = mock<Types.RealtimeChannelPromise>();
    channel.publish.mockReturnValue(Promise.resolve());
    const scheduler = new TickableScheduler();
    const traffic = new AblyTraffic({ maxRate: 1000 }, scheduler);
    traffic.publish(channel, 'message1', {});
    traffic.publish(channel, 'message2', {});
    expect(channel.publish).toBeCalledWith('message1', {});
    expect(channel.publish.mock.calls.length).toBe(1);
    scheduler.tick();
    expect(channel.publish).toBeCalledWith('message2', {});
  });

  test('publishes immediately if rate is low', async () => {
    const channel = mock<Types.RealtimeChannelPromise>();
    channel.publish.mockReturnValue(Promise.resolve());
    const scheduler = new TickableScheduler();
    const traffic = new AblyTraffic({ maxRate: 1000 }, scheduler);
    traffic.publish(channel, 'message1', {});
    expect(channel.publish).toBeCalledWith('message1', {});
    scheduler.tick();
    traffic.publish(channel, 'message2', {});
    expect(channel.publish).toBeCalledWith('message2', {});
  });

  test('maintains ordering even if scheduler misbehaves', async () => {
    const channel = mock<Types.RealtimeChannelPromise>();
    channel.publish.mockReturnValue(Promise.resolve());
    const scheduler = new TickableScheduler();
    const traffic = new AblyTraffic({ maxRate: 1000 }, scheduler);
    traffic.publish(channel, 'message1', {});
    traffic.publish(channel, 'message2', {});
    traffic.publish(channel, 'message3', {});
    scheduler.tick(2); // when 3 would schedule if naive
    scheduler.tick(-1); // when 2 would schedule
    scheduler.tick(1); // 3 is actually not scheduled until 2 clears
    expect(channel.publish.mock.calls).toEqual([
      ['message1', {}], ['message2', {}], ['message3', {}]
    ]);
  });
});