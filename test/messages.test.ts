import { TreeClockMessageService, Message } from '../src/messages'
import { TreeClock } from '../src/clocks';

test('First message send', () => {
  const p1 = new TreeClockMessageService(TreeClock.GENESIS);
  p1.send();
  expect(p1.peek().ticks).toBe(1);
});

test('First message receive', () => {
  const { left: p1Clock, right: p2Clock } = TreeClock.GENESIS.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const messages: Message<TreeClock, string>[] = [];
  p1.receive({ time: p2Clock.ticked(), data: 'Foo' }, [], msg => messages.push(msg));
  expect(p1.peek().equals(p1Clock.ticked().update(p2Clock.ticked()))).toBe(true);
  expect(messages[0].data).toBe('Foo');
});

test('First message buffer', () => {
  const { left: p1Clock, right } = TreeClock.GENESIS.forked();
  const { left: p2Clock, right: p3Clock } = right.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const buffer: Message<TreeClock, string>[] = [];
  p1.receive({
    // Tick the p3 clock to simulate a missed message
    time: p2Clock.ticked().update(p3Clock.ticked()),
    data: 'Bar'
  }, buffer, () => {});
  expect(buffer[0].data).toBe('Bar');
});

test('First message unbuffer', () => {
  const { left: p1Clock, right } = TreeClock.GENESIS.forked();
  const { left: p2Clock, right: p3Clock } = right.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const buffer: Message<TreeClock, string>[] = [];
  const messages: Message<TreeClock, string>[] = [];
  p1.receive({
    // Tick the p3 clock to simulate a missed message
    time: p2Clock.ticked().update(p3Clock.ticked()),
    data: 'Bar'
  }, buffer, msg => messages.push(msg));
  p1.receive({
    // Receive the missed message
    time: p3Clock.ticked(),
    data: 'Foo'
  }, buffer, msg => messages.push(msg));
  expect(messages[0].data).toBe('Foo');
  expect(messages[1].data).toBe('Bar');
  expect(buffer.length).toBe(0);
});