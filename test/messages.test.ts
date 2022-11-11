import { Message, TreeClockMessageService } from '../src/engine/messages';
import { TreeClock } from '../src/engine/clocks';

test('First message send', () => {
  const p1 = new TreeClockMessageService(TreeClock.GENESIS);
  p1.event();
  expect(p1.peek().ticks).toBe(1);
});

test('First message receive', async () => {
  const { left: p1Clock, right: p2Clock } = TreeClock.GENESIS.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const messages: Message<TreeClock, string>[] = [];
  await p1.receive({ time: p2Clock.ticked(), data: 'Foo' }, [], async msg => {
    p1.event();
    messages.push(msg);
  });
  expect(p1.peek().equals(p1Clock.ticked().update(p2Clock.ticked()))).toBe(true);
  expect(messages[0].data).toBe('Foo');
});

test('First message buffer', async () => {
  const { left: p1Clock, right } = TreeClock.GENESIS.forked();
  const { left: p2Clock, right: p3Clock } = right.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const buffer: Message<TreeClock, string>[] = [];
  await p1.receive({
    // Tick the p3 clock to simulate a missed message
    time: p2Clock.ticked().update(p3Clock.ticked()),
    data: 'Bar'
  }, buffer, async () => {});
  expect(buffer[0].data).toBe('Bar');
});

test('First message un-buffer', async () => {
  const { left: p1Clock, right } = TreeClock.GENESIS.forked();
  const { left: p2Clock, right: p3Clock } = right.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const buffer: Message<TreeClock, string>[] = [];
  const messages: Message<TreeClock, string>[] = [];
  await p1.receive({
    // Tick the p3 clock to simulate a missed message
    time: p2Clock.ticked().update(p3Clock.ticked()),
    data: 'Bar'
  }, buffer, async msg => messages.push(msg));
  await p1.receive({
    // Receive the missed message
    time: p3Clock.ticked(),
    data: 'Foo'
  }, buffer, async msg => messages.push(msg));
  expect(messages[0].data).toBe('Foo');
  expect(messages[1].data).toBe('Bar');
  expect(buffer.length).toBe(0);
});

test('Accept own more recent message', async () => {
  const { left: p1Clock } = TreeClock.GENESIS.forked();
  const p1 = new TreeClockMessageService(p1Clock);
  const messages: Message<TreeClock, string>[] = [];
  await p1.receive({
    time: p1Clock.ticked().ticked(), data: 'Foo'
  }, [], async msg => messages.push(msg));
  expect(p1.peek().equals(p1Clock.ticked().ticked())).toBe(true);
  expect(messages[0].data).toBe('Foo');
});