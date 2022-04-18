import { CausalClock, TickTree, TreeClock } from './clocks';

/**
 * An entity carrying a clock which can be manipulated.
 */
export interface ClockHolder<C extends CausalClock> {
  /** Inspect the time according to our clock */
  peek(): C;
  /** Tick the clock to acknowledge some event */
  event(): C;
  /** Synchronise our clock with the given time */
  join(time: C): this;
  /** Arbitrarily reset our clock to the given time */
  push(time: C): this;
}

export interface Message<C, D> {
  time: C;
  data: D;
}

/**
 * A process that accepts messages one at a time
 */
export type Process<M, C extends CausalClock> = (message: M, prev: C) => Promise<unknown>;

/**
 * Encapsulates the updating of a local clock time and the delivery of messages
 * in causal order.
 *
 * While the {@link receive} method will wait for each delivered message to be
 * processed, this class is not otherwise safe from concurrent modification
 * (such as using {@link event}), and should therefore be used with suitable
 * synchronisation.
 */
export abstract class MessageService<C extends CausalClock> implements ClockHolder<C> {
  /**
   * Call to process newly received message data from the wire.
   *
   * The given process MUST tick the clock using {@link event()} prior to sending
   * any data that is caused by receipt of this message.
   *
   * @param message the message from the wire
   * @param buffer  a buffer for out-of-order messages
   * @param process the local message data consumer, which will receive message
   * data in order
   * @return <code>true</code> if the message was delivered, <code>false</code>
   * if buffered
   */
  async receive<M extends Message<C, unknown>>(
    message: M,
    buffer: M[],
    process: Process<M, C>
  ): Promise<boolean> {
    if (this.readyFor(message.time)) {
      await this.deliver(message, buffer, process);
      return true;
    } else {
      buffer.push(message);
      return false;
    }
  }

  async deliver<M extends Message<C, unknown>>(message: M, buffer: M[], process: Process<M, C>) {
    const prev = this.peek();
    this.join(message.time);
    await process(message, prev);
    await this.reconsider(buffer, process);
  }

  async reconsider<M extends Message<C, unknown>>(buffer: M[], process: Process<M, C>) {
    const readyForIdx = buffer.findIndex(msg => this.readyFor(msg.time));
    if (readyForIdx > -1) {
      const msg = buffer[readyForIdx];
      buffer.splice(readyForIdx, 1);
      await this.deliver(msg, buffer, process);
    }
  }

  abstract peek(): C;
  abstract event(): C;
  abstract join(time: C): this;
  abstract push(time: C): this;

  private readyFor(senderTime: C) {
    return !this.peek().anyNonIdLt(senderTime);
  }
}

export class TreeClockMessageService extends MessageService<TreeClock> {
  private localTime: TreeClock;

  constructor(localTime: TreeClock) {
    super();
    this.localTime = localTime;
  }

  peek(): TreeClock {
    return this.localTime;
  }

  event(): TreeClock {
    return this.localTime = this.localTime.ticked();
  }

  join(time: TickTree) {
    this.localTime = this.localTime.update(time);
    return this;
  }

  push(time: TreeClock) {
    this.localTime = time;
    return this;
  }
}

