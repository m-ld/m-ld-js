import { TreeClock, CausalClock } from "./clocks";

export interface Message<C, D> {
  time: C;
  data: D;
}

export abstract class MessageService<C extends CausalClock<C>> {
  /**
   * Call to process newly received message data from the wire.
   *
   * The given process MUST tick the clock using {@link event} prior to sending
   * any data that is caused by receipt of this message.
   *
   * @param message the message from the wire
   * @param buffer  a buffer for out-of-order messages
   * @param process the local message data consumer, which will receive message
   * data in order
   * @return <code>true</code> if the message was delivered, <code>false</code>
   * if buffered
   */
  receive<M extends Message<C, unknown>>(
    message: M, buffer: M[], process: (message: M) => void): boolean {
    if (this.readyFor(message.time)) {
      this.deliver(message, buffer, process);
      return true;
    } else {
      buffer.push(message);
      return false;
    }
  }

  deliver<M extends Message<C, unknown>>(message: M, buffer: M[], process: (message: M) => void) {
    this.join(message.time);
    process(message);
    this.reconsider(buffer, process);
  }

  reconsider<M extends Message<C, unknown>>(buffer: M[], process: (message: M) => void) {
    const readyForIdx = buffer.findIndex(msg => this.readyFor(msg.time));
    if (readyForIdx > -1) {
      const msg = buffer[readyForIdx];
      buffer.splice(readyForIdx, 1);
      this.deliver(msg, buffer, process);
    }
  }

  abstract peek(): C;
  abstract event(): C;
  abstract join(time: C): void;
  abstract fork(): C;

  private readyFor(senderTime: C) {
    return !this.peek().anyLt(senderTime);
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

  join(time: TreeClock): void {
    this.localTime = this.localTime.update(time);
  }

  fork(): TreeClock {
    const fork = this.localTime.forked();
    this.localTime = fork.left;
    return fork.right;
  }
}

