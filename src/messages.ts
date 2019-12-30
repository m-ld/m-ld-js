import { TreeClock, CausalClock } from "./clocks";

export interface Message<C, D> {
  time: C;
  data: D;
}

export abstract class MessageService<C extends CausalClock<C>> {
  send(): C {
    this.event();
    return this.peek();
  }

  receive<M extends Message<C, unknown>>(message: M, buffer: M[], process: (message: M) => void) {
    if (this.readyFor(message.time)) {
      this.event();
      this.deliver(message, buffer, process);
    } else {
      buffer.push(message);
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
      this.event();
      this.deliver(msg, buffer, process);
    }
  }

  abstract peek(): C;
  abstract event(): void;
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

  event(): void {
    const ticked = this.localTime.ticked();
    if (ticked)
      this.localTime = ticked;
    else
      throw new Error('Local time is not consistent');
  }

  join(time: TreeClock): void {
    this.localTime = this.localTime.update(time);
  }

  fork(): TreeClock {
    const fork = this.localTime.forked();
    if (fork) {
      this.localTime = fork.left;
      return fork.right;
    } else {
      throw new Error('Local time is not consistent');
    }
  }
}

