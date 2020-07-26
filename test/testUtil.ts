import { SchedulerLike, SchedulerAction, Subscription } from 'rxjs';
import { shortId } from '../src';

export class TickableScheduler implements SchedulerLike {
  ticks: number = 1; // >0 to not confuse an epoch baseline
  work: { [tick: number]: { [id: string]: () => void } } = {};
  now = () => this.ticks;
  schedule<T>(work: (this: SchedulerAction<T>, state?: T) => void, delay: number = 0, state?: T): Subscription {
    const id = shortId(), time = this.ticks + delay, scheduler = this;
    const action: SchedulerAction<T> = Object.assign(new Subscription(() => delete this.work[time][id]), {
      schedule: (state?: T, delay?: number) => scheduler.schedule(work, delay, state)
    });
    (this.work[time] = this.work[time] ?? {})[id] = () => work.call(action, state);
    if (delay === 0)
      this.tick(0);
    return action;
  }
  tick(n: number = 1) {
    this.ticks += n;
    Object.values(this.work[this.ticks] ?? {}).forEach(execute => execute());
  }
}
