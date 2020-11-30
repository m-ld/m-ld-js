import { Future } from '../engine/util';
import * as Ably from 'ably';
import { Subject, Observable, asyncScheduler, of, SchedulerLike } from 'rxjs';
import { concatMap, delay } from 'rxjs/operators';

export interface AblyTrafficConfig {
  /**
   * Max message rate in Hertz. Default is Infinity.
   * @see https://support.ably.com/support/solutions/articles/3000079684
   */
  maxRate?: number;
  /**
   * Max message size in bytes. Default is Infinity.
   * @see https://support.ably.com/support/solutions/articles/3000035792
   */
  maxSize?: number;
}

class ChannelMessage {
  published: Future = new Future;

  constructor(
    readonly channel: Ably.Types.RealtimeChannelPromise,
    readonly name: string,
    readonly data: object | null) {
  }

  publish = () => this.channel.publish(this.name, this.data)
    .then(...this.published.settle);
}

export class AblyTraffic {
  readonly outbound: Subject<ChannelMessage> = new Subject;

  constructor(config: AblyTrafficConfig, readonly scheduler: SchedulerLike = asyncScheduler) {
    let outLimited: Observable<ChannelMessage> = this.outbound;
    if (config.maxRate != null) {
      const gap = 1000 / config.maxRate;
      let prev = 0;
      // Use of concatMap guarantees ordering
      outLimited = outLimited.pipe(concatMap(msg => {
        const now = scheduler.now();
        if (now < prev + gap) {
          prev = prev + gap;
          return of(msg).pipe(delay(prev - now, scheduler));
        } else {
          prev = now;
          return of(msg);
        }
      }));
    }
    outLimited.subscribe(msg => msg.publish());
  }

  publish(channel: Ably.Types.RealtimeChannelPromise,
    name: string, data: Buffer | object | null): Promise<void> {
    const channelMsg = new ChannelMessage(channel, name, data);
    this.outbound.next(channelMsg);
    return Promise.resolve(channelMsg.published);
  }

  subscribe(channel: Ably.Types.RealtimeChannelPromise,
    handler: (data: any, name: string, clientId: string) => void): Promise<void> {
    return channel.subscribe(message =>
      handler(message.data, message.name, message.clientId));
  }
}
