import { IteratingConsumable } from './IteratingConsumable';
import { Consumable } from '../index';

/**
 * Minimal readable interface required for ReadableConsumable
 * @internal
 */
export interface MinimalReadable<T> {
  readable: boolean;
  read(): T | null;

  destroyed: boolean;
  destroy(error?: Error): void;

  on(event: 'end', listener: () => void): MinimalReadable<T>;
  on(event: 'close', listener: () => void): MinimalReadable<T>;
  on(event: 'readable', listener: () => void): MinimalReadable<T>;
  on(event: 'error', listener: (err: Error) => void): MinimalReadable<T>;

  removeListener(event: 'end', listener: () => void): MinimalReadable<T>;
  removeListener(event: 'close', listener: () => void): MinimalReadable<T>;
  removeListener(event: 'readable', listener: () => void): MinimalReadable<T>;
  removeListener(event: 'error', listener: (err: Error) => void): MinimalReadable<T>;
}

export function consume<T>(readable: MinimalReadable<T>): Consumable<T> {
  return new ReadableConsumable(readable);
}

class ReadableConsumable<T> extends IteratingConsumable<T> {
  private reading = false;
  private ended = false;

  constructor(
    private readonly readable: MinimalReadable<T>) {
    super();
    // AsyncIterators with autoStart can emit 'end' before having a 'readable' listener
    readable
      .on('end', this.end)
      .on('error', this.error)
      .on('close', this.close);
  }

  private onReadable = () => {
    if (!this.reading)
      this.next(this.read());
  };

  private *read() {
    try {
      this.reading = true;
      let next: T | null;
      while ((next = this.readable.read()) != null)
        yield next;
    } finally {
      // Important: we get here when the last value has been consumed
      this.reading = false;
      if (this.ended)
        this.complete();
    }
  }

  private end = () => {
    // Misbehaving readable streams may emit 'end'
    // before the terminal `null` value has been read
    this.ended = true;
    if (!this.reading)
      this.complete();
  };

  private close = () => {
    if (!this.ended)
      this.error(new Error('Stream closed'));
  };

  protected start() {
    this.readable.on('readable', this.onReadable);
    // AsyncIterators do not auto-emit readable on adding the listener
    if (this.readable.readable)
      this.onReadable();
  }

  protected stop() {
    this.readable
      .removeListener('end', this.end)
      .removeListener('error', this.error)
      .removeListener('close', this.close)
      .removeListener('readable', this.onReadable);
    if (!this.readable.destroyed)
      this.readable.destroy();
  }
}