import LRUCache from 'lru-cache';

type FactoryOptions = Omit<LRUCache.Options<unknown, unknown>, 'length' | 'dispose'>;

export class CacheFactory {
  constructor(
    private readonly options: FactoryOptions
  ) {}

  createCache<K, V>(options: LRUCache.Options<K, V>) {
    if (this.options.max !== 0)
      return new LRUCache({ ...this.options, ...options });
  }
}