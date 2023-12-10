import { clone, MeldClone } from '../src/index';
import { MemoryLevel } from 'memory-level';
import { MockRemotes, testConfig } from './testClones';
import { watchQuery, watchSubject } from '../src/rx/index';
import { take, toArray } from 'rxjs/operators';
import { firstValueFrom } from 'rxjs';
import { setTimeout } from 'timers/promises';

describe('Rx utilities', () => {
  let api: MeldClone;

  beforeEach(async () => {
    api = await clone(new MemoryLevel, MockRemotes, testConfig());
  });

  afterEach(() => api.close().catch(() => {/*Some tests do close*/}));

  test('watch query emits if found sync', async () => {
    const observable = watchQuery(api, () => 1, () => 2);
    const found = firstValueFrom(observable.pipe(take(2), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([1, 2]);
  });

  test('watch query does not emit if not found on read sync', async () => {
    const observable = watchQuery(api, () => undefined, () => 2);
    const found = firstValueFrom(observable.pipe(take(1), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([2]);
  });

  test('watch query does not emit if not found on update sync', async () => {
    const observable = watchQuery(api, () => 1, () => undefined);
    const found = firstValueFrom(observable.pipe(take(1), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([1]);
  });

  test('watch query emits if found async on read', async () => {
    const observable = watchQuery(api, () => setTimeout(1, 1), () => 2);
    const found = firstValueFrom(observable.pipe(take(2), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([1, 2]);
  });

  test('watch query emits if found async on update', async () => {
    const observable = watchQuery(api,
      () => 1, () => setTimeout(1, 2));
    const found = firstValueFrom(observable.pipe(take(2), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([1, 2]);
  });

  test('watch query completes if clone closed', async () => {
    const observable = watchQuery(api, () => 1, () => 2);
    const found = firstValueFrom(observable.pipe(toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await api.close();
    await expect(found).resolves.toEqual([1, 2]);
  });

  test('watch can unsubscribe between read and update', async () => {
    const observable = watchQuery(api, () => 1, () => 2);
    const found: number[] = [];
    const subs = observable.subscribe(value => {
      found.push(value);
      subs.unsubscribe();
    });
    await api.write({ '@id': 'fred', name: 'Fred' });
    expect(found).toEqual([1]);
  });

  test('watch reads immediately', async () => {
    const observable = watchQuery(api, state => state.get('fred'));
    await api.write({ '@id': 'fred', name: 'Fred' });
    const found = firstValueFrom(observable.pipe(take(1), toArray()));
    await expect(found).resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
  });

  test('watch reads on update', async () => {
    const observable = watchQuery(api, state => state.get('fred'));
    const found = firstValueFrom(observable.pipe(take(1), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
  });

  test('watch subject initial state', async () => {
    const observable = watchSubject(api, 'fred');
    await api.write({ '@id': 'fred', name: 'Fred' });
    const found = firstValueFrom(observable.pipe(take(1), toArray()));
    await expect(found).resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
  });

  test('watch subject update', async () => {
    const observable = watchSubject(api, 'fred');
    const found = firstValueFrom(observable.pipe(take(1), toArray()));
    await api.write({ '@id': 'fred', name: 'Fred' });
    await expect(found).resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
  });

  test('watch subject update', async () => {
    const observable = watchSubject(api, 'fred');
    await api.write({ '@id': 'fred', name: 'Fred' });
    const found = firstValueFrom(observable.pipe(take(2), toArray()));
    await api.write({ '@update': { '@id': 'fred', name: 'Flintstone' } });
    await expect(found).resolves.toEqual([
      { '@id': 'fred', name: 'Fred' },
      { '@id': 'fred', name: 'Flintstone' }
    ]);
  });

  test('watched subject did not change', async () => {
    const observable = watchSubject(api, 'fred');
    await api.write({ '@id': 'fred', name: 'Fred' });
    const found = firstValueFrom(observable.pipe(take(2), toArray()));
    // A non-overlapping update should be ignored
    await api.write({ '@id': 'wilma', name: 'Wilma' } );
    await api.write({ '@update': { '@id': 'fred', name: 'Flintstone' } });
    await expect(found).resolves.toEqual([
      { '@id': 'fred', name: 'Fred' },
      { '@id': 'fred', name: 'Flintstone' }
    ]);
  });

  test('watch subject update with other data', async () => {
    const observable = watchSubject(api, 'fred');
    await api.write({ '@id': 'fred', name: 'Fred' });
    const found = firstValueFrom(observable.pipe(take(2), toArray()));
    // A non-overlapping update should be ignored
    await api.write({
      '@id': 'wilma',
      name: 'Wilma',
      spouse: { '@id': 'fred', name: 'Flintstone' }
    });
    await expect(found).resolves.toEqual([
      { '@id': 'fred', name: 'Fred' },
      { '@id': 'fred', name: ['Fred', 'Flintstone'] }
    ]);
  });
});