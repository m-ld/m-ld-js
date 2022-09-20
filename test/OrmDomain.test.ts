import { MockRemotes, testConfig } from './testClones';
import { OrmDomain } from '../src/orm/index';
import { clone, Construct, MeldClone, MeldStateMachine } from '../src/index';
import { MemoryLevel } from 'memory-level';
import { Episode, Series } from './ormFixtures';

class TheFlintstones extends OrmDomain {
  series1?: Series;

  initialise(meld: MeldStateMachine) {
    return new Promise<this>((resolve, reject) => {
      meld.read(
        state => this.updating(state, async orm => {
          const src = await state.get('series-1');
          if (src != null)
            this.series1 = new Series(src, orm);
        }).then(() => resolve(this), reject),
        (update, state) => this.updating(state, orm => orm.updated(
          update,
          deleted => {
            if (deleted === this.series1)
              delete this.series1;
          },
          src => {
            if (src['@id'] === 'series-1')
              this.series1 = new Series(src, orm);
          }))
      );
    });
  }
}

describe('Object-RDF Mapping Domain', () => {
  let api: MeldClone;

  beforeEach(async () => {
    api = await clone(new MemoryLevel, MockRemotes, testConfig());
  });

  afterEach(() => api.close());

  test('loads domain from state', async () => {
    await api.write({
      '@id': 'series-1',
      '@list': [{ '@id': 'tff', title: 'The Flintstone Flyer' }]
    });
    const show = await new TheFlintstones().initialise(api);
    expect(show.series1!.episodes[0].title).toBe('The Flintstone Flyer');
  });

  test('saves domain to state', async () => {
    const show = await new TheFlintstones().initialise(api);
    await api.write(async state => {
      await show.updating(state, async orm => {
        const tff = await orm.get('tff', src =>
          new Episode(src, orm, 'The Flintstone Flyer'));
        show.series1 = await orm.get('series-1', src =>
          new Series(src, orm, tff));
      });
      await state.write(show.commit());
    });
    await expect(api.read<Construct>({
      '@construct': {
        '@id': 'series-1',
        '@list': [{ '@id': '?', title: '?' }]
      }
    })).resolves.toEqual([{
      '@id': 'series-1',
      '@list': [{ '@id': 'tff', title: 'The Flintstone Flyer' }]
    }]);
  });
});