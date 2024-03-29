import {
  any, clone, Construct, Describe, Group, MeldClone, MeldStateSubscription, MeldUpdate, Reference,
  Select, Subject, Update
} from '../src';
import { MockRemotes, testConfig } from './testClones';
import { blankRegex, genIdRegex } from './testUtil';
import { DataFactory as RdfDataFactory, Quad } from 'rdf-data-factory';
import { Factory as SparqlFactory } from 'sparqlalgebrajs';
import { EmptyError, Subscription } from 'rxjs';
import { MemoryLevel } from 'memory-level';
import { Future } from '../src/engine/Future';
import { Binding } from '../src/rdfjs-support';

describe('MeldClone', () => {
  let api: MeldClone;
  let captureUpdate: Future<MeldUpdate>;

  beforeEach(async () => {
    api = await clone(new MemoryLevel, MockRemotes, testConfig());
    captureUpdate = new Future;
  });

  afterEach(() => api.close());

  test('retrieves a JSON-LD subject', async () => {
    api.follow(captureUpdate.resolve);
    await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
    await expect(captureUpdate).resolves.toEqual({
      '@ticks': 1,
      '@delete': [],
      '@insert': [{ '@id': 'fred', name: 'Fred' }],
      '@update': [],
      trace: expect.any(Function)
    });
    await expect(api.get('fred'))
      .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
  });

  describe('basic writes', () => {
    test('writes a subject with a type', async () => {
      await api.write<Subject>({ '@id': 'fred', '@type': 'Flintstone', name: 'Fred' });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', '@type': 'Flintstone', name: 'Fred' });
    });

    test('deletes a subject by update', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({ '@delete': { '@id': 'fred' } });
      await expect(api.get('fred')).resolves.toBeUndefined();
      await expect(captureUpdate).resolves.toEqual({
        '@ticks': 2,
        '@delete': [{ '@id': 'fred', name: 'Fred' }],
        '@insert': [],
        '@update': [],
        trace: expect.any(Function)
      });
    });

    test('deletes a property by update', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({ '@delete': { '@id': 'fred', height: 5 } });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
      await expect(captureUpdate).resolves.toEqual({
        '@ticks': 2,
        '@delete': [{ '@id': 'fred', height: 5 }],
        '@insert': [],
        '@update': [],
        trace: expect.any(Function)
      });
    });

    test('deletes where any', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
      await api.write<Update>({ '@delete': { '@id': 'fred', height: any() } });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    });

    test('updates a property', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': { '@id': 'fred', height: 5 },
        '@insert': { '@id': 'fred', height: 6 }
      });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 6 });
      await expect(captureUpdate).resolves.toEqual({
        '@ticks': 2,
        '@delete': [{ '@id': 'fred', height: 5 }],
        '@insert': [{ '@id': 'fred', height: 6 }],
        '@update': [],
        trace: expect.any(Function)
      });
    });

    test('updates a property by query with existing value', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': { '@id': 'fred', height: '?' },
        '@insert': { '@id': 'fred', height: 6 }
      });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 6 });
    });

    test('updates a property by query without existing value', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': { '@id': 'fred', height: '?' },
        '@insert': { '@id': 'fred', height: 6 }
      });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 6 });
    });

    test('does not update a property by query with unmatched where', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': { '@id': 'fred', height: '?height' },
        '@insert': { '@id': 'fred', height: 6 },
        '@where': { '@id': 'fred', height: '?height' }
      });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    });

    test('delete where must match all', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 5 });
      // This write has no effect because we're asking for triples with subject of
      // both fred and bambam
      await api.write<Update>({
        '@delete': [{ '@id': 'fred', height: 5 }, { '@id': 'bambam' }]
      });
      await expect(api.get('fred'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred', height: 5 });
    });

    test('inserts a subject by update', async () => {
      api.follow(captureUpdate.resolve);
      await api.write<Update>({ '@insert': { '@id': 'fred', name: 'Fred' } });
      await expect(api.get('fred')).resolves.toBeDefined();
      await expect(captureUpdate).resolves.toEqual({
        '@ticks': 1,
        '@delete': [],
        '@insert': [{ '@id': 'fred', name: 'Fred' }],
        '@update': [],
        trace: expect.any(Function)
      });
    });

    test('deletes a subject by path', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      api.follow(captureUpdate.resolve);
      await api.delete('fred');
      await expect(api.get('fred')).resolves.toBeUndefined();
      await expect(captureUpdate).resolves.toEqual({
        '@ticks': 2,
        '@delete': [{ '@id': 'fred', name: 'Fred' }],
        '@insert': [],
        '@update': [],
        trace: expect.any(Function)
      });
    });

    test('deletes a property by path', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', wife: { '@id': 'wilma' } });
      await api.delete('wilma');
      await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    });

    test('deletes an object by path', async () => {
      await api.write<Subject>({ '@id': 'fred', wife: { '@id': 'wilma' } });
      await api.delete('wilma');
      await expect(api.get('fred')).resolves.toBeUndefined();
    });

    test('inserts with a bound variable', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: 1 });
      await api.write({
        '@delete': { '@id': 'fred', likes: '?likes' },
        '@insert': { '@id': 'fred', likes: '?newLikes' },
        '@where': {
          '@graph': { '@id': 'fred', likes: '?likes' },
          '@bind': { '?newLikes': { '@plus': ['?likes', 1] } }
        }
      });
      await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', likes: 2 });
    });

    test('inserts with inline bound variable', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: 1 });
      await api.write({
        '@delete': { '@id': 'fred', likes: '?likes' },
        '@insert': { '@id': 'fred', likes: { '@value': '?likes', '@plus': 1 } }
      });
      await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', likes: 2 });
    });

    test('inserts where with inline bound variable', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: 1 });
      await api.write({
        '@insert': { '@id': 'fred', likes: { '@value': '?likes', '@plus': 1 } },
        '@where': { '@id': 'fred', likes: '?likes' }
      });
      await expect(api.get('fred')).resolves.toEqual({
        '@id': 'fred', likes: expect.arrayContaining([1, 2])
      });
    });

    test('deletes with inline filter', async () => {
      await api.write<Subject>({ '@id': 'fred', age: 41 });
      await api.write<Subject>({ '@id': 'wilma', age: 39 });
      await api.write({
        '@delete': { age: { '@gt': 39 } }
      });
      await expect(api.get('fred')).resolves.toBeUndefined();
      await expect(api.get('wilma')).resolves.toEqual({ '@id': 'wilma', age: 39 });
    });

    test('deletes where with inline filter', async () => {
      await api.write<Subject>({ '@id': 'fred', age: 41 });
      await api.write<Subject>({ '@id': 'wilma', age: 39 });
      await api.write<Subject>({ '@id': 'barney', age: 40 });
      await api.write({
        '@delete': { '@id': '?b', age: { '@value': '?age', '@gt': 39 } },
        '@where': { '@id': '?b', age: { '@value': '?age', '@lt': 41 } }
      });
      await expect(api.get('barney')).resolves.toBeUndefined();
      await expect(api.get('wilma')).resolves.toBeDefined();
      await expect(api.get('fred')).resolves.toBeDefined();
    });
  });

  describe('basic updates', () => {
    test('updates a property', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: 1 });
      await api.write({
        '@update': { '@id': 'fred', likes: 2 }
      });
      await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', likes: 2 });
    });

    test('updates with an operator', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: 1 });
      await api.write({
        '@update': { '@id': 'fred', likes: { '@plus': 1 } }
      });
      await expect(api.get('fred')).resolves.toEqual({ '@id': 'fred', likes: 2 });
    });

    test('updates multiple with an operator', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: [1, 2] });
      await api.write({
        '@update': { '@id': 'fred', likes: { '@plus': 1 } }
      });
      await expect(api.get('fred')).resolves.toEqual({
        '@id': 'fred', likes: expect.arrayContaining([2, 3])
      });
    });

    test('updates with filtered variable', async () => {
      await api.write<Subject>({ '@id': 'fred', likes: [1, 11] });
      await api.write({
        '@update': { '@id': 'fred', likes: { '@value': '?old', '@plus': 1 } },
        '@where': { '@id': 'fred', likes: { '@value': '?old', '@lt': 10 } }
      });
      await expect(api.get('fred')).resolves.toEqual({
        '@id': 'fred', likes: expect.arrayContaining([2, 11])
      });
    });
  });

  describe('rdf/js support', () => {
    const rdf = new RdfDataFactory();
    const sparql = new SparqlFactory(rdf);

    test('matches quad subject', done => {
      api.write<Subject>({ '@id': 'fred', name: 'Fred' }).then(() =>
        api.match(rdf.namedNode('http://test.m-ld.org/fred')).on('data', (quad: Quad) => {
          expect(quad.subject.equals(rdf.namedNode('http://test.m-ld.org/fred'))).toBe(true);
          expect(quad.predicate.equals(rdf.namedNode('http://test.m-ld.org/#name'))).toBe(true);
          expect(quad.object.equals(rdf.literal('Fred'))).toBe(true);
          done();
        }));
    });

    test('counts subject quads', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      // MemoryLevel does not provide approximate size measurements, so infinity is expected
      await expect(api.countQuads(rdf.namedNode('http://test.m-ld.org/fred')))
        .resolves.toBe(Infinity);
    });

    test('selects quad', done => {
      api.write<Subject>({ '@id': 'fred', name: 'Fred' }).then(() =>
        api.query(sparql.createProject(
          sparql.createBgp([sparql.createPattern(
            rdf.namedNode('http://test.m-ld.org/fred'),
            rdf.namedNode('http://test.m-ld.org/#name'),
            rdf.variable('name')
          )]),
          [rdf.variable('name')]
        )).on('data', (binding: Binding) => {
          expect(binding['?name'].equals(rdf.literal('Fred'))).toBe(true);
          done();
        }));
    });

    test('inserts quad', async () => {
      api.follow(captureUpdate.resolve);
      const quad = rdf.quad(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#name'),
        rdf.literal('Fred')
      );
      await expect(api.updateQuads({ insert: [quad] })).resolves.toBe(api);
      await expect(api.get('fred')).resolves.toEqual({
        '@id': 'fred', name: 'Fred'
      });
      const update = await captureUpdate;
      expect(update['@delete'].quads).toEqual([]);
      expect(update['@insert'].quads).toEqual([quad]);
    });

    test('deletes quad', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      api.follow(captureUpdate.resolve);
      const quad = rdf.quad(
        rdf.namedNode('http://test.m-ld.org/fred'),
        rdf.namedNode('http://test.m-ld.org/#name'),
        rdf.literal('Fred')
      );
      await expect(api.updateQuads({ delete: [quad] })).resolves.toBe(api);
      await expect(api.get('fred')).resolves.toBeUndefined();
      const update = await captureUpdate;
      expect(update['@delete'].quads).toEqual([quad]);
      expect(update['@insert'].quads).toEqual([]);
    });

    test('updates quad in procedure', async () => {
      await api.write(async state => {
        state = await state.write<Subject>({ '@id': 'fred', name: 'Fred' });
        await expect(state.updateQuads({
          delete: [rdf.quad(
            rdf.namedNode('http://test.m-ld.org/fred'),
            rdf.namedNode('http://test.m-ld.org/#name'),
            rdf.literal('Fred')
          )],
          insert: [rdf.quad(
            rdf.namedNode('http://test.m-ld.org/fred'),
            rdf.namedNode('http://test.m-ld.org/#name'),
            rdf.literal('Fred Flintstone')
          )]
        })).resolves.not.toBe(api);
      });
      await expect(api.get('fred')).resolves.toEqual({
        '@id': 'fred', name: 'Fred Flintstone'
      });
    });
  });

  describe('basic reads', () => {
    test('gets a property', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', age: 40 });
      await expect(api.get('fred', 'name'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    });

    test('gets two properties', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', age: 40 });
      await expect(api.get('fred', 'name', 'age'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred', age: 40 });
    });

    test('does not get a missing property', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await expect(api.get('fred', 'age'))
        .resolves.toBeUndefined();
    });

    test('gets if any available property', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await expect(api.get('fred', 'age', 'name'))
        .resolves.toEqual({ '@id': 'fred', name: 'Fred' });
    });

    test('asks exists', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', age: 40 });
      await expect(api.ask({})).resolves.toBe(true);
      await expect(api.ask({ '@where': { '@id': '?' } })).resolves.toBe(true);
      await expect(api.ask({ '@where': { '@id': 'fred' } })).resolves.toBe(true);
      await expect(api.ask({ '@where': { '@id': 'wilma' } })).resolves.toBe(false);
      await expect(api.ask({ '@where': { '@id': 'fred', age: 100 } })).resolves.toBe(false);
      await expect(api.ask({ '@where': { '@id': 'fred', name: '?' } })).resolves.toBe(true);
      await expect(api.ask({ '@where': { '@id': 'fred', height: '?' } })).resolves.toBe(false);
    });

    test('selects where', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Select>({
        '@select': '?f', '@where': { '@id': '?f', name: 'Fred' }
      })).resolves.toMatchObject([{ '?f': { '@id': 'fred' } }]);
    });

    test('selects where union', async () => {
      await api.write<Group>({
        '@graph': [
          { '@id': 'fred', name: 'Fred' },
          { '@id': 'wilma', name: 'Wilma' }
        ]
      });
      await expect(api.read<Select>({
        '@select': '?s', '@where': {
          '@union': [
            { '@id': '?s', name: 'Wilma' },
            { '@id': '?s', name: 'Fred' }
          ]
        }
      })).resolves.toEqual(expect.arrayContaining([
        expect.objectContaining({ '?s': { '@id': 'fred' } }),
        expect.objectContaining({ '?s': { '@id': 'wilma' } })
      ]));
    });

    test('selects not found', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await expect(api.read<Select>({
        '@select': '?w', '@where': { '@id': '?w', name: 'Wilma' }
      })).resolves.toEqual([]);
    });

    test('describes not found', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await expect(api.read<Describe>({
        '@describe': 'wilma'
      })).resolves.toEqual([]);
    });

    test('describes two subjects', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Describe>({
        '@describe': ['wilma', 'fred']
      })).resolves.toEqual(expect.arrayContaining([
        { '@id': 'fred', name: 'Fred' },
        { '@id': 'wilma', name: 'Wilma' }
      ]));
    });

    test('describes where', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Describe>({
        '@describe': '?f', '@where': { '@id': '?f', name: 'Fred' }
      })).resolves.toEqual([{ '@id': 'fred', name: 'Fred' }]);
    });

    test('describes with boolean value', async () => {
      await api.write<Subject>({ '@id': 'fred', married: true });
      await expect(api.read<Describe>({ '@describe': 'fred' }))
        .resolves.toEqual([{ '@id': 'fred', married: true }]);
    });

    test('describes with double value', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', age: 40.5 });
      await expect(api.read<Describe>({ '@describe': 'fred' }))
        .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred', age: 40.5 }]);
    });

    test('constructs basic match', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'fred', name: '?' }
      })).resolves.toMatchObject([{
        '@id': 'fred', name: 'Fred'
      }]);
    });

    test('constructs with missing property', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', age: undefined });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'fred', name: '?', age: '?' }
      })).resolves.toMatchObject([{
        '@id': 'fred', name: 'Fred'
      }]);
    });

    test('constructs where', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { name: '?name' },
        '@where': { '@id': 'fred', name: '?name' }
      })).resolves.toMatchObject([{
        '@id': expect.stringMatching(blankRegex),
        name: 'Fred'
      }]);
    });

    test('constructs where two matches', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { name: '?name' },
        '@where': { name: '?name' }
      })).resolves.toMatchObject([{
        '@id': expect.stringMatching(blankRegex),
        name: expect.arrayContaining(['Fred', 'Wilma'])
      }]);
    });

    test('constructs new ID from existing', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'fake', name: '?name' },
        '@where': { '@id': 'fred', name: '?name' }
      })).resolves.toMatchObject([{
        '@id': 'fake', name: 'Fred'
      }]);
    });

    test('constructs two matches', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { name: '?' }
      })).resolves.toMatchObject([{
        '@id': expect.stringMatching(blankRegex),
        name: expect.arrayContaining(['Fred', 'Wilma'])
      }]);
    });

    test('constructs two identified matches', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': '?', name: '?' }
      })).resolves.toMatchObject(expect.arrayContaining([
        { '@id': 'fred', name: 'Fred' },
        { '@id': 'wilma', name: 'Wilma' }
      ]));
    });

    test('constructs with two matched properties', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', height: 6 });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'fred', '?': '?' }
      })).resolves.toMatchObject([{
        '@id': 'fred', name: 'Fred', height: 6
      }]);
    });

    test('constructs with nested subject', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', wife: { '@id': 'wilma' } });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'fred', wife: { name: '?' } }
      })).resolves.toMatchObject([{
        '@id': 'fred', wife: { name: 'Wilma' }
      }]);
    });

    test('constructs with nested optional no-match subject', async () => {
      await api.write<Subject>({ '@id': 'barney', name: 'Barney', wife: { '@id': 'betty' } });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'barney', name: '?name', wife: { name: '?wife' } },
        '@where': {
          '@union': [
            { '@id': 'barney', name: '?name' },
            { '@id': 'barney', wife: { name: '?wife' } }
          ]
        }
      })).resolves.toEqual([{
        '@id': 'barney', name: 'Barney'
      }]);
    });

    test('constructs with overlapping bindings', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred', wife: { '@id': 'wilma' } });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'fred', '?': '?', wife: { '@id': '?', name: '?' } }
      })).resolves.toMatchObject([{
        '@id': 'fred', name: 'Fred',
        // Comes from both '?' and wife property matches
        wife: { '@id': 'wilma', name: 'Wilma' }
      }]);
    });

    test('constructs list', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Bread', 'Milk'] });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'shopping', '@list': { '?': '?' } }
      })).resolves.toMatchObject([{
        '@id': 'shopping', '@list': { '0': 'Bread', '1': 'Milk' }
      }]);
    });

    test('constructs item from list', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Bread', 'Milk'] });
      await expect(api.read<Construct>({
        '@construct': { '@id': 'shopping', '@list': { 1: '?' } }
      })).resolves.toMatchObject([{
        '@id': 'shopping', '@list': { '1': 'Milk' }
      }]);
    });
  });

  describe('filters', () => {
    test('selects name equal', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      const selection = await api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f', name: '?n' },
          '@filter': { '@eq': ['?n', 'Fred'] }
        }
      });
      expect(selection).toEqual([{
        '@id': expect.stringMatching(blankRegex),
        '?f': { '@id': 'fred' }
      }]);
    });

    test('selects name in', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await api.write<Subject>({ '@id': 'barney', name: 'Barney' });
      const selection = await api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f', name: '?n' },
          '@filter': { '@in': ['?n', 'Fred', 'Wilma'] }
        }
      });
      expect(selection.length).toBe(2);
      expect(selection).toEqual(expect.arrayContaining([
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'fred' } },
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'wilma' } }
      ]));
    });

    test('selects @id in', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await api.write<Subject>({ '@id': 'barney', name: 'Barney' });
      const selection = await api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f' },
          '@filter': { '@in': ['?f', { '@id': 'fred' }, { '@id': 'wilma' }] }
        }
      });
      expect(selection.length).toBe(2);
      expect(selection).toEqual(expect.arrayContaining([
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'fred' } },
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'wilma' } }
      ]));
    });

    test('selects @id from values', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await api.write<Subject>({ '@id': 'barney', name: 'Barney' });
      const selection = await api.read<Select>({
        '@select': '?n',
        '@where': {
          '@graph': { '@id': '?f', name: '?n' },
          '@values': [{ '?f': { '@id': 'fred' } }, { '?f': { '@id': 'wilma' } }]
        }
      });
      expect(selection.length).toBe(2);
      expect(selection).toEqual(expect.arrayContaining([
        { '@id': expect.stringMatching(blankRegex), '?n': 'Fred' },
        { '@id': expect.stringMatching(blankRegex), '?n': 'Wilma' }
      ]));
    });

    test('selects property from vocab values', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', age: 40 });
      await api.write<Subject>({ '@id': 'barney', height: 5 });
      const selection = await api.read<Select>({
        '@select': '?v',
        '@where': {
          '@graph': { '@id': '?f', '?p': '?v' },
          '@values': [
            // Use of @vocab should resolve against the vocab, and so match a property
            { '?p': { '@vocab': 'name' } },
            // Use of @id should resolve against the base, and so not match a property
            { '?p': { '@id': 'age' } }
          ]
        }
      });
      expect(selection).toEqual([
        { '@id': expect.stringMatching(blankRegex), '?v': 'Fred' }
      ]);
    });

    test('selects name or other name', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await api.write<Subject>({ '@id': 'barney', name: 'Barney' });
      const selection = await api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f', name: '?n' },
          '@filter': { '@or': [{ '@eq': ['?n', 'Fred'] }, { '@eq': ['?n', 'Wilma'] }] }
        }
      });
      expect(selection.length).toBe(2);
      expect(selection).toEqual(expect.arrayContaining([
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'fred' } },
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'wilma' } }
      ]));
    });

    test('selects string gte', async () => {
      await api.write<Subject>({ '@id': 'fred', name: 'Fred' });
      await api.write<Subject>({ '@id': 'wilma', name: 'Wilma' });
      await api.write<Subject>({ '@id': 'barney', name: 'Barney' });
      const selection = await api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f', name: '?n' },
          '@filter': { '@gte': ['?n', 'Fred'] }
        }
      });
      expect(selection.length).toBe(2);
      expect(selection).toEqual(expect.arrayContaining([
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'fred' } },
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'wilma' } }
      ]));
    });

    test('selects number gte', async () => {
      await api.write<Subject>({ '@id': 'fred', height: 6 });
      await api.write<Subject>({ '@id': 'wilma', height: 5 });
      await api.write<Subject>({ '@id': 'barney', height: 4 });
      const selection = await api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f', height: '?h' },
          '@filter': { '@gte': ['?h', 5] }
        }
      });
      expect(selection.length).toBe(2);
      expect(selection).toEqual(expect.arrayContaining([
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'fred' } },
        { '@id': expect.stringMatching(blankRegex), '?f': { '@id': 'wilma' } }
      ]));
    });

    test('selects where inline filtered', async () => {
      await api.write<Subject>({ '@id': 'fred', age: 42 });
      await api.write<Subject>({ '@id': 'wilma', age: 39 });
      await expect(api.read<Select>({
        '@select': '?f', '@where': { '@id': '?f', age: { '@gt': 40 } }
      })).resolves.toMatchObject([{ '?f': { '@id': 'fred' } }]);
    });

    test('constructs where inline filtered', async () => {
      await api.write<Subject>({ '@id': 'fred', age: 42 });
      await api.write<Subject>({ '@id': 'wilma', age: 39 });
      await expect(api.read<Construct>({
        '@construct': { '@id': '?', age: { '@gt': 40 } }
      })).resolves.toMatchObject([{ '@id': 'fred', age: 42 }]);
    });

    test('selects where inline and explicit filtered', async () => {
      await api.write<Subject>({ '@id': 'fred', age: 42, height: 6 });
      await api.write<Subject>({ '@id': 'barney', age: 41, height: 5 });
      await api.write<Subject>({ '@id': 'wilma', age: 39, height: 5 });
      await expect(api.read<Select>({
        '@select': '?f',
        '@where': {
          '@graph': { '@id': '?f', age: { '@gt': 40 }, height: '?h' },
          '@filter': { '@gt': ['?h', 5] }
        }
      })).resolves.toMatchObject([{ '?f': { '@id': 'fred' } }]);
    });
  });

  describe('anonymous subjects', () => {
    test('describes an anonymous subject', async () => {
      await api.write<Update>({ '@insert': { name: 'Fred', height: 5 } });
      await expect(api.read<Describe>({
        '@describe': '?s', '@where': { '@id': '?s', name: 'Fred' }
      })).resolves.toEqual([{ '@id': expect.stringMatching(genIdRegex), name: 'Fred', height: 5 }]);
    });

    test('selects anonymous subject properties', async () => {
      await api.write<Update>({ '@insert': { name: 'Fred', height: 5 } });
      await expect(api.read<Select>({
        '@select': '?h', '@where': { name: 'Fred', height: '?h' }
      })).resolves.toMatchObject([{ '?h': 5 }]);
    });

    test('describes an anonymous nested subject', async () => {
      await api.write<Subject>({ '@id': 'fred', stats: { height: 5, age: 40 } });
      await expect(api.read<Describe>({
        '@describe': '?stats', '@where': { '@id': 'fred', stats: { '@id': '?stats' } }
      })).resolves.toEqual([{ '@id': expect.stringMatching(genIdRegex), height: 5, age: 40 }]);
    });

    test('does not merge anonymous nested subjects', async () => {
      await api.write<Subject>({ '@id': 'fred', stats: { height: 5 } });
      await api.write<Subject>({ '@id': 'fred', stats: { age: 40 } });
      await expect(api.read<Describe>({
        '@describe': '?stats', '@where': { '@id': 'fred', stats: { '@id': '?stats' } }
      })).resolves.toMatchObject(expect.arrayContaining([
        { '@id': expect.stringMatching(genIdRegex), height: 5 },
        { '@id': expect.stringMatching(genIdRegex), age: 40 }
      ]));
    });

    test('matches anonymous subject property in delete-where', async () => {
      await api.write<Subject>({ '@id': 'fred', height: 5, age: 40 });
      await api.write<Update>({ '@delete': { height: 5 } });
      await expect(api.read<Describe>({
        '@describe': 'fred'
      })).resolves.toEqual([{ '@id': 'fred', age: 40 }]);
    });

    test('matches nested property in delete-where', async () => {
      await api.write<Subject>({ '@id': 'fred', stats: { height: 5, age: 40 } });
      await api.write<Update>({ '@delete': { '@id': 'fred', stats: { height: 5 } } });
      // Scary case for documentation: DELETEWHERE is aggressive
      await expect(api.read<Describe>({
        '@describe': '?stats', '@where': { '@id': 'fred', stats: { '@id': '?stats' } }
      })).resolves.toEqual([]);
    });

    test('matches nested property with explicit where', async () => {
      await api.write<Subject>({ '@id': 'fred', stats: { height: 5, age: 40 } });
      await api.write<Update>({
        '@delete': { '@id': '?stats', height: 5 },
        '@where': { '@id': 'fred', stats: { '@id': '?stats', height: 5 } }
      });
      await expect(api.read<Describe>({
        '@describe': '?stats', '@where': { '@id': 'fred', stats: { '@id': '?stats' } }
      })).resolves.toEqual([{ '@id': expect.stringMatching(genIdRegex), age: 40 }]);
    });

    test('imports web-app configuration', async () => {
      await api.write<Subject>(require('./web-app.json'));
      // Do some checks to ensure the data is present
      await expect(api.read<Select>({
        '@select': '?id',
        '@where': { '@id': '?id' }
      })).resolves.toMatchObject(
        // Note 85 quads total (not distinct)
        new Array(85).fill({ '?id': { '@id': expect.stringMatching(genIdRegex) } }));
      await expect(api.read<Describe>({
        '@describe': '?id',
        '@where': { '@id': '?id' }
      })).resolves.toMatchObject(
        new Array(12).fill({ '@id': expect.stringMatching(genIdRegex) }));
      await expect(api.read<Describe>({
        '@describe': '?id',
        '@where': { '@id': '?id', 'servlet-name': 'fileServlet' }
      })).resolves.toEqual([{
        '@id': expect.stringMatching(genIdRegex),
        'servlet-class': 'org.cofax.cds.FileServlet',
        'servlet-name': 'fileServlet'
      }]);
      await expect(api.read<Select>({
        '@select': '?a',
        '@where': {
          'servlet': {
            'init-param': {
              'dataStoreName': 'cofax',
              'configGlossary:adminEmail': '?a'
            }
          }
        }
      })).resolves.toMatchObject([{ '?a': 'ksm@pobox.com' }]);
    });
  });

  describe('lists', () => {
    test('inserts a list as a property', async () => {
      api.follow(captureUpdate.resolve);
      await api.write<Subject>({
        '@id': 'fred', shopping: { '@list': ['Bread', 'Milk'] }
      });
      expect([...(await captureUpdate)['@insert'].graph.values()]).toContainEqual({
        '@id': 'fred',
        shopping: {
          '@id': expect.stringMatching(genIdRegex),
          '@type': 'http://m-ld.org/RdfLseq',
          '@list': {
            0: [
              { '@id': expect.stringMatching(genIdRegex), '@item': 'Bread', '@index': 0 },
              { '@id': expect.stringMatching(genIdRegex), '@item': 'Milk', '@index': 1 }
            ]
          }
        }
      });
      const fred = (await api.read<Describe>({ '@describe': 'fred' }))[0];
      expect(fred).toMatchObject({
        '@id': 'fred', shopping: { '@id': expect.stringMatching(genIdRegex) }
      });
      const listId = (<any>fred.shopping)['@id'];
      await expect(api.read<Describe>({ '@describe': listId })).resolves.toMatchObject([{
        '@id': listId,
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Milk']
      }]);
    });

    test('inserts an identified list', async () => {
      await api.write<Subject>({
        '@id': 'shopping', '@list': ['Bread', 'Milk']
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' })).resolves.toMatchObject([{
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Milk']
      }]);
    });

    test('inserts an identified with index notation', async () => {
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 0: 'Bread', 1: 'Milk' }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' })).resolves.toMatchObject([{
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Milk']
      }]);
    });

    test('appends to a list', async () => {
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 0: 'Bread' }
      });
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 1: 'Milk' }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' })).resolves.toMatchObject([{
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Milk']
      }]);
    });

    test('appends beyond the end of a list', async () => {
      await api.write<Subject>({
        '@id': 'shopping', '@list': ['Bread']
      });
      api.follow(captureUpdate.resolve);
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 2: 'Milk' }
      });
      expect([...(await captureUpdate)['@insert'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          1: [{ '@id': expect.stringMatching(genIdRegex), '@item': 'Milk', '@index': 1 }]
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' })).resolves.toMatchObject([{
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Milk']
      }]);
    });

    test('prepends to a list', async () => {
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 0: 'Milk' }
      });
      api.follow(captureUpdate.resolve);
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 0: 'Bread' }
      });
      expect([...(await captureUpdate)['@insert'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          0: [{ '@id': expect.stringMatching(genIdRegex), '@item': 'Bread', '@index': 0 }]
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' })).resolves.toMatchObject([{
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Milk']
      }]);
      // This checks that the slots have been correctly re-numbered
      await expect(api.read<Select>({
        '@select': ['?0', '?1'],
        '@where': { '@id': 'shopping', '@list': { 0: '?0', 1: '?1' } }
      })).resolves.toMatchObject([{
        '?0': 'Bread',
        '?1': 'Milk'
      }]);
    });

    test('prepends multiple items to a list', async () => {
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 0: 'Milk' }
      });
      api.follow(captureUpdate.resolve);
      await api.write<Subject>({
        '@id': 'shopping', '@list': { 0: ['Bread', 'Candles'] }
      });
      expect([...(await captureUpdate)['@insert'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          0: [
            { '@id': expect.stringMatching(genIdRegex), '@item': 'Bread', '@index': 0 },
            { '@id': expect.stringMatching(genIdRegex), '@item': 'Candles', '@index': 1 }
          ]
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' })).resolves.toMatchObject([{
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': ['Bread', 'Candles', 'Milk']
      }]);
    });

    test('finds by index in a list', async () => {
      await api.write<Group>({
        '@graph': [
          { '@id': 'shopping1', '@list': ['Bread', 'Milk'] },
          { '@id': 'shopping2', '@list': ['Milk', 'Spam'] }
        ]
      });
      await expect(api.read<Select>({
        '@select': '?list',
        '@where': {
          '@id': '?list',
          '@list': { 1: 'Milk' }
        }
      })).resolves.toMatchObject([{
        '?list': { '@id': 'shopping1' }
      }]);
    });

    test('selects item from a list', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk'] });
      await expect(api.read<Select>({
        '@select': '?item',
        '@where': { '@id': 'shopping', '@list': { '?': '?item' } }
      })).resolves.toMatchObject([{ '?item': 'Milk' }]);
    });

    test('selects index from a list', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk'] });
      await expect(api.read<Select>({
        '@select': '?index',
        '@where': { '@id': 'shopping', '@list': { '?index': '?' } }
      })).resolves.toMatchObject([{ '?index': 0 }]);
    });

    test('selects slot from a list', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk'] });
      await expect(api.read<Select>({
        '@select': '?slot',
        '@where': { '@id': 'shopping', '@list': { '?': { '@id': '?slot', '@item': '?' } } }
      })).resolves.toMatchObject([{
        '?slot': { '@id': expect.stringMatching(genIdRegex) }
      }]);
    });

    test('deletes a list with all slots', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk'] });
      // recover the slot ID
      const slotId: string = (<Reference>(await api.read<Select>({
        '@select': '?slot',
        '@where': { '@id': 'shopping', '@list': { '?': { '@id': '?slot', '@item': '?' } } }
      }))[0]['?slot'])['@id'];

      await api.write<Update>({ '@delete': { '@id': 'shopping' } });
      await expect(api.read<Describe>({ '@describe': 'shopping' }))
        .resolves.toEqual([]);
      await expect(api.read<Describe>({ '@describe': slotId }))
        .resolves.toEqual([]);
    });

    test('deletes a slot by index', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk'] });
      // recover the slot ID
      const slotId: string = (<Reference>(await api.read<Select>({
        '@select': '?slot',
        '@where': { '@id': 'shopping', '@list': { '?': { '@id': '?slot', '@item': '?' } } }
      }))[0]['?slot'])['@id'];

      api.follow(captureUpdate.resolve);
      await api.write<Update>({ '@delete': { '@id': 'shopping', '@list': { 0: '?' } } });

      expect([...(await captureUpdate)['@delete'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          0: { '@id': expect.stringMatching(genIdRegex), '@item': 'Milk', '@index': 0 }
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' }))
        .resolves.toEqual([{
          '@id': 'shopping',
          '@type': 'http://m-ld.org/RdfLseq'
        }]);
      await expect(api.read<Describe>({ '@describe': slotId }))
        .resolves.toEqual([]);
    });

    test('move a slot by index', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk', 'Bread', 'Spam'] });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': {
          '@id': 'shopping', '@list': { 1: { '@id': '?slot', '@item': '?item' } }
        },
        '@insert': {
          '@id': 'shopping', '@list': { 0: { '@id': '?slot', '@item': '?item' } }
        }
      });
      const update = await captureUpdate;
      expect([...update['@delete'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          // @item is not included in delete for a move
          1: { '@id': expect.stringMatching(genIdRegex), '@index': 1 }
        }
      });
      expect([...update['@insert'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          0: [{ '@id': expect.stringMatching(genIdRegex), '@index': 0 }]
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' }))
        .resolves.toEqual([{
          '@id': 'shopping',
          '@type': 'http://m-ld.org/RdfLseq',
          '@list': ['Bread', 'Milk', 'Spam']
        }]);
    });

    test('move a slot by value', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk', 'Bread', 'Spam'] });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': {
          '@id': 'shopping', '@list': { '?i': { '@id': '?slot', '@item': 'Spam' } }
        },
        '@insert': {
          '@id': 'shopping', '@list': { 0: { '@id': '?slot', '@item': 'Spam' } }
        }
      });
      expect([...(await captureUpdate)['@delete'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          // @item is not included in delete for a move
          2: { '@id': expect.stringMatching(genIdRegex), '@index': 2 }
        }
      });
      expect([...(await captureUpdate)['@insert'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@list': {
          0: [{ '@id': expect.stringMatching(genIdRegex), '@index': 0 }]
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' }))
        .resolves.toEqual([{
          '@id': 'shopping',
          '@type': 'http://m-ld.org/RdfLseq',
          '@list': ['Spam', 'Milk', 'Bread']
        }]);
      // This checks that the slots have been correctly re-numbered
      await expect(api.read<Select>({
        '@select': ['?0', '?1', '?2'],
        '@where': { '@id': 'shopping', '@list': { 0: '?0', 1: '?1', 2: '?2' } }
      })).resolves.toMatchObject([{
        '?0': 'Spam',
        '?1': 'Milk',
        '?2': 'Bread'
      }]);
    });

    test('change an item', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk', 'Bread'] });
      api.follow(captureUpdate.resolve);
      await api.write<Update>({
        '@delete': { '@id': 'shopping', '@list': { '?1': 'Bread' } },
        '@insert': { '@id': 'shopping', '@list': { '?1': 'Spam' } }
      });
      const update = await captureUpdate;
      expect([...update['@delete'].graph.values()]).toContainEqual({
        '@id': expect.stringMatching(genIdRegex), '@item': 'Bread'
      });
      expect([...update['@insert'].graph.values()]).toContainEqual({
        '@id': expect.stringMatching(genIdRegex), '@item': 'Spam'
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' }))
        .resolves.toEqual([{
          '@id': 'shopping',
          '@type': 'http://m-ld.org/RdfLseq',
          '@list': ['Milk', 'Spam']
        }]);
    });

    test('nested lists are created', async () => {
      api.follow(captureUpdate.resolve);
      await api.write<Subject>({
        '@id': 'shopping', '@list': ['Milk', ['Bread', 'Spam']]
      });
      expect([...(await captureUpdate)['@insert'].graph.values()]).toContainEqual({
        '@id': 'shopping',
        '@type': 'http://m-ld.org/RdfLseq',
        '@list': {
          0: [{
            '@id': expect.stringMatching(genIdRegex), '@item': 'Milk', '@index': 0
          }, {
            '@id': expect.stringMatching(genIdRegex),
            '@item': {
              '@id': expect.stringMatching(genIdRegex),
              '@type': 'http://m-ld.org/RdfLseq',
              '@list': {
                0: [
                  { '@id': expect.stringMatching(genIdRegex), '@item': 'Bread', '@index': 0 },
                  { '@id': expect.stringMatching(genIdRegex), '@item': 'Spam', '@index': 1 }
                ]
              }
            },
            '@index': 1
          }]
        }
      });
      await expect(api.read<Describe>({ '@describe': 'shopping' }))
        .resolves.toMatchObject([{
          '@list': ['Milk', { '@id': expect.stringMatching(genIdRegex) }]
        }]);
    });

    test('cannot force an item to be multi-valued', async () => {
      await api.write<Subject>({ '@id': 'shopping', '@list': ['Milk', 'Bread'] });
      await expect(api.write<Update>({
        // Do not @delete the old value
        '@insert': { '@id': 'shopping', '@list': { '?1': 'Spam' } },
        '@where': { '@id': 'shopping', '@list': { '?1': 'Bread' } }
      })).rejects.toBeDefined();
    });
  });

  describe('state procedures', () => {
    test('reads with a procedure', done => {
      api.write<Subject>({ '@id': 'fred', name: 'Fred' }).then(() => {
        api.read(async state => {
          await expect(state.read<Describe>({ '@describe': 'fred' }))
            .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred' }]);
          done();
        });
      });
    });

    test('writes with a procedure', done => {
      api.write(async state => {
        state = await state.write<Subject>({ '@id': 'fred', name: 'Fred' });
        await expect(state.read<Describe>({ '@describe': 'fred' }))
          .resolves.toMatchObject([{ '@id': 'fred', name: 'Fred' }]);
        done();
      });
    });

    test('write state is predictable', () => Promise.all([
      api.write(async state => {
        state = await state.write<Subject>({ '@id': 'fred', age: 40 });
        await new Promise(resolve => setTimeout(resolve, 5));
        await expect(state.read<Describe>({
          '@describe': '?id', '@where': { '@id': '?id', age: 40 }
        }))
          // We only expect one person of that age
          .resolves.toMatchObject([{ '@id': 'fred', age: 40 }]);
      }),
      // Immediately make another write which could affect the query
      api.write(state => state.write<Subject>({ '@id': 'wilma', age: 40 }))
    ]));

    test('handler state follows writes', done => {
      let hadFred = false;
      api.read(async state => {
        await expect(state.read<Describe>({
          '@describe': '?id', '@where': { '@id': '?id', age: 40 }
        })).resolves.toEqual([]);
      }, async (_, state) => {
        if (!hadFred) {
          await expect(state.read<Describe>({
            '@describe': '?id', '@where': { '@id': '?id', age: 40 }
          })).resolves.toMatchObject([{ '@id': 'fred', age: 40 }]);
          hadFred = true;
        } else {
          await expect(state.read<Describe>({
            '@describe': '?id', '@where': { '@id': '?id', age: 40 }
          })).resolves.toMatchObject([{ '@id': 'wilma', age: 40 }]);
          done();
        }
      });
      api.write(async state => {
        state = await state.write<Subject>({ '@id': 'fred', age: 40 });
        await state.write<Subject>({
          '@delete': { '@id': 'fred', age: 40 },
          '@insert': { '@id': 'wilma', age: 40 }
        });
      });
    });

    test('consumes from consistent snapshot', done => {
      const writeDone = new Future;
      const subjectsSeen = new Set<string>();
      api.write(async state => {
        state = await state.write<Group>({
          '@graph': [
            { '@id': 'fred', '@type': 'Flintstone', name: 'Fred' },
            { '@id': 'wilma', '@type': 'Flintstone', name: 'Wilma' }
          ]
        });
        // Don't await this state read, but consume it asynchronously
        state.read<Describe>({
          '@describe': '?f',
          '@where': { '@id': '?f', '@type': 'Flintstone' }
        }).consume.subscribe({
          async next({ value, next }) {
            subjectsSeen.add(value['@id']);
            expect(value['@type']).toBe('Flintstone');
            // Push the next until long after the `write` below has been
            // requested and would have been processed if it were not waiting
            await new Promise(resolve => setTimeout(resolve, 5));
            next();
          },
          complete() {
            expect(subjectsSeen.size).toBe(2);
            expect(writeDone.pending).toBe(true);
            writeDone.then(done);
          },
          error: done
        });
      });
      // This checks that the test is working – we have not yet streamed
      expect(subjectsSeen.size).toBeLessThan(2);
      // Immediately write an update to change the flintstones, this should
      // block until the query results have been consumed
      api.write<Update>({
        '@delete': { '@id': '?f', '@type': 'Flintstone' },
        '@insert': { '@id': '?f', '@type': 'Jetson' }
      }).then(() => writeDone.resolve());
    });

    test('abandoned read does not block write', done => {
      api.write(async state => {
        state = await state.write<Group>({
          '@graph': [
            { '@id': 'fred', '@type': 'Flintstone', name: 'Fred' },
            { '@id': 'wilma', '@type': 'Flintstone', name: 'Wilma' }
          ]
        });
        // Don't await this state read, but consume it asynchronously
        const subs: Subscription = state.read<Describe>({
          '@describe': '?f',
          '@where': { '@id': '?f', '@type': 'Flintstone' }
        }).consume.subscribe(() =>
          // Push the abandonment out an unreasonable time
          setTimeout(() => subs.unsubscribe(), 5));
      });
      api.write<Update>({
        '@delete': { '@id': '?f', '@type': 'Flintstone' },
        '@insert': { '@id': '?f', '@type': 'Jetson' }
      }).then(() => done());
    });

    test('can async iterate to follow states', () => Promise.all([
      (async function () {
        // noinspection LoopStatementThatDoesntLoopJS
        for await (let [update] of api.follow()) {
          expect(update['@insert']).toEqual([{ '@id': 'fred', name: 'Fred' }]);
          return;
        }
      })(),
      api.write({ '@id': 'fred', name: 'Fred' })
    ]));

    test('states are immutable during async iterate', () => Promise.all([
      (async function () {
        for await (let [update, state] of api.follow()) {
          // First update should be Fred
          const { '@insert': [{ '@id': id }] } = update;
          if (id === 'wilma')
            return;
          expect(id).toBe('fred');
          // This separate write should have no effect on the state until next
          api.write({ '@id': 'wilma', name: 'Wilma' }).then();
          await new Promise(resolve => setTimeout(resolve, 5));
          await expect(state.ask({ '@where': { '@id': 'wilma' } })).resolves.toBe(false);
        }
      })(),
      api.write({ '@id': 'fred', name: 'Fred' })
    ]));

    test('immutable state is released by async break', () => Promise.all([
      new Promise(async resolve => {
        // noinspection LoopStatementThatDoesntLoopJS
        for await (let [_update, state] of api.follow()) {
          // This separate write should have no effect on the state until break
          api.write({ '@id': 'wilma', name: 'Wilma' }).then(resolve);
          await new Promise(resolve => setTimeout(resolve, 5));
          await expect(state.ask({ '@where': { '@id': 'wilma' } })).resolves.toBe(false);
          break;
        }
      }),
      api.write({ '@id': 'fred', name: 'Fred' })
    ]));

    test('unsubscribe ends follow', () => Promise.all([
      new Promise(async (resolve, reject) => {
        const subs = api.follow();
        for await (let [update, _state] of subs) {
          const { '@insert': [{ '@id': id }] } = update;
          if (id === 'wilma')
            return reject('Wilma should not be notified!');
          // This separate write should not appear in the follow...
          api.write({ '@id': 'wilma', name: 'Wilma' }).then(resolve);
          subs.unsubscribe(); // ... because we cancel the subscription
        }
      }),
      api.write({ '@id': 'fred', name: 'Fred' })
    ]));

    test('unsubscribe after read cancels follow', () => Promise.all([
      (async function () {
        const subs = api.read(async () => {
          await new Promise(resolve => setTimeout(resolve, 5));
          subs.unsubscribe();
          return 5;
        });
        // noinspection LoopStatementThatDoesntLoopJS
        for await (let _anything of subs)
          throw 'Follow should have cancelled!';
      })(),
      api.write({ '@id': 'fred', name: 'Fred' })
    ]));

    test('can async iterate immediately after read', () => Promise.all([
      // This write should appear in the read but not the follow
      api.write({ '@id': 'wilma', name: 'Wilma' }),
      new Promise<void>(async resolve => {
        const subs = api.read(async state => {
          // This cheeky write should appear in the follow
          api.write({ '@id': 'fred', name: 'Fred' }).then();
          await expect(state.ask({ '@where': { '@id': 'wilma' } })).resolves.toBe(true);
          await new Promise(resolve => setTimeout(resolve, 5));
        });
        // noinspection LoopStatementThatDoesntLoopJS
        for await (let [update] of subs) {
          expect(update['@insert']).toEqual([{ '@id': 'fred', name: 'Fred' }]);
          return resolve();
        }
      })
    ]));

    test('can await read result and then follow', () => Promise.all([
      // This write should appear in the read but not the follow
      api.write({ '@id': 'wilma', name: 'Wilma' }),
      new Promise<void>(async resolve => {
        const subs = api.read(async state => {
          // This cheeky write should appear in the follow
          api.write({ '@id': 'fred', name: 'Fred' }).then();
          await expect(state.ask({ '@where': { '@id': 'wilma' } })).resolves.toBe(true);
          await new Promise(resolve => setTimeout(resolve, 5));
          return 5;
        });
        expect(await subs).toBe(5);
        // noinspection LoopStatementThatDoesntLoopJS
        for await (let [update] of subs) {
          expect(update['@insert']).toEqual([{ '@id': 'fred', name: 'Fred' }]);
          return resolve();
        }
      })
    ]));

    test('can cancel read result with unsubscribe', async () => {
      const subs = api.read(() =>
        new Promise<number>(resolve => setTimeout(resolve, 5, 5)));
      subs.unsubscribe();
      await expect(subs).rejects.toBe(EmptyError);
    });

    test('cannot await read result for a pure follow', async () => {
      const subs = api.follow();
      await expect(subs).rejects.toBe(EmptyError);
    });

    test('delayed async iterate after read misses updates', async () => {
      // Wait for the cheeky write in the read to complete
      // (wrapped in an object to prevent resolve from settling the promise)
      const { subs } = await new Promise<{ subs: MeldStateSubscription<void> }>(resolve => {
        const subs = api.read(async state => {
          api.write({ '@id': 'fred', name: 'Fred' }).then(() => resolve({ subs }));
          await expect(state.ask({ '@where': { '@id': 'fred' } })).resolves.toBe(false);
        });
      });
      api.write({ '@id': 'wilma', name: 'Wilma' }).then();
      // noinspection LoopStatementThatDoesntLoopJS
      for await (let [update] of subs) {
        expect(update['@insert']).toEqual([{ '@id': 'wilma', name: 'Wilma' }]);
        return;
      }
    });

    test('can catch read procedure exception and still follow', () => Promise.all([
      (async function () {
        const subs = api.read(() => Promise.reject('Bang'));
        await expect(subs).rejects.toBe('Bang');
        // noinspection LoopStatementThatDoesntLoopJS
        for await (let [update] of subs) {
          expect(update).toMatchObject({ '@insert': [{ '@id': 'fred' }] });
          return;
        }
      })(),
      api.write({ '@id': 'fred', name: 'Fred' })
    ]));
  });
});

