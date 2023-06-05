import { clone } from '../src';
import { MemoryLevel } from 'memory-level';
import { MockRemotes, testConfig } from './testClones';
import { TSeqDatatype } from '../src/tseq';
import { MeldUpdate } from '@m-ld/m-ld-spec/types';

describe('TSeq datatype', () => {
  test('can insert and retrieve a TSeq', async () => {
    const meld = await clone(new MemoryLevel, MockRemotes, testConfig(), new TSeqDatatype());
    await meld.write({
      '@id': 'fred',
      story: {
        '@type': 'http://ext.m-ld.org/tseq/TSeq',
        '@value': 'Flintstones, meet the Flintstones'
      }
    });
    await expect(meld.get('fred')).resolves.toEqual({
      '@id': 'fred',
      story: {
        '@type': 'http://ext.m-ld.org/tseq/TSeq',
        '@value': 'Flintstones, meet the Flintstones'
      }
    });
  });

  test('can insert and retrieve a TSeq using a context', async () => {
    const meld = await clone(new MemoryLevel, MockRemotes, testConfig({
      '@context': { story: { '@type': 'http://ext.m-ld.org/tseq/TSeq' } }
    }), new TSeqDatatype());
    await meld.write({
      '@id': 'fred', story: 'Flintstones, meet the Flintstones'
    });
    await expect(meld.get('fred')).resolves.toEqual({
      '@id': 'fred', story: 'Flintstones, meet the Flintstones'
    });
  });

  test('can concat to a TSeq', async () => {
    const meld = await clone(new MemoryLevel, MockRemotes, testConfig({
      '@context': { story: { '@type': 'http://ext.m-ld.org/tseq/TSeq' } }
    }), new TSeqDatatype());
    await meld.write({
      '@id': 'fred', story: 'Flintstones, meet the Flintstones'
    });
    await meld.write({
      '@update': {
        '@id': 'fred', story: { '@concat': ',\nThey\'re the modern stone age family' }
      }
    });
    await expect(meld.get('fred')).resolves.toEqual({
      '@id': 'fred',
      story: 'Flintstones, meet the Flintstones,\n' +
        'They\'re the modern stone age family'
    });
  });

  test('can splice into a TSeq', async () => {
    const meld = await clone(new MemoryLevel, MockRemotes, testConfig({
      '@context': { story: { '@type': 'http://ext.m-ld.org/tseq/TSeq' } }
    }), new TSeqDatatype());
    await meld.write({
      '@id': 'fred', story: 'Flintstones, tweet the Flintstones'
    });
    const updates: MeldUpdate[] = [];
    meld.follow(update => { updates.push(update); });
    await meld.write({
      '@update': { '@id': 'fred', story: { '@splice': [13, 2, 'm'] } }
    });
    expect(updates).toMatchObject([{
      '@update': [{ '@id': 'fred', story: { '@splice': [13, 2, 'm'] } }]
    }]);
    await expect(meld.get('fred')).resolves.toEqual({
      '@id': 'fred', story: 'Flintstones, meet the Flintstones'
    });
  });
});