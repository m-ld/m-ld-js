import { clone, MeldUpdate } from '../src';
import { MemoryLevel } from 'memory-level';
import { MockRemotes, testConfig } from './testClones';
import { TSeqText } from '../src/tseq';

describe('TSeq datatype', () => {
  test('can insert and retrieve a TSeq', async () => {
    const datatype = new TSeqText('http://test.m-ld.org/#story');
    const meld = await clone(new MemoryLevel,
      MockRemotes,
      testConfig(),
      datatype
    );
    const validate = jest.spyOn(datatype, 'validate');
    const toValue = jest.spyOn(datatype, 'toValue');
    await meld.write({
      '@id': 'fred', story: 'Flintstones, meet the Flintstones'
    });
    expect(validate).toHaveBeenCalled();
    await expect(meld.get('fred')).resolves.toEqual({
      '@id': 'fred', story: 'Flintstones, meet the Flintstones'
    });
    expect(toValue).toHaveBeenCalled();
  });

  test('can concat to a TSeq', async () => {
    const meld = await clone(new MemoryLevel,
      MockRemotes,
      testConfig(),
      new TSeqText('http://test.m-ld.org/#story')
    );
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
    const meld = await clone(new MemoryLevel,
      MockRemotes,
      testConfig(),
      new TSeqText('http://test.m-ld.org/#story')
    );
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

  test('can declare a TSeq in data', async () => {
    const meld = await clone(new MemoryLevel, MockRemotes, testConfig());
    await meld.write(TSeqText.declare(0, 'story'));
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