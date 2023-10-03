const { Clone } = require('@m-ld/m-ld-test');

describe('TSeq text datatype', () => {
  let clones;

  const installDatatype = path => ({
    '@context': {
      'mld': 'http://m-ld.org/#',
      'js': 'http://js.m-ld.org/#',
      'sh': 'http://www.w3.org/ns/shacl#'
    },
    '@id': 'http://m-ld.org/extensions',
    '@list': [{
      'js:require': '@m-ld/m-ld/ext/tseq',
      'js:class': 'TSeqText',
      'sh:targetObjectsOf': { '@vocab': path }
    }]
  });

  it('manipulates text', async () => {
    const [clone] = clones = await Clone.start(1);
    await clone.transact(installDatatype('story'));
    await clone.transact({ '@id': 'fred', story: 'Flintstones, meet the Flintstones' });
    await clone.transact({
      '@update': {
        '@id': 'fred', story: { '@concat': ',\nThey\'re the modern stone age family' }
      }
    });
    await expectAsync(clone.transact({ '@describe': 'fred' })).toBeResolvedTo([{
      '@id': 'fred',
      story: 'Flintstones, meet the Flintstones,\n' +
        'They\'re the modern stone age family'
    }]);
  });

  it('shares text after cloning', async () => {
    const [clone1, clone2] = clones = Clone.create(2);
    await clone1.start(true);
    await clone1.transact(installDatatype('story'));
    await clone1.transact({ '@id': 'fred', story: 'Flintstones, meet the Flintstones' });
    await clone2.start();
    await expectAsync(clone2.transact({ '@describe': 'fred' })).toBeResolvedTo([{
      '@id': 'fred',
      story: 'Flintstones, meet the Flintstones'
    }]);
  });

  it('correctly merges text', async () => {
    const [clone1, clone2] = clones = await Clone.start(2);
    await clone1.transact(installDatatype('story'));
    await Promise.all([
      clone1.transact({ '@id': 'fred', story: 'Flintstones, meet the Flintstones' }),
      clone2.updated('@insert', 'fred')
    ]);
    await Promise.all([
      clone1.transact({
        '@update': {
          '@id': 'fred', story: { '@concat': ',\nThey\'re the modern stone age family' }
        }
      }),
      clone2.transact({
        '@insert': { '@id': 'fred', name: 'Fred' },
        '@update': {
          '@id': 'fred', story: { '@splice': [13, 1, 'tw'] }
        }
      }),
      clone1.updated('@insert', 'Fred')
    ]);
    await expectAsync(clone1.transact({ '@describe': 'fred' })).toBeResolvedTo([{
      '@id': 'fred',
      name: 'Fred',
      story: 'Flintstones, tweet the Flintstones,\n' +
        'They\'re the modern stone age family'
    }]);
  });

  it('reverts text during voiding', async () => {
    const [clone1, clone2] = clones = await Clone.start(2);
    await clone1.transact(installDatatype('story'));
    await Promise.all([
      clone1.transact({ '@id': 'fred', story: 'Flintstones, meet the Flintstones' }),
      clone2.updated('@insert', 'fred')
    ]);
    await clone2.partition();
    await clone2.transact({
      '@update': {
        '@id': 'fred', story: { '@concat': ',\nThey\'re the modern stone age family' }
      },
      '@agree': true
    });
    await clone1.transact({
      '@update': { '@id': 'fred', story: { '@splice': [11, 22] } } // -> 'Flintstones'
    });
    await Promise.all([
      clone2.partition(false),
      clone1.updated('@update', 'fred')
    ]);
    await expectAsync(clone1.transact({ '@describe': 'fred' })).toBeResolvedTo([{
      '@id': 'fred',
      story: 'Flintstones, meet the Flintstones,\n' +
        'They\'re the modern stone age family'
    }]);
  });

  afterEach(() => Promise.all(clones?.map(clone => clone.destroy())));
});