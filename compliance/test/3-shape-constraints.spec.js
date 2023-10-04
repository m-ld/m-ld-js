const { Clone } = require('@m-ld/m-ld-test');

describe('SHACL shape constraints', () => {
  let clones;

  const installConstraint = ({ path = 'name', minCount, maxCount }) => ({
    '@context': {
      'mld': 'http://m-ld.org/#',
      'js': 'http://js.m-ld.org/#',
      'sh': 'http://www.w3.org/ns/shacl#'
    },
    '@id': 'http://m-ld.org/extensions',
    '@list': [{
      'js:require': '@m-ld/m-ld/ext/shacl',
      'js:class': 'ShapeConstrained',
      'mld:controlled-shape': {
        'sh:path': { '@vocab': path },
        'sh:minCount': minCount,
        'sh:maxCount': maxCount
      }
    }]
  });

  it('enforces max count 1', async () => {
    const [clone] = clones = await Clone.start(1);
    await clone.transact(installConstraint({ maxCount: 1 }));
    await clone.transact({ '@id': 'fred', name: 'Fred' });
    await expectAsync(clone.transact({ '@id': 'fred', name: 'Flintstone' }))
      .toBeRejectedWithError();
  });

  it('enforces min count 1', async () => {
    const [clone] = clones = await Clone.start(1);
    await clone.transact(installConstraint({ minCount: 1 }));
    await clone.transact({ '@id': 'fred', name: 'Fred', height: 1 });
    await expectAsync(clone.transact({ '@delete': { '@id': 'fred', name: 'Fred' } }))
      .toBeRejectedWithError();
  });

  it('allows full subject deletion', async () => {
    const [clone] = clones = await Clone.start(1);
    await clone.transact(installConstraint({ minCount: 1 }));
    await clone.transact({ '@id': 'fred', name: 'Fred' });
    await expectAsync(clone.transact({ '@delete': { '@id': 'fred', name: 'Fred' } }))
      .toBeResolved();
  });

  it('resolves to the max value', async () => {
    const [clone1, clone2] = clones = await Clone.start(2);
    await clone1.transact(installConstraint({ maxCount: 1 }));
    await Promise.all([
      clone1.transact({ '@id': 'fred', name: 'Flintstone', height: 6 }),
      clone2.transact({ '@id': 'fred', name: 'Fred', sex: 'm' }),
      clone1.updated('@insert', 'sex', 'm'),
      clone2.updated('@insert', 'height', 6)
    ]);
    const [fred1] = await clone1.transact({ '@describe': 'fred' });
    expect(fred1.name).toBe('Fred');
    const [fred2] = await clone2.transact({ '@describe': 'fred' });
    expect(fred2.name).toBe('Fred');
  });

  it('resolves despite explicit delete', async () => {
    const [clone1, clone2, clone3] = clones = Clone.create(3);
    await clone1.start();
    await clone1.transact(installConstraint({ maxCount: 1 }));
    await clone2.start(true);
    await clone3.start(true);
    await clone2.partition();
    await Promise.all([
      clone1.transact({ '@id': 'fred', name: 'Fred' }),
      clone2.transact({ '@id': 'fred', name: 'Flintstone' }),
      clone3.updated('@insert', 'Fred') // Now has Fred but not Flintstone
    ]);
    await clone3.partition();
    // Explicit delete of Fred leaves clone3 without a value while partitioned
    await clone3.transact({ '@delete': { '@id': 'fred', name: 'Fred' } });
    await Promise.all([
      clone2.partition(false), // Publishes Flintstone post-recovery
      clone2.updated('@insert', 'Fred') // Converged with clone1
      // (No way to tell when clone1 gets Flintstone, because no update)
    ]);
    await Promise.all([
      clone3.partition(false),
      clone1.updated('@delete', 'Fred'),
      clone2.updated('@delete', 'Fred'),
      clone3.updated('@insert', 'Flintstone')
    ]);
    // Everyone converges
    const subjects = await Promise.all(
      clones.map(clone => clone.transact({ '@describe': 'fred' })));
    expect(subjects.every(([fred]) => fred.name === 'Flintstone')).toBe(true);
  });

  afterEach(() => Promise.all(clones?.map(clone => clone.destroy())));
});