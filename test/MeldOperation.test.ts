import { MeldEncoder } from '../src/engine/MeldEncoding';
import { RdfFactory } from '../src/engine/quads';
import { baseVocab, domainBase } from '../src/engine/dataset/index';
import { EntryReversion, MeldOperation } from '../src/engine/MeldOperation';
import { TreeClock } from '../src/engine/clocks';
import { XS } from '../src/ns';
import { blankRegex } from './testUtil';
import { EncodedOperation } from '../src/engine/index';
import { IndirectedData, SharedDatatype } from '../src/index';
import { mock, mockFn } from 'jest-mock-extended';

// Note that MeldOperation is well tested via SuSetDataset etc.
// This file is primarily for edge cases and failure cases.
describe('Meld Operation class', () => {
  let encoder: MeldEncoder;
  let indirectedData: ReturnType<typeof mockFn<IndirectedData>>;
  let rdf: RdfFactory;
  let base: string;
  let vocab: string;

  beforeEach(async () => {
    const domain = 'test.m-ld.org';
    base = domainBase(domain);
    vocab = baseVocab(base);
    rdf = new RdfFactory(base);
    indirectedData = mockFn<IndirectedData>();
    encoder = new MeldEncoder(domain, rdf, indirectedData);
    await encoder.initialise();
  });

  const extractUpdate = (op: MeldOperation): [any, any, any] => MeldEncoder.jsonFromBuffer(
    op.encoded[EncodedOperation.Key.update],
    op.encoded[EncodedOperation.Key.encoding]
  );

  test('encodes insert', () => {
    const time = TreeClock.GENESIS;
    const op = MeldOperation.fromOperation(encoder, {
      from: time.ticks,
      time: time,
      deletes: [],
      inserts: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [time.hash]]],
      updates: [],
      principalId: null,
      agreed: null
    });
    expect(op.encoded).toMatchObject([
      5, 0, time.toJSON(), expect.any(Buffer), expect.any(Array), null, null
    ]);
    expect(extractUpdate(op)).toEqual([{}, {
      '@id': 'fred', name: 'Fred'
    }]);
  });

  test('encodes fused insert', () => {
    const time1 = TreeClock.GENESIS, time2 = time1.ticked();
    const op = MeldOperation.fromOperation(encoder, {
      from: time1.ticks,
      time: time2,
      deletes: [],
      inserts: [
        [rdf.quad(
          rdf.namedNode(base + 'fred'),
          rdf.namedNode(vocab + 'name'),
          rdf.literal('Fred')
        ), [time1.hash]],
        [rdf.quad(
          rdf.namedNode(base + 'fred'),
          rdf.namedNode(vocab + 'age'),
          rdf.literal('40', rdf.namedNode(XS.integer))
        ), [time2.hash]]
      ],
      updates: [],
      principalId: null,
      agreed: null
    });
    expect(op.encoded).toMatchObject([
      5, 0, time2.toJSON(), expect.any(Buffer), expect.any(Array), null, null
    ]);
    expect(extractUpdate(op)).toEqual([
      {},
      [
        {
          '@id': expect.stringMatching(blankRegex),
          s: 'fred', p: '#name', o: 'Fred', tid: time1.hash
        },
        {
          '@id': expect.stringMatching(blankRegex),
          s: 'fred', p: '#age', o: 40, tid: time2.hash
        }
      ]
    ]);
  });

  test('fuses inserts', () => {
    const time1 = TreeClock.GENESIS;
    const op1 = MeldOperation.fromOperation(encoder, {
      from: time1.ticks,
      time: time1,
      deletes: [],
      inserts: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [time1.hash]]],
      updates: [],
      principalId: null,
      agreed: null
    });
    const time2 = time1.ticked();
    const op2 = MeldOperation.fromOperation(encoder, {
      from: time2.ticks,
      time: time2,
      deletes: [],
      inserts: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'age'),
        rdf.literal('40', rdf.namedNode(XS.integer))
      ), [time2.hash]]],
      updates: [],
      principalId: null,
      agreed: null
    });
    const op = MeldOperation.fromOperation(encoder, op1.fusion().next(op2).commit());
    expect(op.encoded).toMatchObject([
      5, 0, time2.toJSON(), expect.any(Buffer), expect.any(Array), null, null
    ]);
    expect(extractUpdate(op)).toEqual([
      {},
      [
        {
          '@id': expect.stringMatching(blankRegex),
          s: 'fred', p: '#name', o: 'Fred', tid: time1.hash
        },
        {
          '@id': expect.stringMatching(blankRegex),
          s: 'fred', p: '#age', o: 40, tid: time2.hash
        }
      ]
    ]);
  });

  test('fuses and cuts insert-delete', () => {
    const time1 = TreeClock.GENESIS;
    const op1 = MeldOperation.fromOperation(encoder, {
      from: time1.ticks,
      time: time1,
      deletes: [],
      inserts: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [time1.hash]]],
      updates: [],
      principalId: null,
      agreed: null
    });
    const time2 = time1.ticked();
    const op2 = MeldOperation.fromOperation(encoder, {
      from: time2.ticks,
      time: time2,
      deletes: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [time1.hash]]],
      inserts: [],
      updates: [],
      principalId: null,
      agreed: null
    });
    const fOp = MeldOperation.fromOperation(encoder, op1.fusion().next(op2).commit());
    expect(fOp.encoded).toMatchObject([
      5, 0, time2.toJSON(), expect.any(Buffer), expect.any(Array), null, null
    ]);
    expect(extractUpdate(fOp)).toEqual([{}, {}]);
    const cOp = MeldOperation.fromOperation(encoder, fOp.cutting().next(op1).commit());
    const expectedDeletion = {
      '@id': expect.stringMatching(blankRegex),
      s: 'fred', p: '#name', o: 'Fred', tid: time1.hash
    };
    expect(extractUpdate(op2)).toEqual([expectedDeletion, {}]);
    expect(extractUpdate(cOp)).toEqual([expectedDeletion, {}]);
  });

  test('fuses insert-delete with TID reversion', () => {
    const time1 = TreeClock.GENESIS;
    const op1 = MeldOperation.fromOperation(encoder, {
      from: time1.ticks,
      time: time1,
      deletes: [],
      inserts: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [time1.hash]]],
      updates: [],
      principalId: null,
      agreed: null
    });
    const rev1: EntryReversion = {};
    const time2 = time1.ticked();
    const op2 = MeldOperation.fromOperation(encoder, {
      from: time2.ticks,
      time: time2,
      deletes: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [time1.hash]]],
      inserts: [],
      updates: [],
      principalId: null,
      agreed: null
    });
    const rev2: EntryReversion = { [extractUpdate(op2)[0]['@id']]: [time1.hash] };
    const fusion = op1.fusion(rev1).next(op2, rev2);
    fusion.commit();
    expect(fusion.reversion).toEqual({});
  });

  test('fuses delete-delete with TID reversion', () => {
    // Two clones independently create fred at time 1 & 2,
    // and the local clone deletes each fred independently at time 3 & 4 respectively.
    let { left: localTime, right: remoteTime } = TreeClock.GENESIS.forked();
    let { left: remoteTime1, right: remoteTime2 } = remoteTime.forked();
    remoteTime1 = remoteTime1.ticked();
    remoteTime2 = remoteTime2.ticked();
    const time1 = localTime.update(remoteTime1).update(remoteTime2).ticked();
    const op1 = MeldOperation.fromOperation(encoder, {
      from: time1.ticks,
      time: time1,
      deletes: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [remoteTime1.hash]]],
      inserts: [],
      updates: [],
      principalId: null,
      agreed: null
    });
    const rev1: EntryReversion = { [extractUpdate(op1)[0]['@id']]: [remoteTime1.hash] };
    const time2 = time1.ticked();
    const op2 = MeldOperation.fromOperation(encoder, {
      from: time2.ticks,
      time: time2,
      deletes: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'name'),
        rdf.literal('Fred')
      ), [remoteTime2.hash]]],
      inserts: [],
      updates: [],
      principalId: null,
      agreed: null
    });
    const rev2: EntryReversion = { [extractUpdate(op2)[0]['@id']]: [remoteTime2.hash] };
    const fusion = op1.fusion(rev1).next(op2, rev2);
    const op = MeldOperation.fromOperation(encoder, fusion.commit());
    expect(extractUpdate(op)).toEqual([{
      '@id': expect.stringMatching(blankRegex),
      s: 'fred', p: '#name', o: 'Fred', tid: [remoteTime1.hash, remoteTime2.hash]
    }, {}]);
    expect(fusion.reversion).toEqual({
      [extractUpdate(op)[0]['@id']]: [remoteTime1.hash, remoteTime2.hash]
    });
  });

  test('fuses update-update with meta reversion', () => {
    const dt = mock<SharedDatatype<string, string, string>>({
      '@id': vocab + 'test-datatype',
      update: mockFn(), // Required to identify as a shared datatype
      fuse: ([op1, rv1], [op2, rv2]) => [op1 + op2, rv1 && rv2 && rv1 + rv2]
    });
    indirectedData.mockImplementation(id => {
      if (id === dt['@id']) return dt;
    });
    const time1 = TreeClock.GENESIS;
    const op1 = MeldOperation.fromOperation(encoder, {
      from: time1.ticks,
      time: time1,
      deletes: [],
      inserts: [],
      updates: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'data'),
        rdf.literal('data-id', dt, 'shared-data-1')
      ), 'update-op-1']],
      principalId: null,
      agreed: null
    });
    const rev1: EntryReversion = { [extractUpdate(op1)[2]['@id']]: 'update-meta-1' };
    const time2 = time1.ticked();
    const op2 = MeldOperation.fromOperation(encoder, {
      from: time2.ticks,
      time: time2,
      deletes: [],
      inserts: [],
      updates: [[rdf.quad(
        rdf.namedNode(base + 'fred'),
        rdf.namedNode(vocab + 'data'),
        rdf.literal('data-id', dt, 'shared-data-2')
      ), 'update-op-2']],
      principalId: null,
      agreed: null
    });
    const rev2: EntryReversion = { [extractUpdate(op2)[2]['@id']]: 'update-meta-2' };
    const fusion = op1.fusion(rev1).next(op2, rev2);
    const op = MeldOperation.fromOperation(encoder, fusion.commit());
    expect(fusion.reversion).toEqual({
      // the update reversion should be keyed against the final op's update
      [extractUpdate(op)[2]['@id']]: 'update-meta-1update-meta-2'
    });
  });
});