import { Graph, KvpBatch, PatchQuads, TxnContext } from '.';
import { blank, Datatype, GraphSubject, IndirectedData, isSharedDatatype } from '../../api';
import { Atom, Expression, Result, Subject, Value } from '../../jrql-support';
import {
  inPosition, isLiteralTriple, isTypedTriple, LiteralTriple, Quad, Quad_Object, Term, Triple,
  tripleKey, TypedData
} from '../quads';
import { JRQL } from '../../ns';
import { SubjectGraph } from '../SubjectGraph';
import { JrqlMode, toIndexDataUrl } from '../jrql-util';
import { clone, concatIter, IndexKeyGenerator, isArray, mapIter, mapObject } from '../util';
import { SubjectQuads } from '../SubjectQuads';
import { Binding } from '../../rdfjs-support';
import * as MsgPack from '../msgPack';
import { Operation } from '../ops';
import { takeWhile } from 'rxjs/operators';
import { drain } from 'rx-flowable';
import { JsonldContext } from '../jsonld';
import { IndexMap } from '../indices';
import { CacheFactory } from '../cache';

export class JrqlQuads {
  /**
   * Cache of loaded data. This primarily allows shared data to persist in
   * memory while having operations applied, for example while a user types. The
   * `ticks` key lags the `data` during an operation, as it's applied on commit.
   */
  private dataCache: {
    get(tripleKey: DataKey): LoadedData | undefined,
    set(triple: LiteralTriple, loaded: LoadedData, txc?: TxnContext): LoadedData
  };

  constructor(
    readonly graph: Graph,
    readonly indirectedData: IndirectedData,
    readonly cacheFactory: CacheFactory
  ) {
    const lruCache = cacheFactory.createCache<DataKey, LoadedData>({
      length: loaded => loaded.type.sizeOf(loaded.data)
    });
    // Allow for concurrent transactions, but there should only ever be one
    const txnData = new class extends IndexMap<TxnContext, Set<DataKey>> {
      getIndex(key: TxnContext) { return key.id; }
    }();
    const newTxc = (txc: TxnContext): Set<DataKey> => {
      txc.on('commit', () => txnData.delete(txc));
      txc.on('rollback', () => {
        const loaded = txnData.delete(txc);
        if (lruCache != null && loaded?.size)
          for (let tripleKey of loaded ?? [])
            lruCache.del(tripleKey);
      });
      return new Set;
    };
    this.dataCache = {
      get(tripleKey) {
        return lruCache?.get(tripleKey);
      },
      set(triple, loaded, txc) {
        const { type, data, tripleKey } = loaded;
        triple.object.typed = { type, data };
        const added = lruCache?.set(tripleKey, loaded);
        if (added && txc != null)
          txnData.with(txc, newTxc).add(tripleKey);
        return loaded;
      }
    };
  }

  async solutionSubject(
    results: Result[] | Result,
    solution: Binding,
    ctx: JsonldContext
  ): Promise<GraphSubject> {
    const solutionId = this.rdf.blankNode(blank());
    const pseudoPropertyQuads = Object.entries(solution).map(([variable, term]) => this.graph.quad(
      solutionId,
      this.rdf.namedNode(JRQL.hiddenVar(variable.slice(1))),
      inPosition('object', term)
    ));
    // Construct quads that represent the solution's variable values
    const subject = await this.toApiSubject(
      pseudoPropertyQuads, [ /* TODO: list-items */], ctx);
    // Unhide the variables and strip out anything that's not selected
    return <GraphSubject>mapObject(subject, (key, value) => {
      switch (key) {
        case '@id':
          return { [key]: value };
        default:
          const varName = JRQL.matchHiddenVar(key), newKey = (varName ? '?' + varName : key);
          if (isSelected(results, newKey))
            return { [newKey]: value };
      }
    });
  }

  get rdf() {
    return this.graph.rdf;
  }

  in(mode: JrqlMode, ctx: JsonldContext) {
    return new SubjectQuads(this.rdf, mode, ctx, this.indirectedData);
  }

  toQuads(
    subjects: Subject | Subject[],
    mode: JrqlMode,
    ctx: JsonldContext
  ): Quad[] {
    return this.in(mode, ctx).toQuads(subjects);
  }

  /**
   * @param propertyQuads subject-property-value quads
   * @param listItemQuads subject-index-item quads for list-like subjects
   * @param ctx JSON-LD context
   * @returns a single subject compacted against the given context
   */
  async toApiSubject(
    propertyQuads: Quad[],
    listItemQuads: Quad[],
    ctx: JsonldContext
  ): Promise<GraphSubject> {
    await Promise.all(propertyQuads.map(quad => this.loadData(quad)));
    const subjects = SubjectGraph.fromRDF(propertyQuads, { ctx });
    const subject = { ...subjects[0] };
    if (listItemQuads.length) {
      // Sort the list items lexically by index
      // TODO: Allow for a list implementation-specific ordering
      const indexes = new Set(listItemQuads.map(iq => iq.predicate.value).sort()
        .map(index => ctx.compactIri(index)));
      // Create a subject containing only the list items
      const list = await this.toApiSubject(listItemQuads, [], ctx);
      subject['@list'] = [...indexes].map(index => <Value>list[index]);
    }
    return subject;
  }

  genSubValue(parentValue: Term, subVarName: JRQL.SubVarName) {
    switch (subVarName) {
      case 'listKey':
        // Generating a data URL for the index key
        return this.rdf.namedNode(toIndexDataUrl([Number(parentValue.value)]));
      case 'slotId':
        // Index exists, so a slot can be made
        return this.rdf.skolem();
    }
  }

  toObjectTerm(value: Atom, ctx: JsonldContext): Quad_Object {
    return this.in(JrqlMode.match, ctx).objectTerm(value);
  }

  async applyTripleUpdate(
    triple: Quad,
    update: Expression,
    txc: TxnContext
  ): Promise<UpdateMeta | undefined> {
    if (isLiteralTriple(triple)) {
      const datatype = this.indirectedData(
        triple.predicate.value, triple.object.datatype.value);
      // TODO: Bug: what if the datatype is no longer shared?
      if (datatype != null && isSharedDatatype(datatype)) {
        const loaded = await this.loadDataOfType(triple, datatype);
        if (loaded != null) {
          const [data, operation, revert] =
            datatype.update(loaded.data, update);
          this.dataCache.set(triple, { ...loaded, data }, txc);
          return { operation, update, revert };
        }
      }
    }
  }

  async applyTripleOperation(
    triple: Quad,
    operation: unknown,
    txc: TxnContext
  ): Promise<UpdateMeta | undefined> {
    if (isLiteralTriple(triple)) {
      const loaded = await this.loadData(triple);
      if (loaded != null && isSharedDatatype(loaded.type)) {
        // Shared datatypes have UUID values, so the type should be correct
        const [data, update, revert] = loaded.type.apply(loaded.data, operation);
        this.dataCache.set(triple, { ...loaded, data }, txc);
        return { operation, update, revert };
      }
    }
  }

  loadHasData(triples: Iterable<JrqlDataQuad>) {
    return Promise.all(mapIter(triples, async triple => {
      if (isLiteralTriple(triple) && triple.hasData == null) {
        const datatype = this.indirectedData(
          triple.predicate.value, triple.object.datatype.value);
        if (datatype != null) {
          const tripleKey = this.dataKeyFor(triple);
          const cached = this.dataCache.get(tripleKey);
          if (cached != null)
            return triple.hasData = cached;
          const keys = await this.loadDataAndOps(tripleKey, { values: false });
          if (keys.length > 0)
            return triple.hasData = this.getDataMeta(datatype, keys);
          // Not updating the cache here because we haven't loaded the data itself
        }
      }
      triple.hasData = false;
    }));
  }

  private getDataMeta(datatype: Datatype, keys: [string, unknown][] = []): DataMeta {
    return {
      shared: isSharedDatatype(datatype),
      ticks: keys.slice(1).map(([key]) => this.dataTickFrom(key))
    };
  }

  private loadDataAndOps(tripleKey: DataKey, opts: { values?: false } = {}) {
    return drain(this.graph.read({ gte: tripleKey, ...opts }).pipe(
      takeWhile(({ value: [key] }) => key.startsWith(tripleKey))));
  }

  async loadData(triple: Triple): Promise<LoadedData | undefined> {
    if (isLiteralTriple(triple)) {
      const datatype = this.indirectedData(
        triple.predicate.value, triple.object.datatype.value);
      if (datatype != null)
        return this.loadDataOfType(triple, datatype);
    }
  }

  private async loadDataOfType(
    triple: LiteralTriple,
    datatype: Datatype
  ): Promise<LoadedData | undefined> {
    // TODO: Allow for datatype caching
    const tripleKey = this.dataKeyFor(triple);
    const cached = this.dataCache.get(tripleKey);
    if (cached != null)
      return triple.object.typed = cached;
    const keyValues = await this.loadDataAndOps(tripleKey);
    if (keyValues.length > 0) {
      const [snapshot, ...ops] = keyValues.map(([, data]) => MsgPack.decode(data));
      let data = datatype.fromJSON ? datatype.fromJSON(snapshot) : snapshot;
      if (isSharedDatatype(datatype)) {
        for (let op of ops)
          [data] = datatype.apply(data, op);
      } else if (ops.length > 0) {
        throw new Error('Operations found for a non-shared datatype');
      }
      return this.dataCache.set(triple, {
        tripleKey, type: datatype, data, ...this.getDataMeta(datatype, keyValues)
      });
    }
  }

  saveData(txc: TxnContext, patch: JrqlQuadOperation, batch: KvpBatch, tick?: number) {
    for (let triple of concatIter(patch.deletes, patch.inserts)) {
      if (triple.hasData) {
        batch.del(this.dataKeyFor(triple));
        if (triple.hasData.shared) {
          for (let tick of triple.hasData.ticks)
            batch.del(this.dataKeyFor(triple, tick));
        }
      }
    }
    for (let quad of patch.inserts) {
      if (isTypedTriple(quad)) {
        const { type: datatype, data } = quad.object.typed;
        const json = datatype.toJSON ? datatype.toJSON(data) : data;
        const tripleKey = this.dataKeyFor(quad);
        batch.put(tripleKey, MsgPack.encode(json));
        this.dataCache.set(quad, {
          tripleKey, ...quad.object.typed, ...this.getDataMeta(datatype)
        }, txc);
      }
    }
    for (let [quad, { operation }] of patch.updates) {
      if (tick == null)
        throw new RangeError('Saving shared data operations requires a tick');
      const tripleKey = this.dataKeyFor(quad);
      const loaded = this.dataCache.get(tripleKey);
      // If the loaded data is too big, it may not be in the cache at all
      if (loaded?.shared)
        loaded.ticks.push(tick); // Mutates cache content! Avoids size re-calc
      // FIXME: If the same triple appears twice!
      batch.put(this.dataKeyFor(quad, tick), MsgPack.encode(operation));
    }
  }

  private dataKeyFor(triple: Triple, tick?: number): DataKey {
    /** Prefix for data keys */
    const suffix = tick == null ? '' : `,${DATA_KEY_GEN.key(tick)}`;
    return `${DATA_KEY_PRE}${tripleKey(triple, this.graph.prefixes)}${suffix}`;
  }

  private dataTickFrom(key: DataKey) {
    return DATA_KEY_GEN.index(key.slice(-DATA_KEY_GEN.length));
  }
}

type DataKey = string;
const DATA_KEY_PRE = '_qs:dat:';
const DATA_KEY_GEN = new IndexKeyGenerator();

/** Whether data is shared, and if so, what operation ticks are stored */
type DataMeta = Readonly<{ shared: false } | { shared: true, ticks: number[] }>;
/** Stored data with metadata */
type LoadedData = TypedData & DataMeta & {
  /** Key for the triple itself (not the ticks; redundant with cache key) */
  tripleKey: DataKey,
};

export interface JrqlDataQuad extends Quad {
  /** Does this triple have attached data? If so, shared metadata */
  hasData?: false | DataMeta;
}

export interface UpdateMeta {
  /** The operation */
  operation: unknown,
  /** The json-rql expression(s) used to perform the update */
  update: Expression | Expression[],
  /** Journaled metadata required to revert the operation */
  revert: unknown
}

/** Operation over quads, with attached metadata (interface is read-only) */
export interface JrqlQuadOperation extends Operation<JrqlDataQuad> {
  /** Metadata of shared datatype operations */
  updates: Iterable<[Quad, UpdateMeta]>;
}

export class JrqlPatchQuads extends PatchQuads implements JrqlQuadOperation {
  /**
   * Quad, having shared data, with operation on that data
   */
  readonly updates: [Quad, UpdateMeta][] = [];

  constructor(patch: Partial<JrqlQuadOperation> = {}) {
    super(patch);
    this.inheritMeta(patch);
  }

  include(patch: Partial<JrqlQuadOperation>) {
    this.inheritMeta(patch);
    return super.include(patch);
  }

  append(patch: Partial<JrqlQuadOperation>) {
    this.inheritMeta(patch);
    return super.append(patch);
  }

  addUpdateMeta(triple: Quad, opMeta: UpdateMeta) {
    if (!isLiteralTriple(triple))
      throw new RangeError('Shared data triple must have a literal object');
    if (isTypedTriple(triple)) {
      // Un-type a typed triple, as the data is not relevant
      const { typed: _, ...object } = triple.object;
      triple = clone(triple, { ...triple, object: clone(triple.object, object) });
    }
    this.updates.push([triple, opMeta]);
  }

  get isEmpty(): boolean {
    return super.isEmpty && this.updates.length === 0;
  }

  private inheritMeta(patch: Partial<JrqlQuadOperation>) {
    if (patch.updates != null) {
      for (let [triple, opMetas] of patch.updates)
        this.addUpdateMeta(triple, opMetas);
    }
  }
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (isArray(results) ? results.includes(key) : results === key);
}