import { Graph, KvpBatch, PatchQuads, TxnContext } from '.';
import {
  Datatype, GraphSubject, IndirectedData, isSharedDatatype, LocalDataOperation
} from '../../api';
import { Atom, Expression, Result, Subject, Value } from '../../jrql-support';
import {
  inPosition, isLiteralTriple, isTypedTriple, Literal, LiteralTriple, Quad, Quad_Object, Term,
  TypedData, TypedLiteral, TypedTriple
} from '../quads';
import { JRQL } from '../../ns';
import { SubjectGraph } from '../SubjectGraph';
import { JrqlMode, toIndexDataUrl } from '../jrql-util';
import { IndexKeyGenerator, isArray, mapIter, mapObject } from '../util';
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
    get(dataKey: DataKey): LoadedData | undefined,
    set(literal: Literal, loaded: LoadedData, txc?: TxnContext): LoadedData
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
      set(literal, loaded, txc) {
        const { type, data, id } = loaded;
        literal.typed = { type, data };
        const added = lruCache?.set(id, loaded);
        if (added && txc != null)
          txnData.with(txc, newTxc).add(id);
        return loaded;
      }
    };
  }

  async solutionSubject(
    results: Result[] | Result,
    solution: Binding,
    ctx: JsonldContext
  ): Promise<GraphSubject> {
    const solutionId = this.rdf.blankNode();
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
    await Promise.all(propertyQuads.map(quad => this.loadData(quad.object)));
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
    quad: JrqlQuad,
    update: Expression,
    txc: TxnContext
  ): Promise<QuadUpdate | undefined> {
    if (isLiteralTriple(quad)) {
      const datatype = this.indirectedData(quad.object.datatype.value);
      // TODO: Bug: what if the datatype is no longer shared?
      if (datatype != null && isSharedDatatype(datatype)) {
        const loaded = await this.loadDataOfType(quad.object, datatype);
        if (loaded != null) {
          const [data, operation, revert] =
            datatype.update(loaded.data, update);
          if (!this.upgradeToDataQuad(quad, { ...loaded, data }, txc))
            throw new TypeError();
          return { quad, operation, update, revert };
        }
      }
    }
  }

  async applyTripleOperation(
    quad: Quad,
    reversions: LocalDataOperation[],
    operation: unknown | undefined,
    txc: TxnContext
  ): Promise<QuadUpdate | undefined> {
    if (isLiteralTriple(quad)) {
      const loaded = await this.loadData(quad.object);
      if (loaded != null && isSharedDatatype(loaded.type)) {
        // Shared datatypes have UUID values, so the type should be correct
        const [data, update, revert] =
          loaded.type.apply(loaded.data, reversions, operation);
        if (!this.upgradeToDataQuad(quad, { ...loaded, data }, txc))
          throw new TypeError();
        const snapshot = !!reversions.length || undefined;
        return { quad, operation, update, revert, snapshot };
      }
    }
  }

  private upgradeToDataQuad(
    quad: JrqlQuad & LiteralTriple,
    loaded: LoadedData,
    txc: TxnContext
  ): quad is JrqlSharedDataQuad {
    this.dataCache.set(quad.object, loaded, txc);
    // Redundant with the typed object literal, co-mutable
    quad.hasData = loaded;
    return isTypedTriple(quad);
  }

  loadHasData(triples: Iterable<JrqlQuad>) {
    return Promise.all(mapIter(triples, async triple => {
      if (isLiteralTriple(triple) && triple.hasData == null) {
        const datatype = this.indirectedData(triple.object.datatype.value);
        if (datatype != null) {
          const dataKey = this.dataKeyFor(triple.object);
          const cached = this.dataCache.get(dataKey);
          if (cached != null)
            return triple.hasData = cached;
          const keys = await this.loadDataAndOps(dataKey, { values: false });
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

  async loadData(object: Quad_Object): Promise<LoadedData | undefined> {
    if (object.termType == 'Literal') {
      const datatype = this.indirectedData(object.datatype.value);
      if (datatype != null)
        return this.loadDataOfType(object, datatype);
    }
  }

  private async loadDataOfType(
    literal: Literal,
    datatype: Datatype
  ): Promise<LoadedData | undefined> {
    // TODO: Allow for datatype caching
    const dataKey = this.dataKeyFor(literal);
    const cached = this.dataCache.get(dataKey);
    if (cached != null) {
      return literal.typed = cached;
    } else {
      const keyValues = await this.loadDataAndOps(dataKey);
      if (keyValues.length > 0) {
        const [snapshot, ...ops] = keyValues.map(([, data]) => MsgPack.decode(data));
        let data = datatype.fromJSON ? datatype.fromJSON(snapshot) : snapshot;
        if (isSharedDatatype(datatype)) {
          for (let op of ops)
            [data] = datatype.apply(data, [], op);
        } else if (ops.length > 0) {
          throw new Error('Operations found for a non-shared datatype');
        }
        return this.dataCache.set(literal, {
          id: dataKey, type: datatype, data, ...this.getDataMeta(datatype, keyValues)
        });
      }
    }
  }

  saveData(txc: TxnContext, patch: JrqlQuadOperation, batch: KvpBatch, tick?: number) {
    for (let quad of patch.inserts) {
      if (isTypedTriple(quad))
        this.saveDataSnapshot(txc, quad, batch, true);
    }
    for (let { quad, operation, snapshot } of patch.updates) {
      if (tick == null)
        throw new RangeError('Saving shared data operations requires a tick');
      if (snapshot) {
        for (let tick of quad.hasData.ticks)
          batch.del(this.dataKeyFor(quad.object, tick));
        this.saveDataSnapshot(txc, quad, batch, false);
      } else {
        quad.hasData.ticks.push(tick); // Mutates cache content! Avoids size re-calc
        batch.put(this.dataKeyFor(quad.object, tick), MsgPack.encode(operation));
      }
    }
  }

  private saveDataSnapshot(
    txc: TxnContext,
    quad: JrqlQuad & TypedTriple,
    batch: KvpBatch,
    updateCache: boolean
  ) {
    const dataKey = this.dataKeyFor(quad.object);
    if (quad.object.typed.type != null) {
      const { type: datatype, data } = quad.object.typed;
      const json = datatype.toJSON ? datatype.toJSON(data) : data;
      batch.put(dataKey, MsgPack.encode(json));
      if (updateCache) {
        this.dataCache.set(quad.object, {
          id: dataKey, ...quad.object.typed, ...this.getDataMeta(datatype)
        }, txc);
      }
    } else {
      // Data is already JSON; never caching
      batch.put(dataKey, MsgPack.encode(quad.object.typed.data));
    }
  }

  private dataKeyFor(literal: Literal, tick?: number): DataKey {
    /** Prefix for data keys */
    const suffix = tick == null ? '' : `,${DATA_KEY_GEN.key(tick)}`;
    const dataTypeIri = this.graph.prefixes.compactIri(literal.datatype.value);
    return `${DATA_KEY_PRE}${JSON.stringify([dataTypeIri, literal.value])}${suffix}`;
  }

  private dataTickFrom(key: DataKey) {
    return DATA_KEY_GEN.index(key.slice(-DATA_KEY_GEN.length));
  }
}

type DataKey = string;
const DATA_KEY_PRE = '_qs:dat:';
const DATA_KEY_GEN = new IndexKeyGenerator();

/** What operation ticks are stored for shared data */
type SharedDataMeta = { shared: true, ticks: number[] };
/** Whether data is shared, and if so, what operation ticks are stored */
type DataMeta = Readonly<{ shared: false } | SharedDataMeta>;
/** Stored data with metadata */
type LoadedData = TypedData & DataMeta & {
  /** Key for the data itself (not the ticks; redundant with cache key) */
  id: DataKey
};

export interface JrqlQuad extends Quad {
  /** Does this triple have attached data? If so, shared metadata */
  hasData?: false | DataMeta;
}

export interface JrqlSharedDataQuad extends JrqlQuad {
  object: TypedLiteral;
  hasData: SharedDataMeta;
}

export interface QuadUpdate {
  /** The quad, including the data after update */
  quad: JrqlSharedDataQuad,
  /** The operation */
  operation: unknown,
  /** The json-rql expression(s) used to perform the update */
  update: Expression | Expression[],
  /** Journaled metadata required to revert the operation */
  revert: unknown,
  /** Flag to collapse the data+oplog to a snapshot, required if voiding */
  snapshot?: true
}

/** Operation over quads, with attached metadata (interface is read-only) */
export interface JrqlQuadOperation extends Operation<JrqlQuad> {
  /** Metadata of shared datatype operations */
  updates: Iterable<QuadUpdate>;
}

export class JrqlPatchQuads extends PatchQuads implements JrqlQuadOperation {
  /**
   * Quad, having shared data, with operation on that data
   */
  readonly updates: QuadUpdate[] = [];

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

  addUpdateMeta(opMeta: QuadUpdate) {
    this.updates.push(opMeta);
  }

  get isEmpty(): boolean {
    return super.isEmpty && this.updates.length === 0;
  }

  private inheritMeta(patch: Partial<JrqlQuadOperation>) {
    if (patch.updates != null) {
      for (let opMeta of patch.updates)
        this.addUpdateMeta(opMeta);
    }
  }
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (isArray(results) ? results.includes(key) : results === key);
}