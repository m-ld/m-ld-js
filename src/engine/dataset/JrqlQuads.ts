import { Graph, KvpBatch, PatchQuads } from '.';
import { blank, Datatype, GraphSubject, isSharedDatatype } from '../../api';
import { Atom, Expression, Result, Subject, Value } from '../../jrql-support';
import {
  inPosition,
  isLiteralTriple,
  isTypedTriple,
  LiteralTriple,
  Quad,
  Quad_Object,
  Term,
  Triple,
  tripleKey
} from '../quads';
import { JRQL } from '../../ns';
import { SubjectGraph } from '../SubjectGraph';
import { JrqlMode, toIndexDataUrl } from '../jrql-util';
import { clone, concatIter, IndexKeyGenerator, isArray, mapIter, mapObject } from '../util';
import { JrqlContext, SubjectQuads } from '../SubjectQuads';
import { Binding } from '../../rdfjs-support';
import * as MsgPack from '../msgPack';
import { Operation } from '../ops';
import { takeWhile } from 'rxjs/operators';
import { drain } from 'rx-flowable';

export class JrqlQuads {
  constructor(
    readonly graph: Graph
  ) {}

  async solutionSubject(
    results: Result[] | Result,
    solution: Binding,
    ctx: JrqlContext
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

  in(mode: JrqlMode, ctx: JrqlContext) {
    return new SubjectQuads(this.rdf, mode, ctx);
  }

  toQuads(
    subjects: Subject | Subject[],
    mode: JrqlMode,
    ctx: JrqlContext
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
    ctx: JrqlContext
  ): Promise<GraphSubject> {
    await Promise.all(propertyQuads.map(quad => this.loadData(quad, ctx)));
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

  toObjectTerm(value: Atom, ctx: JrqlContext): Quad_Object {
    return new SubjectQuads(this.rdf, JrqlMode.match, ctx).objectTerm(value);
  }

  async applyTripleUpdate(
    triple: Quad,
    update: Expression,
    ctx: JrqlContext
  ): Promise<UpdateMeta | undefined> {
    if (isLiteralTriple(triple)) {
      const datatype = ctx.getDatatype(triple.object.datatype.value);
      // TODO: Bug: what if the datatype is no longer shared?
      if (datatype != null && isSharedDatatype(datatype)) {
        await this.loadDataOfType(triple, datatype);
        if (isTypedTriple(triple)) {
          const [data, operation, revert] =
            datatype.update(triple.object.typed.data, update);
          triple.object.typed.data = data; // In case immutable
          return { operation, update, revert };
        }
      }
    }
  }

  async applyTripleOperation(
    triple: Quad,
    operation: unknown,
    ctx: JrqlContext
  ): Promise<UpdateMeta | undefined> {
    if (isLiteralTriple(triple)) {
      await this.loadData(triple, ctx);
      if (isTypedTriple(triple) && isSharedDatatype(triple.object.typed.type)) {
        // Shared datatypes have UUID values, so the type should be correct
        const [data, update, revert] = triple.object.typed.type.apply(
          triple.object.typed.data, operation);
        triple.object.typed.data = data; // In case immutable
        return { operation, update, revert };
      }
    }
  }

  loadHasData(triples: Iterable<JrqlQuad>, ctx: JrqlContext) {
    return Promise.all(mapIter(triples, async triple => {
      if (isLiteralTriple(triple) && triple.hasData == null) {
        const datatype = ctx.getDatatype(triple.object.datatype.value);
        if (datatype != null) {
          const keys = await this.loadDataAndOps(triple, { values: false });
          if (keys.length > 0) {
            return triple.hasData = {
              shared: isSharedDatatype(datatype),
              ticks: keys.slice(1).map(([key]) => DATA_KEY_GEN.tickFrom(key))
            };
          }
        }
      }
      triple.hasData = false;
    }));
  }

  private loadDataAndOps(triple: LiteralTriple, opts: { values?: false } = {}) {
    const tripleKey = DATA_KEY_GEN.keyFor(triple, this.graph);
    return drain(this.graph.read({ gte: tripleKey, ...opts }).pipe(
      takeWhile(({ value: [key] }) => key.startsWith(tripleKey))));
  }

  async loadData(triple: Triple, ctx: JrqlContext) {
    if (isLiteralTriple(triple)) {
      const datatype = ctx.getDatatype(triple.object.datatype.value);
      if (datatype != null)
        await this.loadDataOfType(triple, datatype);
    }
  }

  private async loadDataOfType(triple: LiteralTriple, datatype: Datatype) {
    // TODO: Allow for datatype caching
    const keyValues = await this.loadDataAndOps(triple);
    if (keyValues.length > 0) {
      const [snapshot, ...ops] = keyValues.map(([, data]) => MsgPack.decode(data));
      let data = datatype.fromJSON ? datatype.fromJSON(snapshot) : snapshot;
      if (isSharedDatatype(datatype)) {
        for (let op of ops)
          [data] = datatype.apply(data, op);
      } else if (ops.length > 0) {
        throw new Error('Operations found for a non-shared datatype');
      }
      triple.object.typed = { type: datatype, data };
    }
  }

  saveData(patch: JrqlQuadOperation, batch: KvpBatch, tick?: number) {
    for (let triple of concatIter(patch.deletes, patch.inserts)) {
      if (triple.hasData) {
        batch.del(DATA_KEY_GEN.keyFor(triple, this.graph));
        if (triple.hasData.shared) {
          for (let tick of triple.hasData.ticks)
            batch.del(DATA_KEY_GEN.keyFor(triple, this.graph, tick));
        }
      }
    }
    for (let quad of patch.inserts) {
      if (isTypedTriple(quad)) {
        const { type: datatype, data } = quad.object.typed;
        const json = datatype.toJSON ? datatype.toJSON(data) : data;
        batch.put(DATA_KEY_GEN.keyFor(quad, this.graph), MsgPack.encode(json));
      }
    }
    for (let [quad, { operation }] of patch.updates) {
      if (tick == null)
        throw new RangeError('Saving shared data operations requires a tick');
      batch.put(
        // FIXME: If the same triple appears twice!
        DATA_KEY_GEN.keyFor(quad, this.graph, tick),
        MsgPack.encode(operation)
      );
    }
  }
}

const DATA_KEY_PRE = '_qs:dat:';
const DATA_KEY_GEN = new class extends IndexKeyGenerator {
  keyFor(triple: Triple, graph: Graph, tick?: number) {
    /** Prefix for data keys */
    const suffix = tick == null ? '' : `,${this.key(tick)}`;
    return `${DATA_KEY_PRE}${tripleKey(triple, graph.prefixes)}${suffix}`;
  }

  tickFrom(key: string) {
    return this.index(key.slice(-this.length));
  }
}();

export interface JrqlQuad extends Quad {
  /**
   * Does this triple have attached data? If so, and the data is shared, lists
   * the operation ticks found.
   */
  hasData?: false | { shared: false } | { shared: true, ticks: number[] };
}

export interface UpdateMeta {
  /** The operation */
  operation: unknown,
  /** The json-rql expression used to perform the update */
  update: Expression,
  /** Journaled metadata required to revert the operation */
  revert: unknown
}

/** Operation over quads, with attached metadata (interface is read-only) */
export interface JrqlQuadOperation extends Operation<JrqlQuad> {
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