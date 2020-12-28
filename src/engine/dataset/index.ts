import {
  Quad, DefaultGraph, NamedNode, Quad_Subject,
  Quad_Predicate, Quad_Object, DataFactory
} from 'rdf-js';
import { Quadstore } from 'quadstore';
import { AbstractChainedBatch, AbstractLevelDOWN } from 'abstract-leveldown';
import { Observable } from 'rxjs';
import { generate as uuid } from 'short-uuid';
import { check, observeStream, Stopwatch } from '../util';
import { LockManager } from '../locks';
import { QuadSet } from '../quads';
import { Filter } from '../indices';
import dataFactory = require('@rdfjs/data-model');
import { BatchOpts, Binding, DefaultGraphMode, ResultType, TermName } from 'quadstore/dist/lib/types';
import { Context } from 'jsonld/jsonld-spec';
import { activeCtx, compactIri, expandTerm } from '../jsonld';
import { ActiveContext } from 'jsonld/lib/context';
import { Algebra } from 'sparqlalgebrajs';

/**
 * Atomically-applied patch to a quad-store.
 */
export interface Patch {
  readonly oldQuads: Quad[] | Partial<Quad>;
  readonly newQuads: Quad[];
}

/**
 * Requires that the oldQuads are concrete Quads and not a MatchTerms.
 */
export type DefinitePatch = { [key in keyof Patch]?: Iterable<Quad> };

/**
 * Specialised patch that allows concatenation.
 */
export class PatchQuads implements Patch, DefinitePatch {
  private readonly sets: { [key in keyof Patch]: QuadSet };

  constructor({ oldQuads = [], newQuads = [] }: DefinitePatch = {}) {
    this.sets = { oldQuads: new QuadSet(oldQuads), newQuads: new QuadSet(newQuads) };
    this.ensureMinimal();
  }

  get oldQuads() {
    return [...this.sets.oldQuads];
  }

  get newQuads() {
    return [...this.sets.newQuads];
  }

  get isEmpty() {
    return this.sets.newQuads.size === 0 && this.sets.oldQuads.size === 0;
  }

  append(patch: DefinitePatch) {
    this.sets.oldQuads.addAll(patch.oldQuads);
    this.sets.newQuads.addAll(patch.newQuads);
    this.ensureMinimal();
    return this;
  }

  remove(key: keyof Patch, quads: Iterable<Quad> | Filter<Quad>): Quad[] {
    return [...this.sets[key].deleteAll(quads)];
  }

  private ensureMinimal() {
    this.sets.newQuads.deleteAll(this.sets.oldQuads.deleteAll(this.sets.newQuads));
  }
}

export type GraphName = DefaultGraph | NamedNode;

/**
 * Writeable dataset. Transactions are atomically and serially applied.
 * Note that the patch created by a transaction can span Graphs - each
 * Quad in the patch will have a graph property.
 */
export interface Dataset {
  readonly location: string;
  readonly dataFactory: Required<DataFactory>;

  graph(name?: GraphName): Graph;

  /**
   * Ensures that write transactions are executed serially against the store.
   * @param prepare prepares a write operation to be performed
   */
  transact(txn: TxnOptions<TxnResult>): Promise<void>;
  transact<T>(txn: TxnOptions<TxnValueResult<T>>): Promise<T>;

  get(key: string): Promise<Buffer | undefined>;

  clear(): Promise<void>;

  close(): Promise<void>;
  readonly closed: boolean;
}

export type Kvps = // NonNullable<BatchOpts['preWrite']> with strong kv types
  (batch: AbstractChainedBatch<string, Buffer>) => Promise<unknown> | unknown;

export interface TxnResult {
  patch?: Patch;
  kvps?: Kvps;
  after?(): unknown | Promise<unknown>;
  return?: unknown;
}

export interface TxnValueResult<T> extends TxnResult {
  return: T
}

export interface TxnOptions<T extends TxnResult> extends Partial<TxnContext> {
  prepare(txc: TxnContext): Promise<T>;
}

export interface TxnContext {
  id: string,
  sw: Stopwatch
}

const notClosed = check((d: Dataset) => !d.closed, () => new Error('Dataset closed'));

/**
 * Read-only utility interface for reading Quads from a Dataset.
 */
export interface Graph {
  readonly name: GraphName;
  readonly dataFactory: Required<DataFactory>;

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Observable<Quad>;

  query(query: Algebra.Construct): Observable<Quad>;
  query(query: Algebra.Describe): Observable<Quad>;
  query(query: Algebra.Project): Observable<Binding>;
}

/**
 * Context for Quadstore dataset storage. Mix in with a domain context to
 * optimise (minimise) both control and user content.
 */
export const STORAGE_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/',
  xs: 'http://www.w3.org/2001/XMLSchema#',
  rdf: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
}

export class QuadStoreDataset implements Dataset {
  readonly location: string;
  private /* readonly */ store: Quadstore;
  private readonly activeCtx?: Promise<ActiveContext>;
  private readonly lock = new LockManager;
  private isClosed: boolean = false;

  constructor(private readonly backend: AbstractLevelDOWN, context?: Context) {
    // Internal of level-js and leveldown
    this.location = (<any>backend).location ?? uuid();
    this.activeCtx = activeCtx(Object.assign({}, STORAGE_CONTEXT, context));
  }

  async initialise(): Promise<QuadStoreDataset> {
    const activeCtx = await this.activeCtx;
    this.store = new Quadstore({
      backend: this.backend,
      dataFactory,
      indexes: [
        [TermName.GRAPH, TermName.SUBJECT, TermName.PREDICATE, TermName.OBJECT],
        [TermName.GRAPH, TermName.OBJECT, TermName.SUBJECT, TermName.PREDICATE],
        [TermName.GRAPH, TermName.PREDICATE, TermName.OBJECT, TermName.SUBJECT]
      ],
      prefixes: activeCtx == null ? undefined : {
        expandTerm: term => expandTerm(term, activeCtx),
        compactIri: iri => compactIri(iri, activeCtx)
      },
      defaultGraphMode: DefaultGraphMode.DEFAULT
    });
    await this.store.open();
    return this;
  }

  get dataFactory() {
    return <Required<DataFactory>>this.store.dataFactory;
  };

  graph(name?: GraphName): Graph {
    return new QuadStoreGraph(this.store, name || this.dataFactory.defaultGraph());
  }

  @notClosed.async
  transact<T, O extends TxnResult>(txn: TxnOptions<O>): Promise<T> {
    const id = txn.id ?? uuid();
    const sw = txn.sw ?? new Stopwatch('txn', id);
    // The transaction lock ensures that read operations that are part of a
    // transaction (e.g. evaluating the @where clause) are not affected by
    // concurrent transactions (fully serialiseable consistency). This is
    // particularly important for SU-Set operation.
    /*
    TODO: This could be improved with snapshots, if all the reads were on the
    same event loop tick, see https://github.com/Level/leveldown/issues/486
    */
    sw.next('lock-wait');
    return this.lock.exclusive('txn', async () => {
      sw.next('prepare');
      const result = await txn.prepare({ id, sw: sw.lap });
      sw.next('apply');
      if (result.patch != null)
        await this.applyQuads(result.patch, { preWrite: result.kvps });
      else if (result.kvps != null)
        await this.applyKvps(result.kvps);
      sw.stop();
      await result.after?.();
      return <T>result.return;
    });
  }

  private async applyKvps(kvps: Kvps) {
    const batch = this.store.db.batch();
    await kvps(batch);
    return new Promise<void>((resolve, reject) =>
      batch.write(err => err ? reject(err) : resolve()));
  }

  private async applyQuads(patch: Patch, opts?: BatchOpts) {
    if (Array.isArray(patch.oldQuads)) {
      await this.store.multiPatch(patch.oldQuads, patch.newQuads, opts);
    } else {
      // FIXME: Not atomic! – Rarely used
      const { subject, predicate, object, graph } = patch.oldQuads;
      await new Promise((resolve, reject) =>
        this.store.removeMatches(subject, predicate, object, graph)
          .on('end', resolve).on('error', reject));
      await this.store.multiPut(patch.newQuads, opts);
    }
  }

  @notClosed.async
  get(key: string): Promise<Buffer | undefined> {
    return new Promise<Buffer | undefined>((resolve, reject) =>
      this.store.db.get(key, { asBuffer: true }, (err, buf) => {
        if (err) {
          if (err.message.startsWith('NotFound'))
            resolve(undefined);
          else
            reject(err);
        } else {
          resolve(buf);
        }
      }));
  }

  @notClosed.async
  clear(): Promise<void> {
    return new Promise<void>((resolve, reject) =>
      this.store.db.clear(err => err ? reject(err) : resolve()));
  }

  @notClosed.async
  close(): Promise<void> {
    // Make efforts to ensure no transactions are running
    return this.lock.exclusive('txn', () => {
      this.isClosed = true;
      return this.store.close();
    });
  }

  get closed(): boolean {
    return this.isClosed;
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly store: Quadstore,
    readonly name: GraphName) {
  }

  get dataFactory() {
    return <Required<DataFactory>>this.store.dataFactory;
  };

  query(query: Algebra.Construct): Observable<Quad>;
  query(query: Algebra.Describe): Observable<Quad>;
  query(query: Algebra.Project): Observable<Binding>;
  query(query: Algebra.Project | Algebra.Describe | Algebra.Construct): Observable<Binding | Quad> {
    return observeStream(async () => {
      const stream = await this.store.sparqlStream(query);
      if (stream.type === ResultType.BINDINGS || stream.type === ResultType.QUADS)
        return stream.iterator;
      else
        throw new Error('Expected bindings or quads');
    })
  }

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Observable<Quad> {
    return observeStream(async () => this.store.match(subject, predicate, object, this.name));
  }
}