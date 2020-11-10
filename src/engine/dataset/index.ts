import { Quad, DefaultGraph, NamedNode, Quad_Subject, Quad_Predicate, Quad_Object } from 'rdf-js';
import { defaultGraph } from '@rdfjs/data-model';
import { RdfStore, MatchTerms } from 'quadstore';
import { AbstractLevelDOWN, AbstractOpenOptions } from 'abstract-leveldown';
import { Observable } from 'rxjs';
import { generate as uuid } from 'short-uuid';
import { check, Stopwatch } from '../util';
import { LockManager } from '../locks';
import { promisify } from 'util';
import { QuadSet } from '../quads';
import { Filter } from '../indices';

/**
 * Atomically-applied patch to a quad-store.
 */
export interface Patch {
  readonly oldQuads: Quad[] | MatchTerms<Quad>;
  readonly newQuads: Quad[];
}

/**
 * Specialised patch that allows concatenation.
 * Requires that the oldQuads are concrete Quads and not a MatchTerms.
 */
export class PatchQuads implements Patch {
  private readonly sets: { [key in keyof Patch]: QuadSet };

  constructor(
    oldQuads: Iterable<Quad> = [],
    newQuads: Iterable<Quad> = []) {
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

  append(patch: { [key in keyof Patch]?: Iterable<Quad> }) {
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

  graph(name?: GraphName): Graph;

  /**
   * Ensures that write transactions are executed serially against the store.
   * @param prepare prepares a write operation to be performed
   */
  transact(txn: TxnOptions<TxnResult>): Promise<void>;
  transact<T>(txn: TxnOptions<TxnValueResult<T>>): Promise<T>;

  close(): Promise<void>;
  readonly closed: boolean;
}

export interface TxnResult {
  patch?: Patch,
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

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Observable<Quad>;
}

export class QuadStoreDataset implements Dataset {
  readonly location: string;
  private readonly store: RdfStore;
  private readonly lock = new LockManager;
  private isClosed: boolean = false;

  constructor(
    private readonly leveldown: AbstractLevelDOWN,
    opts?: AbstractOpenOptions) {
    this.store = new RdfStore(leveldown, opts);
    // Internal of level-js and leveldown
    this.location = (<any>leveldown).location ?? uuid();
  }

  graph(name?: GraphName): Graph {
    return new QuadStoreGraph(this.store, name || defaultGraph());
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
        await this.store.patch(result.patch.oldQuads, result.patch.newQuads);
      sw.stop();
      await result.after?.();
      return <T>result.return;
    });
  }

  @notClosed.async
  close(): Promise<void> {
    // Make efforts to ensure no transactions are running
    return this.lock.exclusive('txn', () => {
      this.isClosed = true;
      return promisify(this.leveldown.close.bind(this.leveldown))();
    });
  }

  get closed(): boolean {
    return this.isClosed;
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly store: RdfStore,
    readonly name: GraphName) {
  }

  match(subject?: Quad_Subject, predicate?: Quad_Predicate, object?: Quad_Object): Observable<Quad> {
    return new Observable(subs => {
      try {
        // match can throw! (Bug in quadstore)
        this.store.match(subject, predicate, object, this.name)
          .on('data', quad => subs.next(quad))
          .on('error', err => subs.error(err))
          .on('end', () => subs.complete());
      } catch (err) {
        subs.error(err);
      }
    });
  }
}

