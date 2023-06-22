import {
  Bindings, DefaultGraph, NamedNode, Prefixes, Quad, Quad_Object, Quad_Predicate, Quad_Subject,
  QuadSet, QuadSource, RdfFactory, toBinding
} from '../quads';
import { BatchOpts, Quadstore } from 'quadstore';
import { AbstractChainedBatch, AbstractIteratorOptions, AbstractLevel } from 'abstract-level';
import { Observable } from 'rxjs';
import { LockManager } from '../locks';
import { Context, Iri } from '@m-ld/jsonld';
import { JsonldContext } from '../jsonld';
import { Algebra } from 'sparqlalgebrajs';
import { Engine } from 'quadstore-comunica';
import { JRQL, M_LD, RDF, XS } from '../../ns';
import async from '../async';
import { MutableOperation } from '../ops';
import { BaseStream, Binding, CountableRdf, QueryableRdfSource } from '../../rdfjs-support';
import { uuid } from '../../util';
import { Stopwatch } from '../Stopwatch';
import { check } from '../check';
import { Consumable } from 'rx-flowable';
import { MeldError } from '../../api';
import EventEmitter = require('events');

/**
 * Atomically-applied patch to a quad-store.
 */
export interface Patch {
  readonly deletes: Iterable<Quad> | Partial<Quad>;
  readonly inserts: Iterable<Quad>;
}

function isDeletePattern(deletes: Patch['deletes']): deletes is Partial<Quad> {
  return !(Symbol.iterator in deletes);
}

/**
 * Specialised patch that allows concatenation.
 */
export class PatchQuads extends MutableOperation<Quad> implements Patch {
  protected constructSet(quads?: Iterable<Quad>): QuadSet {
    return new QuadSet(quads);
  }
}

export type GraphName = DefaultGraph | NamedNode;

export interface KvpSet {
  /** Exact match kvp retrieval */
  get(key: string): Promise<Buffer | undefined>;
  /** Exact match kvp retrieval */
  has(key: string): Promise<boolean>;
  /** Kvp retrieval by range options */
  read(range: AbstractIteratorOptions<string, Buffer>): Consumable<[string, Buffer]>;
  /** Term storage compaction, if applicable */
  readonly prefixes: Prefixes;
}

export interface KvpStore extends KvpSet {
  /**
   * Ensures that write transactions are executed serially against the store.
   * @param txn prepares a write operation to be performed
   */
  transact<T = unknown>(txn: TxnOptions<KvpResult<T>>): Promise<T>;
  /** Transaction and state locking, can also be used for other locks */
  readonly lock: LockManager<'state' | 'txn' | string>;
}

export interface TripleKeyStore extends KvpStore {
  readonly rdf: RdfFactory;
}

/**
 * Writeable dataset. Transactions are atomically and serially applied.
 * Note that the patch created by a transaction can span Graphs - each
 * Quad in the patch will have a graph property.
 */
export interface Dataset extends TripleKeyStore {
  readonly location: string;

  graph(name?: GraphName): Graph;

  transact<T = unknown>(txn: TxnOptions<PatchResult<T>>): Promise<T>;

  clear(): Promise<void>;

  close(): Promise<void>;
  readonly closed: boolean;
}

export type KvpBatch = Pick<AbstractChainedBatch<any, string, Buffer>, 'put' | 'del'>;
export type Kvps = // NonNullable<BatchOpts['preWrite']> with strong kv types
  (batch: KvpBatch) => Promise<unknown> | unknown;

export interface TxnResult<T = unknown> {
  return?: T;
}

export interface KvpResult<T = unknown> extends TxnResult<T> {
  kvps?: Kvps;
}

export interface PatchResult<T = unknown> extends KvpResult<T> {
  patch?: Patch;
}

export interface TxnOptions<T extends KvpResult> extends Partial<TxnContext> {
  prepare(txc: TxnContext): T | Promise<T>;
  lock?: string;
}

export interface TxnContext {
  id: string,
  sw: Stopwatch
  on(state: 'commit', handler: () => unknown | Promise<unknown>): this;
  on(state: 'rollback', handler: (err: any) => unknown | Promise<unknown>): this;
}

const notClosed = check((d: Dataset) => !d.closed,
  // m-ld-specific error used here to simplify exception handling
  () => new MeldError('Clone has closed'));

/**
 * Read-only utility interface for reading Quads from a Dataset.
 */
export interface Graph extends QueryableRdfSource, KvpSet {
  readonly name: GraphName;
  readonly lock: LockManager<'state'>;
  readonly rdf: RdfFactory;

  quad(subject: Quad_Subject, predicate: Quad_Predicate, object: Quad_Object): Quad;

  query(...args: Parameters<QuadSource['match']>): async.AsyncIterator<Quad>;
  query(query: Algebra.Construct): async.AsyncIterator<Quad>;
  query(query: Algebra.Describe): async.AsyncIterator<Quad>;
  query(query: Algebra.Project): async.AsyncIterator<Binding>;
  query(query: Algebra.Distinct): async.AsyncIterator<Binding>;

  ask(query: Algebra.Ask): Promise<boolean>;
}

/**
 * Default mapping of a domain name to a base URL
 * @param domain IETF domain name
 */
export function domainBase(domain: string) {
  if (!/^[a-z0-9_]+([\-.][a-z0-9_]+)*\.[a-z]{2,6}$/.test(domain))
    throw new RangeError('Domain not specified or not valid');
  return `http://${domain}/`;
}

/**
 * Default vocabulary IRI for a given base IRI. Note this ties the vocabulary in
 * use to the base which precludes sharing of data; so should only be used as a
 * default and not for linked data applications.
 * @param base base IRI
 */
export function baseVocab(base?: Iri) {
  return new URL('/#', base).href;
}

/**
 * Context for Quadstore dataset storage. Mix in with a domain context to
 * optimise (minimise) both control and user content.
 */
const STORAGE_CONTEXT: Context = {
  jrql: JRQL.$base,
  mld: M_LD.$base,
  xs: XS.$base,
  rdf: RDF.$base
};

export class QuadStoreDataset implements Dataset {
  readonly location: string;
  /* readonly */
  store: Quadstore;
  engine: Engine;
  prefixes: Prefixes;
  rdf: RdfFactory;
  private readonly activeCtx: Promise<JsonldContext>;
  readonly lock = new LockManager;
  private isClosed: boolean = false;

  constructor(
    domain: string,
    private readonly backend: AbstractLevel<any>,
    private readonly events?: EventEmitter
  ) {
    // Internal of ClassicLevel and BrowserLevel
    this.location = (<any>backend).location ?? uuid();
    this.rdf = new RdfFactory(domainBase(domain));
    this.activeCtx = JsonldContext.active({
      '@base': this.rdf.base,
      '@vocab': baseVocab(this.rdf.base),
      ...STORAGE_CONTEXT
    });
  }

  async initialise(sw?: Stopwatch): Promise<QuadStoreDataset> {
    sw?.next('active-context');
    const activeCtx = await this.activeCtx;
    sw?.next('open-store');
    this.prefixes = {
      expandTerm: term => activeCtx.expandTerm(term),
      compactIri: iri => activeCtx.compactIri(iri)
    };
    this.store = new Quadstore({
      backend: this.backend,
      dataFactory: this.rdf,
      indexes: [
        ['graph', 'subject', 'predicate', 'object'],
        ['graph', 'object', 'subject', 'predicate'],
        ['graph', 'predicate', 'object', 'subject']
      ],
      prefixes: this.prefixes
    });
    this.engine = new Engine(this.store);
    await this.store.open();
    return this;
  }

  graph(name?: GraphName): Graph {
    return new QuadStoreGraph(this, name || this.rdf.defaultGraph());
  }

  @notClosed.async
  transact<T, O extends PatchResult>(txn: TxnOptions<O>): Promise<T> {
    const lockKey = txn.lock ?? 'txn';
    const id = txn.id ?? uuid();
    const sw = txn.sw ?? new Stopwatch(lockKey, id);
    const txc = new class extends EventEmitter implements TxnContext {
      sw: Stopwatch;
      id = id;
    }();
    // The transaction lock ensures that read operations that are part of a
    // transaction (e.g. evaluating the @where clause) are not affected by
    // concurrent transactions (fully serialisable consistency). This is
    // particularly important for SU-Set operation.
    /*
    TODO: This could be improved with snapshots, if all the reads were on the
    same event loop tick, see https://github.com/Level/classic-level/issues/28
    */
    sw.next('lock-wait');
    return this.lock.exclusive(lockKey, 'transaction', async () => {
      sw.next('prepare');
      txc.sw = sw.lap;
      const result = await txn.prepare(txc);
      sw.next('apply');
      if (result.patch != null) {
        await this.applyQuads(result.patch, {
          preWrite: batch => result.kvps?.(this.kvpBatch(batch))
        });
      } else if (result.kvps != null) {
        await this.applyKvps(result.kvps);
      }
      sw.next('after');
      txc.removeAllListeners('rollback'); // Fail in commit != rollback
      await Promise.all(txc.listeners('commit').map(fn => fn()));
      sw.stop();
      return <T>result.return;
    }).then(result => {
      this.events?.emit('commit', txn.id);
      return result;
    }, async err => {
      await Promise.all(txc.listeners('rollback').map(fn => fn(err)));
      this.events?.emit('error', err);
      throw err;
    });
  }

  private async applyKvps(kvps: Kvps) {
    const batch = this.store.db.batch();
    await kvps(this.kvpBatch(batch));
    return batch.write();
  }

  private kvpBatch(batch: ReturnType<Quadstore['db']['batch']>): KvpBatch {
    return {
      put(key: string, value: Buffer, options?: object) {
        batch.put(key, value, { valueEncoding: 'buffer', ...options });
        return this;
      },
      del(key: string) {
        batch.del(key);
        return this;
      }
    };
  }

  private async applyQuads(patch: Patch, opts?: BatchOpts) {
    if (!isDeletePattern(patch.deletes)) {
      await this.store.multiPatch([...patch.deletes], [...patch.inserts], opts);
    } else {
      // FIXME: Not atomic! – Rarely used
      const { subject, predicate, object, graph } = patch.deletes;
      await new Promise((resolve, reject) =>
        this.store.removeMatches(subject, predicate, object, graph)
          .on('end', resolve).on('error', reject));
      await this.store.multiPut([...patch.inserts], opts);
    }
  }

  @notClosed.async
  get(key: string): Promise<Buffer | undefined> {
    return new Promise<Buffer | undefined>((resolve, reject) =>
      this.store.db.get(key, { valueEncoding: 'buffer' }, (err, buf) => {
        if (err) {
          if (err.message.startsWith('NotFound')) {
            resolve(undefined);
          } else {
            reject(err);
            this.events?.emit('error', err);
          }
        } else {
          resolve(buf);
        }
      }));
  }

  @notClosed.async
  has(key: string): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      const it = this.store.db.iterator({
        gte: key, limit: 1, values: false, keyEncoding: 'utf8'
      });
      it.next((err, foundKey: string) => {
        if (err) {
          reject(err);
          this.events?.emit('error', err);
        } else if (foundKey != null) {
          resolve(foundKey === key);
        } else {
          resolve(false);
        }
        this.end(it);
      });
    });
  }

  @notClosed.rx
  read(range: AbstractIteratorOptions<string, Buffer>): Consumable<[string, Buffer]> {
    return new Observable(subs => {
      const options = { ...range, keyEncoding: 'utf8', valueEncoding: 'buffer' };
      const values = range.values !== false;
      const it = values ? this.store.db.iterator(options) : this.store.db.keys(options);
      const errored = (err: any) => {
        subs.error(err);
        this.events?.emit('error', err);
      };
      const pull = () => {
        if (!subs.closed) {
          // Calling next on an iterator that is already ended e.g. by closing the
          // store, may cause a truly evil exception which kills the process
          if (this.closed) {
            errored(new MeldError('Clone has closed'));
          } else {
            it.next((err: any, key: string, value: Buffer) => {
              if (err) {
                errored(err);
              } else if (key != null && (!values || value != null)) {
                subs.next({ value: [key, value], next: pull });
              } else {
                subs.complete();
              }
            });
          }
        }
        return true as true;
      };
      pull();
      return () => this.end(it);
    });
  }

  @notClosed.async
  clear(): Promise<void> {
    return new Promise<void>((resolve, reject) =>
      this.store.db.clear(err => {
        if (err) {
          reject(err);
          this.events?.emit('error', err);
        } else {
          resolve();
          this.events?.emit('clear');
        }
      }));
  }

  @notClosed.async
  close(): Promise<void> {
    // Make efforts to ensure no transactions are running
    return this.lock.exclusive('txn', 'closing', () => {
      this.isClosed = true;
      return this.store.close();
    });
  }

  get closed(): boolean {
    return this.isClosed;
  }

  private end(it: { close(): Promise<void> }) {
    it.close().catch(err => err && this.events?.emit('error', err));
  }
}

class QuadStoreGraph implements Graph {
  constructor(
    readonly dataset: QuadStoreDataset,
    readonly name: GraphName
  ) {}

  get lock() {
    return this.dataset.lock;
  }

  get prefixes() {
    return this.dataset.prefixes;
  }

  get rdf() {
    return this.dataset.rdf;
  }

  get(key: string): Promise<Buffer | undefined> {
    return this.dataset.get(key);
  }

  has(key: string): Promise<boolean> {
    return this.dataset.has(key);
  }

  read(range: AbstractIteratorOptions<string, Buffer>): Consumable<[string, Buffer]> {
    return this.dataset.read(range);
  }

  match: Graph['match'] = (subject, predicate, object, graph) => {
    if (graph != null && !graph.equals(this.name))
      return async.empty();
    else
      // Must specify graph term due to optimised indexing
      return (<QuadSource>this.dataset.store).match(subject, predicate, object, this.name);
  };

  countQuads: Graph['countQuads'] = async (subject, predicate, object, graph) => {
    if (graph != null && !graph.equals(this.name))
      return 0;
    else
      // Must specify graph term due to optimised indexing
      return (<CountableRdf>this.dataset.store).countQuads(subject, predicate, object, this.name);
  };

  query(...args: Parameters<QuadSource['match']>): async.AsyncIterator<Quad>;
  query(query: Algebra.Construct): async.AsyncIterator<Quad>;
  query(query: Algebra.Describe): async.AsyncIterator<Quad>;
  query(query: Algebra.Project): async.AsyncIterator<Binding>;
  query(query: Algebra.Distinct): async.AsyncIterator<Binding>;
  query(
    ...args: Parameters<QuadSource['match']> | [Algebra.Operation]
  ): async.AsyncIterator<Binding | Quad> {
    const source: Promise<BaseStream<Quad | Bindings>> = (async () => {
      try {
        const [algebra] = args;
        if (algebra != null && 'type' in algebra) {
          const query = await this.dataset.engine.query(algebra);
          if (query.resultType === 'bindings' || query.resultType === 'quads')
            return query.execute();
        } else {
          return this.match(...<Parameters<QuadSource['match']>>args);
        }
      } catch (err) {
        // TODO: Comunica bug? Cannot read property 'close' of undefined, if stream empty
        if (err instanceof TypeError)
          return new async.EmptyIterator<Quad | Bindings>();
        throw err;
      }
      throw new Error('Expected bindings or quads');
    })();
    return new async.SimpleTransformIterator<Bindings | Quad, Binding | Quad>(
      // A transform iterator is actually capable of taking a base stream, despite typings
      this.dataset.lock.extend('state', 'query', <any>source), {
        map: item => ('type' in item && item.type === 'bindings') ? toBinding(item) : <Quad>item
      });
  }

  ask(algebra: Algebra.Ask): Promise<boolean> {
    return this.dataset.engine.queryBoolean(algebra);
  }

  quad = (subject: Quad_Subject, predicate: Quad_Predicate, object: Quad_Object) =>
    this.rdf.quad(subject, predicate, object, this.name);
}