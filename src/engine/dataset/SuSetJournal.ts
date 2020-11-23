import { Subject } from '../../jrql-support';
import { fromPrefixedId, qsName, SUSET_CONTEXT, toPrefixedId } from './SuSetGraph';
import { Iri } from 'jsonld/jsonld-spec';
import { UUID, MeldDelta, EncodedDelta } from '..';
import { TreeClock } from '../clocks';
import { PatchQuads, Patch, Dataset } from '.';
import { JrqlGraph } from './JrqlGraph';
import { MsgPack } from '../util';

interface JournalJson extends Subject {
  '@id': 'qs:journal', // Singleton object
  body: string; // JSON-encoded body
}

interface JournalBody {
  tail: JournalEntryJson['@id'];
  time: any; // JSON-encoded TreeClock (the local clock)
}

/**
 * Journal entries are queryable by ticks. Otherwise they bundle properties into
 * a body for storage, to minimise triple count.
 */
interface JournalEntryJson extends Subject {
  /** entry:<encoded block hash> */
  '@id': Iri;
  /**
   * Base64-encoded message-packed body
   */
  body: string;
  /**
   * Local clock ticks for which this entry was the Journal tail. This can
   * include ticks not associated with any message, such as forking of the
   * process clock.
   */
  ticks: number | number[];
}

interface JournalEntryBody {
  /** Raw delta - may contain Buffers */
  delta: EncodedDelta;
  /** JSON-encoded transaction message time */
  time: any;
  /**
   * JSON-encoded public clock time ('global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike the message time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal ticks
   * for the local clone identity. This clock has no identity.
   */
  gwc: any;
}

export interface EntryCreateDetails {
  delta: MeldDelta;
  localTime: TreeClock;
  remoteTime?: TreeClock;
}

/** Immutable */
export class SuSetJournalEntry {
  constructor(
    private readonly graph: SuSetJournalGraph,
    private readonly json: JournalEntryJson,
    private readonly body: JournalEntryBody = SuSetJournalEntry.decodeBody(json.body),
    readonly id: Iri = json['@id'],
    readonly seq: number = SuSetJournalEntry.seq(id),
    readonly time = TreeClock.fromJson(body.time) as TreeClock, // null iff temporary head
    readonly gwc = TreeClock.fromJson(body.gwc) as TreeClock, // null iff temporary head
    readonly delta = body.delta) { // undefined iff head
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    return this.graph.entry(SuSetJournalEntry.id(this.seq + 1));
  }

  static headJson(localTime?: TreeClock, gwc?: TreeClock):
    Partial<JournalEntryJson> & Subject {
    const headEntryId = SuSetJournalEntry.id(0);
    const body: Partial<JournalEntryBody> = {
      time: localTime?.toJson(),
      gwc: gwc?.toJson()
    };
    const entry: Partial<JournalEntryJson> = {
      '@id': headEntryId,
      body: SuSetJournalEntry.encodeBody(body),
      ticks: localTime?.ticks
    };
    return entry;
  }

  async setHeadTime(localTime: TreeClock): Promise<PatchQuads> {
    if (this.body.delta != null)
      throw new Error('Trying to set head time for a non-head entry');
    // The dummy head time will only ever be used for a genesis clone, as all
    // other clones will have their journal reset with a snapshot. So, it's safe
    // to use the local time as the gwc, which is needed for subsequent entries.
    const patchBody = await this.updateBody({
      time: localTime.toJson(), gwc: localTime.scrubId().toJson()
    });
    const patchTicks = await this.graph.insert({
      '@id': SuSetJournalEntry.id(this.seq), ticks: localTime.ticks
    });
    return patchBody.append(patchTicks);
  }

  builder(journal: SuSetJournal, entry: EntryCreateDetails) {
    const graph = this.graph;
    class EntryBuild {
      seq: number;
      gwc: TreeClock;
      time: TreeClock;
      next?: EntryBuild;
      constructor(prevSeq: number, prevGwc: TreeClock, readonly details: EntryCreateDetails) {
        this.seq = prevSeq + 1;
        this.time = details.remoteTime ?? details.localTime;
        this.gwc = prevGwc.update(this.time);
      }
      *build(): Iterable<SuSetJournalEntry> {
        const body: JournalEntryBody = {
          time: this.time.toJson(),
          gwc: this.gwc.toJson(),
          delta: this.details.delta.encoded, // may contain buffers
        };
        const id = SuSetJournalEntry.id(this.seq);
        yield new SuSetJournalEntry(graph, {
          '@id': id,
          body: SuSetJournalEntry.encodeBody(body),
          ticks: this.details.localTime.ticks
        }, body, id, this.seq, this.time, this.gwc);
        if (this.next)
          yield* this.next.build();
      }
    }
    let head = new EntryBuild(this.seq, this.gwc, entry), tail = head;
    const builder = {
      /**
       * Adds another journal entry to this builder
       */
      next: (entry: EntryCreateDetails) => {
        tail = tail.next = new EntryBuild(tail.seq, tail.gwc, entry);
      },
      /**
       * Commits the built journal entries to the journal
       */
      commit: async () => {
        const entries = Array.from(head.build());
        const patch = await this.graph.insert(entries.map(entry => entry.json));
        patch.append(await journal.commit(entries.slice(-1)[0], tail.details.localTime));
        return patch;
      }
    };
    return builder;
  }

  private updateBody(update: Partial<JournalEntryBody>): Promise<PatchQuads> {
    return this.graph.write({
      '@delete': { '@id': this.id, body: this.json.body },
      '@insert': { '@id': this.id, body: SuSetJournalEntry.encodeBody({ ...this.body, ...update }) }
    });
  }

  private static encodeBody(body: Partial<JournalEntryBody>): string {
    return MsgPack.encode(body).toString('base64');
  }

  private static decodeBody(encoded: string): JournalEntryBody {
    let body: JournalEntryBody;
    try {
      body = MsgPack.decode(Buffer.from(encoded, 'base64'));
    } catch (err) {
      // This might be a v0.2 format
      const parsed = JSON.parse(encoded);
      if (typeof parsed.delta?.tid == 'string')
        body = { ...parsed, delta: [0, parsed.delta.tid, parsed.delta.delete, parsed.delta.insert] };
      else
        throw err;
    }
    if (body.gwc == null && 'remote' in <any>body) {
      // Prior versions with remote flag had no gwc. Use scrubbed time.
      body.gwc = (TreeClock.fromJson(body.time) as TreeClock).scrubId().toJson();
    }
    return body;
  }

  private static id(seq: number): Iri {
    return toPrefixedId('entry', seq.toString());
  }

  private static seq(id: Iri): number {
    return Number(fromPrefixedId('entry', id)[0]);
  }
}

export class SuSetJournal {
  /** Tail state cache */
  _tail: SuSetJournalEntry | null = null;

  constructor(
    private readonly graph: SuSetJournalGraph,
    private readonly json: JournalJson,
    private readonly body: JournalBody = JSON.parse(json.body),
    readonly time = body.time != null ? TreeClock.fromJson(body.time) : null) {
  }

  get safeTime() {
    if (this.time == null)
      throw new Error('Journal time not available');
    return this.time;
  }

  async tail(): Promise<SuSetJournalEntry> {
    if (this._tail == null) {
      if (this.body.tail == null)
        throw new Error('Journal has no tail yet');
      this._tail = await this.graph.entry(this.body.tail) ?? null;
      if (this._tail == null)
        throw new Error('Journal tail is missing');
    }
    return this._tail;
  }

  async findEntry(ticks: number) {
    const foundId = await this.graph.find1<JournalEntryJson>({ ticks });
    return foundId ? this.graph.entry(foundId) : null;
  }

  async setLocalTime(localTime: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const patch = await this.update(SuSetJournal.jsonWith(
      { ...this.body, time: localTime.toJson() }));
    if (newClone) {
      // For a new clone, the journal's dummy tail does not have a timestamp
      const tail = await this.tail();
      patch.append(await tail.setHeadTime(localTime));
    } else {
      // This time might be seen by the outside world, so ensure that the tail
      // entry is marked as covering it
      patch.append(await this.graph.insert({
        '@id': this.body.tail, ticks: localTime.ticks
      }));
    }
    // Not updating caches for rare time update
    this.graph._journal = null;
    this._tail = null;
    return patch;
  }

  static initJson(headEntry: Partial<JournalEntryJson>, localTime?: TreeClock): JournalJson {
    const body: Partial<JournalBody> = { tail: headEntry['@id'] };
    if (localTime != null)
      body.time = localTime.toJson();
    return { '@id': 'qs:journal', body: JSON.stringify(body) };
  }

  /**
   * Commits a new tail and time, with updates to the journal and tail cache
   */
  async commit(tail: SuSetJournalEntry, localTime: TreeClock): Promise<PatchQuads> {
    const body: JournalBody = { tail: tail.id, time: localTime.toJson() };
    const json = SuSetJournal.jsonWith(body);
    this.graph._journal = new SuSetJournal(this.graph, json, body, localTime);
    this.graph._journal._tail = tail;
    return this.update(json);
  }

  private update(json: JournalJson): Promise<PatchQuads> {
    return this.graph.write({ '@delete': this.json, '@insert': json });
  }

  private static jsonWith(body: JournalBody): JournalJson {
    return { '@id': 'qs:journal', body: JSON.stringify(body) };
  }
}

export class SuSetJournalGraph extends JrqlGraph {
  /** Journal state cache */
  _journal: SuSetJournal | null = null;

  constructor(dataset: Dataset) {
    // Named graph for control quads i.e. Journal (name is legacy)
    super(dataset.graph(qsName('control')), SUSET_CONTEXT);
  }

  async initialise(): Promise<Patch | undefined> {
    // Create the Journal if not exists
    const journal = await this.describe1<JournalJson>('qs:journal');
    if (journal == null)
      return this.reset();
  }

  async reset(gwc?: TreeClock, localTime?: TreeClock): Promise<Patch> {
    const head = SuSetJournalEntry.headJson(localTime, gwc);
    const journal = SuSetJournal.initJson(head, localTime);
    const insert = await this.insert([journal, head]);
    this._journal = null; // Not caching one-time change
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  async journal() {
    if (this._journal == null) {
      const json = await this.describe1<JournalJson>('qs:journal');
      if (json == null)
        throw new Error('Missing journal');
      this._journal = new SuSetJournal(this, json);
    }
    return this._journal;
  }

  async entry(id: JournalEntryJson['@id']) {
    const entry = await this.describe1<JournalEntryJson>(id);
    if (entry != null)
      return new SuSetJournalEntry(this, entry);
  }
}
