import { Subject } from '../../jrql-support';
import { toPrefixedId } from './SuSetGraph';
import { Iri } from 'jsonld/jsonld-spec';
import { UUID, MeldDelta, EncodedDelta } from '..';
import { JsonDeltaBagBlock } from '../MeldEncoding';
import { TreeClock } from '../clocks';
import { PatchQuads, Patch } from '.';
import { JrqlGraph } from './JrqlGraph';
import { Hash } from '../hash';
import { MsgPack } from '../util';

interface Journal {
  '@id': 'qs:journal', // Singleton object
  body: string; // JSON-encoded body
}

interface JournalBody {
  tail: JournalEntry['@id'];
  time: any; // JSON-encoded TreeClock (the local clock)
}

/**
 * Journal entries are queryable by ticks. Otherwise they bundle properties into
 * a body for storage, to minimise triple count.
 */
interface JournalEntry {
  /** entry:<encoded block hash> */
  '@id': Iri;
  /**
   * Base64-encoded message-packed body
   * FIXME: This will get converted back to binary by encoding-down
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
  /** Transaction ID */
  tid: UUID;
  /** Encoded Hash */
  hash: string;
  /** Raw delta - may contain Buffers */
  delta: EncodedDelta;
  /** JSON-encoded transaction message time */
  time: any;
  /**
   * JSON-encoded public clock time ('Global wall clock' or 'Great Westminster
   * Clock'). This has latest public ticks seen for all processes (not internal
   * ticks), unlike the message time, which may be causally related to older
   * messages from third parties, and the journal time, which has internal
   * ticks. The clock has no identity.
   */
  gwc: any;
  /** Next entry in the linked list */
  next?: JournalEntry['@id'];
}

export interface EntryCreateDetails {
  delta: MeldDelta;
  localTime: TreeClock;
  remoteTime?: TreeClock;
}

/** Immutable */
export class SuSetJournalEntry {
  private readonly body: JournalEntryBody;

  constructor(
    private readonly journal: SuSetJournal,
    private readonly data: JournalEntry) {
    try {
      this.body = MsgPack.decode(Buffer.from(data.body, 'base64'));
    } catch (err) {
      // This might be a v0.2 format
      const body = JSON.parse(data.body);
      if (typeof body.delta?.tid == 'string')
        this.body = { ...body, delta: [0, body.delta.tid, body.delta.delete, body.delta.insert] };
      else
        throw err;
    }
  }

  get id(): Iri {
    return this.data['@id'];
  }

  get hash(): Hash {
    return Hash.decode(this.body.hash);
  }

  get time(): TreeClock {
    return TreeClock.fromJson(this.body.time) as TreeClock;
  }

  get gwc(): TreeClock {
    return TreeClock.fromJson(this.body.gwc) as TreeClock;
  }

  get delta(): EncodedDelta {
    return this.body.delta;
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.body.next)
      return this.journal.entry(this.body.next);
  }

  static headEntry(hash: Hash, localTime?: TreeClock, gwc?: TreeClock): Subject {
    const encodedHash = hash.encode();
    const headEntryId = SuSetJournalEntry.id(encodedHash);
    const body: Partial<JournalEntryBody> = { hash: encodedHash };
    if (gwc != null)
      body.gwc = gwc.toJson();
    const entry: Partial<JournalEntry> = {
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
    const patchTicks = await this.journal.graph.insert({ '@id': this.id, ticks: localTime.ticks });
    return patchBody.append(patchTicks);
  }

  buildPatch(journal: SuSetJournalState, entry: EntryCreateDetails) {
    class EntryCreate {
      next?: EntryCreate;
      hash: Hash;
      gwc: TreeClock;
      time: TreeClock;
      constructor(prevHash: Hash, prevGwc: TreeClock, readonly details: EntryCreateDetails) {
        this.hash = SuSetJournalEntry.nextHash(prevHash, details.delta);
        this.time = details.remoteTime ?? details.localTime;
        this.gwc = prevGwc.update(this.time);
      }
      *create(): Iterable<JournalEntry & Subject> {
        const encodedHash = this.hash.encode();
        const body: JournalEntryBody = {
          hash: encodedHash,
          tid: this.details.delta.tid,
          time: this.time.toJson(),
          gwc: this.gwc.toJson(),
          delta: this.details.delta.encoded, // may contain buffers
        };
        if (this.next != null)
          body.next = SuSetJournalEntry.id(this.next.hash.encode());
        yield {
          '@id': SuSetJournalEntry.id(encodedHash),
          body: SuSetJournalEntry.encodeBody(body),
          ticks: this.details.localTime.ticks
        };
        if (this.next)
          yield* this.next.create();
      }
    }
    let head = new EntryCreate(this.hash, this.gwc, entry), tail = head;
    const builder = {
      next: (entry: EntryCreateDetails) => {
        tail = tail.next = new EntryCreate(tail.hash, tail.gwc, entry);
      },
      build: async () => {
        const entries = Array.from(head.create());
        const patch = await this.updateBody({ next: entries[0]['@id'] });
        patch.append(await this.journal.graph.insert(entries));
        patch.append(await journal.setTail(entries.slice(-1)[0]['@id'], tail.details.localTime));
        return patch;
      }
    };
    return builder;
  }

  private updateBody(update: Partial<JournalEntryBody>): Promise<PatchQuads> {
    return this.journal.graph.write({
      '@delete': { '@id': this.id, body: this.data.body },
      '@insert': { '@id': this.id, body: SuSetJournalEntry.encodeBody({ ...this.body, ...update }) }
    });
  }

  private static encodeBody(body: Partial<JournalEntryBody>): string {
    return MsgPack.encode(body).toString('base64');
  }

  private static id(encodedHash: string) {
    return toPrefixedId('entry', encodedHash);
  }

  private static nextHash(prevHash: Hash, delta: MeldDelta) {
    return new JsonDeltaBagBlock(prevHash).next(delta.encoded).id;
  }
}

export class SuSetJournalState {
  private readonly body: JournalBody;

  constructor(
    private readonly journal: SuSetJournal,
    private readonly data: Journal) {
    this.body = JSON.parse(data.body);
  }

  get maybeTime() {
    return this.body.time != null ? TreeClock.fromJson(this.body.time) : null;
  }

  get time() {
    return TreeClock.fromJson(this.body.time) as TreeClock;
  }

  async tail(): Promise<SuSetJournalEntry> {
    if (this.body.tail == null)
      throw new Error('Journal has no tail yet');
    return this.journal.entry(this.body.tail);
  }

  async findEntry(ticks: number) {
    const foundId = await this.journal.graph.find1<JournalEntry>({ ticks });
    return foundId ? this.journal.entry(foundId) : null;
  }

  async setLocalTime(localTime: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const patch = await this.updateBody({ time: localTime.toJson() });
    if (newClone) {
      // For a new clone, the journal's dummy tail does not have a timestamp
      const tail = await this.tail();
      patch.append(await tail.setHeadTime(localTime));
    } else {
      // This time might be seen by the outside world, so ensure that the tail
      // entry is marked as covering it
      patch.append(await this.journal.graph.insert({
        '@id': this.body.tail, ticks: localTime.ticks
      }));
    }
    return patch;
  }

  static initState(headEntry: Partial<JournalEntry>, localTime?: TreeClock): Subject {
    const body: Partial<JournalBody> = { tail: headEntry['@id'] };
    if (localTime != null)
      body.time = localTime.toJson();
    return { '@id': 'qs:journal', body: JSON.stringify(body) };
  }

  async setTail(tailId: Iri, localTime: TreeClock): Promise<PatchQuads> {
    return this.updateBody({ tail: tailId, time: localTime.toJson() });
  }

  private updateBody(update: Partial<JournalBody>): Promise<PatchQuads> {
    return this.journal.graph.write({
      '@delete': { '@id': 'qs:journal', body: this.data.body },
      '@insert': { '@id': 'qs:journal', body: JSON.stringify({ ...this.body, ...update }) }
    });
  }
}

export class SuSetJournal {
  constructor(
    readonly graph: JrqlGraph) {
  }

  async initialise(): Promise<Patch | undefined> {
    // Create the Journal if not exists
    const journal = await this.graph.describe1<Journal>('qs:journal');
    if (journal == null)
      return this.reset(Hash.random());
  }

  async reset(hash: Hash, gwc?: TreeClock, localTime?: TreeClock): Promise<Patch> {
    const entry = SuSetJournalEntry.headEntry(hash, localTime, gwc);
    const journal = SuSetJournalState.initState(entry, localTime);
    const insert = await this.graph.insert([journal, entry]);
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  async state() {
    const journal = await this.graph.describe1<Journal>('qs:journal');
    if (journal == null)
      throw new Error('Missing journal');
    return new SuSetJournalState(this, journal);
  }

  async entry(id: JournalEntry['@id']) {
    const entry = await this.graph.describe1<JournalEntry>(id);
    if (entry == null)
      throw new Error('Missing entry');
    return new SuSetJournalEntry(this, entry);
  }
}
