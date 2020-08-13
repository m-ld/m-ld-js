import { Subject } from './jrql-support';
import { toPrefixedId } from './SuSetGraph';
import { Iri } from 'jsonld/jsonld-spec';
import { UUID, MeldDelta, JsonDelta } from '../m-ld';
import { JsonDeltaBagBlock } from '../m-ld/MeldJson';
import { TreeClock } from '../clocks';
import { PatchQuads, Patch } from '.';
import { JrqlGraph } from './JrqlGraph';
import { isEmpty } from 'rxjs/operators';
import { Hash } from '../hash';

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
  '@id': Iri; // entry:<encoded block hash>
  body: string; // JSON-encoded body
  ticks: number; // Local clock ticks on delivery
}

interface JournalEntryBody {
  tid: UUID; // Transaction ID
  hash: string; // Encoded Hash
  delta: JsonDelta; // JSON-encoded JsonDelta
  remote: boolean;
  time: any; // JSON-encoded TreeClock (the remote clock)
  next?: JournalEntry['@id'];
}

export interface EntryCreateDetails {
  delta: MeldDelta;
  localTime: TreeClock;
  remoteTime?: TreeClock;
}

export class SuSetJournalEntry {
  private readonly body: JournalEntryBody;

  constructor(
    private readonly journal: SuSetJournal,
    private readonly data: JournalEntry) {
    this.body = JSON.parse(data.body);
  }

  get id(): Iri {
    return this.data['@id'];
  }

  get ticks(): number {
    return this.data.ticks;
  }

  get hash(): Hash {
    return Hash.decode(this.body.hash);
  }

  get remote(): boolean {
    return this.body.remote;
  }

  get time(): TreeClock {
    return TreeClock.fromJson(this.body.time) as TreeClock;
  }

  get delta(): JsonDelta {
    return this.body.delta;
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.body.next)
      return new SuSetJournalEntry(this.journal,
        await this.journal.graph.describe1<JournalEntry>(this.body.next));
  }

  static headEntry(startingHash: Hash, localTime?: TreeClock, startingTime?: TreeClock): Subject {
    const encodedHash = startingHash.encode();
    const headEntryId = SuSetJournalEntry.id(encodedHash);
    const body: Partial<JournalEntryBody> = { hash: encodedHash };
    if (startingTime != null)
      body.time = startingTime.toJson();
    const entry: Partial<JournalEntry> = {
      '@id': headEntryId,
      body: JSON.stringify(body),
      ticks: localTime?.ticks
    };
    return entry;
  }

  private static createEntry(
    hash: Hash, delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock, nextHash?: Hash) {
    const encodedHash = hash.encode();
    const body: JournalEntryBody = {
      remote: remoteTime != null,
      hash: encodedHash,
      tid: delta.tid,
      time: (remoteTime ?? localTime).toJson(),
      delta: delta.json,
      next: nextHash != null ? SuSetJournalEntry.id(nextHash.encode()) : undefined
    };
    return {
      '@id': SuSetJournalEntry.id(encodedHash),
      body: JSON.stringify(body),
      ticks: localTime.ticks
    };
  }

  private static id(encodedHash: string) {
    return toPrefixedId('entry', encodedHash);
  }

  private static nextHash(prevHash: Hash, delta: MeldDelta) {
    return new JsonDeltaBagBlock(prevHash).next(delta.json).id;
  }

  async setHeadTime(localTime: TreeClock): Promise<PatchQuads> {
    const patchBody = await this.updateBody({ time: localTime.toJson() });
    const patchTicks = await this.journal.graph.insert({ '@id': this.id, ticks: localTime.ticks });
    return patchBody.concat(patchTicks);
  }

  async createNext(...next: EntryCreateDetails[]): Promise<{ patch: PatchQuads, tailId: Iri }> {
    if (!next.length)
      throw new Error('next required');
    // First create the hashes from previous or this
    const hashes = next.reduce<Hash[]>((hashes, next) =>
      hashes.concat(SuSetJournalEntry.nextHash(
        hashes.length ? hashes.slice(-1)[0] : this.hash, next.delta)), []);
    // Then create the entries
    const entries = next.map((next, i) =>
      SuSetJournalEntry.createEntry(
        hashes[i], next.delta, next.localTime, next.remoteTime, hashes[i + 1]));

    const patch = await this.journal.graph.insert(entries);
    return {
      patch: patch.concat(await this.updateBody({ next: entries[0]['@id'] })),
      tailId: entries.slice(-1)[0]['@id']
    };
  }

  private updateBody(update: Partial<JournalEntryBody>): Promise<PatchQuads> {
    return this.journal.graph.write({
      '@delete': { '@id': this.id, body: this.data.body },
      '@insert': { '@id': this.id, body: JSON.stringify({ ...this.body, ...update }) }
    });
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
    return this.entry(this.body.tail);
  }

  async findEntry(ticks: number) {
    const foundId = await this.journal.graph.find1<JournalEntry>({ ticks });
    return foundId ? this.entry(foundId) : null;
  }

  async setLocalTime(localTime: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const patch = await this.updateBody({ time: localTime.toJson() });
    if (newClone) {
      // For a new clone, the journal's dummy tail does not have a timestamp
      const tail = await this.tail();
      return patch.concat(await tail.setHeadTime(localTime));
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

  private async entry(id: JournalEntry['@id']) {
    return new SuSetJournalEntry(this.journal,
      await this.journal.graph.describe1<JournalEntry>(id));
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
    if (await this.graph.describe('qs:journal').pipe(isEmpty()).toPromise())
      return this.reset(Hash.random());
  }

  async reset(startingHash: Hash,
    startingTime?: TreeClock, localTime?: TreeClock): Promise<Patch> {
    const entry = SuSetJournalEntry.headEntry(startingHash, localTime, startingTime);
    const journal = SuSetJournalState.initState(entry, localTime);
    const insert = await this.graph.insert([journal, entry]);
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  async state(): Promise<SuSetJournalState> {
    return new SuSetJournalState(this, await this.graph.describe1<Journal>('qs:journal'));
  }
}
