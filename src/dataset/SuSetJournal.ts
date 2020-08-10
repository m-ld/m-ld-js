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

interface JournalUpdate {
  patch: PatchQuads,
  entry: SuSetJournalEntry
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
    const headEntryId = toPrefixedId('entry', encodedHash);
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

  async setHeadTime(localTime: TreeClock): Promise<PatchQuads> {
    const patchBody = await this.updateBody({ time: localTime.toJson() });
    const patchTicks = await this.journal.graph.insert({ '@id': this.id, ticks: localTime.ticks });
    return patchBody.concat(patchTicks);
  }

  async createNext(
    delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock): Promise<JournalUpdate> {
    const block = new JsonDeltaBagBlock(this.hash).next(delta.json);
    const entryId = toPrefixedId('entry', block.id.encode());
    const newBody: JournalEntryBody = {
      remote: remoteTime != null,
      hash: block.id.encode(),
      tid: delta.tid,
      time: (remoteTime ?? localTime).toJson(),
      delta: delta.json
    };
    const newEntry: JournalEntry & Subject = {
      '@id': entryId,
      body: JSON.stringify(newBody),
      ticks: localTime.ticks
    };
    const patch = await this.updateBody({ next: entryId });
    return {
      patch: patch.concat(await this.journal.graph.insert(newEntry)),
      entry: new SuSetJournalEntry(this.journal, newEntry)
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

  async setNext(entry: SuSetJournalEntry, localTime: TreeClock): Promise<PatchQuads> {
    return this.updateBody({ tail: entry.id, time: localTime.toJson() });
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