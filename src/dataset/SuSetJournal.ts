import { Context, Subject } from './jrql-support';
import { BASE_CONTEXT, toPrefixedId } from './SuSetGraph';
import { Iri } from 'jsonld/jsonld-spec';
import { UUID, DeltaMessage, MeldDelta } from '../m-ld';
import { fromTimeString, toTimeString, JsonDeltaBagBlock } from '../m-ld/MeldJson';
import { TreeClock } from '../clocks';
import { PatchQuads, Patch } from '.';
import { JrqlGraph } from './JrqlGraph';
import { isEmpty } from 'rxjs/operators';
import { Hash } from '../hash';

export const JOURNAL_CONTEXT: Context = {
  ...BASE_CONTEXT,
  tid: 'qs:#tid', // Property of journal entry
  tail: { '@id': 'qs:#tail', '@type': '@id' }, // Property of the journal
  lastDelivered: { '@id': 'qs:#lastDelivered', '@type': '@id' }, // Property of the journal
  entry: 'qs:journal/entry/', // Namespace for journal entries
  hash: 'qs:#hash', // Property of a journal entry
  delta: 'qs:#delta', // Property of a journal entry
  remote: 'qs:#remote', // Property of a journal entry
  time: 'qs:#time', // Property of journal AND a journal entry
  ticks: 'qs:#ticks', // Property of a journal entry
  next: { '@id': 'qs:#next', '@type': '@id' } // Property of a journal entry
};

interface Journal extends Subject {
  '@id': 'qs:journal', // Singleton object
  tail: JournalEntry['@id'];
  lastDelivered: JournalEntry['@id'];
  time: string; // JSON-encoded TreeClock (the local clock)
}

/**
 * A journal entry includes a transaction Id
 */
interface JournalEntry extends Subject {
  '@id': Iri; // entry:<encoded block hash>
  tid: UUID; // Transaction ID
  hash: string; // Encoded Hash
  delta: string; // JSON-encoded JsonDelta
  remote: boolean;
  time: string; // JSON-encoded TreeClock (the remote clock)
  ticks: number; // Local clock ticks on delivery
  next?: JournalEntry['@id'];
}

function safeTime(je: Journal | JournalEntry) {
  return fromTimeString(je.time) as TreeClock; // Safe after init
}

interface JournalLocalUpdate {
  patch: PatchQuads,
  deltaMsg: DeltaMessage,
  entry: SuSetJournalEntry
}

export class SuSetJournalEntry {
  constructor(
    private readonly journal: SuSetJournal,
    private readonly data: JournalEntry) {
  }

  get id() {
    return this.data['@id'];
  }

  get hash() {
    return Hash.decode(this.data.hash);
  }

  get remote() {
    return this.data.remote;
  }

  get time() {
    return safeTime(this.data);
  }

  get delta() {
    return JSON.parse(this.data.delta);
  }

  async next(): Promise<SuSetJournalEntry | undefined> {
    if (this.data.next)
      return new SuSetJournalEntry(this.journal,
        await this.journal.graph.describe1<JournalEntry>(this.data.next));
  }

  async createNext(delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock): Promise<{
    patch: PatchQuads,
    entry: SuSetJournalEntry
  }> {
    const block = new JsonDeltaBagBlock(this.hash).next(delta.json);
    const entryId = toPrefixedId('entry', block.id.encode());
    const linkFromPrev: Partial<JournalEntry> = { '@id': this.id, next: entryId };
    const newEntry: JournalEntry = {
      '@id': entryId,
      remote: remoteTime != null,
      hash: block.id.encode(),
      tid: delta.tid,
      time: toTimeString(remoteTime ?? localTime),
      ticks: localTime.ticks,
      delta: JSON.stringify(delta.json)
    };
    return {
      patch: await this.journal.graph.insert([linkFromPrev, newEntry]),
      entry: new SuSetJournalEntry(this.journal, newEntry)
    };
  }

  async markDelivered() {
    return (await this.journal.state()).markDelivered(this);
  }
}

export class SuSetJournalState {
  constructor(
    private readonly journal: SuSetJournal,
    private readonly data: Journal) {
  }

  get maybeTime() {
    return fromTimeString(this.data.time);
  }

  get time() {
    return safeTime(this.data);
  }

  async tail(): Promise<SuSetJournalEntry> {
    if (this.data.tail == null)
      throw new Error('Journal has no tail yet');
    return this.entry(this.data.tail);
  }

  async lastDelivered(): Promise<SuSetJournalEntry> {
    return this.entry(this.data.lastDelivered);
  }

  async markDelivered(entry: SuSetJournalEntry) {
    return this.journal.graph.write({
      '@delete': { '@id': 'qs:journal', lastDelivered: this.data.lastDelivered } as Partial<Journal>,
      '@insert': { '@id': 'qs:journal', lastDelivered: entry.id } as Partial<Journal>
    });
  }

  async findEntry(ticks: number) {
    const foundId = await this.journal.graph.find1<JournalEntry>({ ticks });
    return foundId ? this.entry(foundId) : null;
  }

  async setLocalTime(localTime: TreeClock, newClone?: boolean): Promise<PatchQuads> {
    const encodedTime = toTimeString(localTime);
    const update = {
      '@delete': { '@id': 'qs:journal', time: this.data.time } as Partial<Journal>,
      '@insert': [{ '@id': 'qs:journal', time: encodedTime }] as Subject[]
    };
    if (newClone) {
      // For a new clone, the journal's dummy tail does not already have a
      // timestamp
      const tailPatch: Partial<JournalEntry> = {
        '@id': this.data.tail,
        time: encodedTime,
        ticks: localTime.ticks
      };
      update['@insert'].push(tailPatch);
    }
    return this.journal.graph.write(update);
  }

  async nextEntry(delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock) {
    const oldTail = await this.tail();
    const { patch, entry } = await oldTail.createNext(delta, localTime, remoteTime);
    return {
      patch: patch.concat(await this.journal.graph.write({
        '@delete': { '@id': 'qs:journal', tail: oldTail.id } as Partial<Journal>,
        '@insert': { '@id': 'qs:journal', tail: entry.id } as Partial<Journal>
      })).concat(await this.setLocalTime(localTime)),
      entry
    };
  }

  private async entry(id: JournalEntry['@id']) {
    return new SuSetJournalEntry(this.journal,
      await this.journal.graph.describe1<JournalEntry>(id));
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
    const encodedHash = startingHash.encode();
    const headEntryId = toPrefixedId('entry', encodedHash);
    const journal: Partial<Journal> = {
      '@id': 'qs:journal',
      lastDelivered: headEntryId,
      tail: headEntryId,
    };
    if (localTime != null)
      journal.time = toTimeString(localTime);
    const entry: Partial<JournalEntry> = {
      '@id': headEntryId,
      hash: encodedHash,
      ticks: localTime?.ticks
    };
    if (startingTime != null)
      entry.time = toTimeString(startingTime);
    const insert = await this.graph.insert([journal, entry]);
    // Delete matches everything in all graphs
    return { oldQuads: {}, newQuads: insert.newQuads };
  }

  async state(): Promise<SuSetJournalState> {
    return new SuSetJournalState(this, await this.graph.describe1<Journal>('qs:journal'));
  }

  async journal(delta: MeldDelta, localTime: TreeClock): Promise<JournalLocalUpdate>;
  async journal(delta: MeldDelta, localTime: TreeClock, remoteTime: TreeClock): Promise<PatchQuads>;
  async journal(delta: MeldDelta, localTime: TreeClock, remoteTime?: TreeClock): Promise<JournalLocalUpdate | PatchQuads> {
    const journal = await this.state();
    const nextEntry = await journal.nextEntry(delta, localTime, remoteTime);
    if (remoteTime == null) { // This is a local transaction
      // Create a DeltaMessage to be marked delivered later
      return { ...nextEntry, deltaMsg: new DeltaMessage(localTime, delta.json) };
    } else { // This is a remote transaction
      return nextEntry.patch;
    }
  };
}