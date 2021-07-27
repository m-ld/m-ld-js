import { MeldReadState, readResult } from '../../api';
import { Read } from '../../jrql-support';
import { JrqlGraph } from './JrqlGraph';
import { defaultIfEmpty, firstValueFrom } from 'rxjs';
import { QuadSource } from '../quads';

/**
 * Utility class to directly coerce a JrqlGraph to be a MeldReadState, with no
 * context.
 */
export class GraphState implements MeldReadState {
  constructor(
    readonly graph: JrqlGraph) {
  }

  match: QuadSource['match'] = (...args) => this.graph.graph.match(...args);

  read(request: Read) {
    return readResult(this.graph.read(request));
  }

  get(id: string) {
    return firstValueFrom(this.graph.describe1(id).pipe(defaultIfEmpty(undefined)));
  }
}
