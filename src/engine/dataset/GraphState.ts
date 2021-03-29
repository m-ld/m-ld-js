import { MeldReadState, readResult } from '../../api';
import { Subject, Read } from '../../jrql-support';
import { JrqlGraph } from './JrqlGraph';

/**
 * Utility class to directly coerce a JrqlGraph to be a MeldReadState, with no
 * context.
 */
export class GraphState implements MeldReadState {
  constructor(
    readonly graph: JrqlGraph) {
  }

  read(request: Read) {
    return readResult(this.graph.read(request));
  }

  get(id: string) {
    return this.graph.describe1(id).toPromise();
  }
}
