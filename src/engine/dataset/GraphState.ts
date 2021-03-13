import { MeldReadState, Resource, readResult } from '../../api';
import { Subject, Read } from '../../jrql-support';
import { JrqlGraph } from './JrqlGraph';
import { map } from 'rxjs/operators';

/**
 * Utility class to directly coerce a JrqlGraph to be a MeldReadState, with no
 * context.
 */
export class GraphState implements MeldReadState {
  constructor(
    readonly graph: JrqlGraph) {
  }

  read<S>(request: Read) {
    return readResult(this.graph.read(request));
  }

  get<S = Subject>(id: string) {
    return <Promise<Resource<S> | undefined>>this.graph.describe1(id).toPromise();
  }
}
