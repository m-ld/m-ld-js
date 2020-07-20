import { Quad, Quad_Object, Quad_Predicate } from 'rdf-js'

export type VarValues = { [name: string]: Quad_Object | Quad_Predicate | Quad_Object };
export type TriplePos = Exclude<keyof Quad, 'equals' | 'graph'>;

export class QuadSolution {
  static EMPTY: QuadSolution = new QuadSolution({}, []);

  private constructor(
    readonly vars: VarValues,
    readonly quads: Quad[]) {
  }

  join(pattern: Quad, actual: Quad): QuadSolution | null {
    let vars: VarValues | null = this.vars;
    vars = joinVar(pattern, actual, 'subject', vars);
    vars = joinVar(pattern, actual, 'predicate', vars);
    vars = joinVar(pattern, actual, 'object', vars);
    return vars ? new QuadSolution(vars, [...this.quads, actual]) : null;
  }
}

function joinVar(pattern: Quad, actual: Quad,
  pos: TriplePos, vars: VarValues | null): VarValues | null {
  if (vars && pattern[pos].termType == 'Variable') {
    if (vars[pattern[pos].value]) {
      if (!vars[pattern[pos].value].equals(actual[pos]))
        return null; // Conflict
    } else {
      const newVars = { ...vars };
      newVars[pattern[pos].value] = actual[pos];
      return newVars;
    }
  }
  return vars;
}

