import { Quad, Term } from 'rdf-js'

type VarValues = { [name: string]: Term };

export class QuadSolution {
  static EMPTY: QuadSolution = new QuadSolution({}, []);

  private constructor(
    readonly vars: VarValues,
    readonly quads: Quad[]) {
  }

  intersect(pattern: Quad, actual: Quad): QuadSolution | null {
    let vars: VarValues | null = this.vars;
    vars = intersectVar(pattern, actual, 'subject', vars);
    vars = intersectVar(pattern, actual, 'predicate', vars);
    vars = intersectVar(pattern, actual, 'object', vars);
    return vars ? new QuadSolution(vars, [...this.quads, actual]) : null;
  }
}

function intersectVar(pattern: Quad, actual: Quad,
  pos: Exclude<keyof Quad, 'equals'>, vars: VarValues | null): VarValues | null {
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

