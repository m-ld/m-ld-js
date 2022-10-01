import { Binding, Graph } from '.';
import { blank, GraphSubject } from '../../api';
import { Atom, Result, Subject, Value, VocabReference } from '../../jrql-support';
import { ActiveContext, compactIri } from '../jsonld';
import { inPosition, Quad, Quad_Object, Term } from '../quads';
import { JRQL } from '../../ns';
import { SubjectGraph } from '../SubjectGraph';
import { JrqlMode, toIndexDataUrl } from '../jrql-util';
import { isArray, mapObject } from '../util';
import { SubjectQuads } from '../SubjectQuads';

export interface JrqlQuadsOptions {
  mode: JrqlMode;
  /** The variable names found (sans '?') */
  vars?: Set<string>;
}

export class JrqlQuads {
  constructor(
    readonly graph: Graph) {
  }

  solutionSubject(
    results: Result[] | Result, solution: Binding, ctx: ActiveContext): GraphSubject {
    const solutionId = this.graph.blankNode(blank());
    const pseudoPropertyQuads = Object.entries(solution).map(([variable, term]) => this.graph.quad(
      solutionId,
      this.graph.namedNode(JRQL.hiddenVar(variable.slice(1))),
      inPosition('object', term)));
    // Construct quads that represent the solution's variable values
    const subject = this.toApiSubject(pseudoPropertyQuads, [ /* TODO: list-items */], ctx);
    // Unhide the variables and strip out anything that's not selected
    return <GraphSubject>mapObject(subject, (key, value) => {
      switch (key) {
        case '@id': return { [key]: value };
        default:
          const varName = JRQL.matchHiddenVar(key), newKey = (varName ? '?' + varName : key);
          if (isSelected(results, newKey))
            return { [newKey]: value };
      }
    });
  }

  quads(subjects: Subject | Subject[],
    opts: JrqlQuadsOptions, ctx: ActiveContext): Quad[] {
    const processor = new SubjectQuads(opts.mode, ctx, this.graph, opts.vars);
    return [...processor.quads(subjects)];
  }

  /**
   * @param propertyQuads subject-property-value quads
   * @param listItemQuads subject-index-item quads for list-like subjects
   * @param ctx JSON-LD context
   * @returns a single subject compacted against the given context
   */
  toApiSubject(
    propertyQuads: Quad[], listItemQuads: Quad[], ctx: ActiveContext): GraphSubject {
    const subjects = SubjectGraph.fromRDF(propertyQuads, { ctx });
    const subject = { ...subjects[0] };
    if (listItemQuads.length) {
      // Sort the list items lexically by index
      // TODO: Allow for a list implementation-specific ordering
      const indexes = new Set(listItemQuads.map(iq => iq.predicate.value).sort()
        .map(index => compactIri(index, ctx)));
      // Create a subject containing only the list items
      const list = this.toApiSubject(listItemQuads, [], ctx);
      subject['@list'] = [...indexes].map(index => <Value>list[index]);
    }
    return subject;
  }

  genSubValue(parentValue: Term, subVarName: JRQL.SubVarName) {
    switch (subVarName) {
      case 'listKey':
        // Generating a data URL for the index key
        return this.graph.namedNode(toIndexDataUrl([Number(parentValue.value)]));
      case 'slotId':
        // Index exists, so a slot can be made
        if (this.graph.skolem != null)
          return this.graph.skolem();
    }
  }

  toObjectTerm(value: Atom | VocabReference, ctx: ActiveContext): Quad_Object {
    return new SubjectQuads('match', ctx, this.graph).objectTerm(value);
  }
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (isArray(results) ? results.includes(key) : results === key);
}
