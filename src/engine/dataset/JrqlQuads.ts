import { Binding } from 'quadstore';
import { Quad, Quad_Object, Term } from 'rdf-js';
import { Graph } from '.';
import { GraphSubject, blank } from '../..';
import {
  Context, Subject, Result, Value, Atom, Reference} from '../../jrql-support';
import { activeCtx, compactIri } from '../jsonld';
import { inPosition } from '../quads';
import { jrql } from '../../ns';
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

  async solutionSubject(
    results: Result[] | Result, solution: Binding, context: Context): Promise<GraphSubject> {
    const solutionId = this.graph.blankNode(blank());
    const pseudoPropertyQuads = Object.entries(solution).map(([variable, term]) => this.graph.quad(
      solutionId,
      this.graph.namedNode(jrql.hiddenVar(variable.slice(1))),
      inPosition('object', term)));
    // Construct quads that represent the solution's variable values
    const subject = await this.toApiSubject(pseudoPropertyQuads, [/* TODO: list-items */], context);
    // Unhide the variables and strip out anything that's not selected
    return <GraphSubject>mapObject(subject, (key, value) => {
      switch (key) {
        case '@id': return { [key]: value };
        default:
          const varName = jrql.matchHiddenVar(key), newKey = (varName ? '?' + varName : key);
          if (isSelected(results, newKey))
            return { [newKey]: value };
      }
    });
  }

  async quads(subjects: Subject | Subject[],
    opts: JrqlQuadsOptions, context: Context): Promise<Quad[]> {
    const processor = new SubjectQuads(
      opts.mode, await activeCtx(context), this.graph, opts.vars);
    return [...processor.quads(subjects)];
  }

  /**
   * @param propertyQuads subject-property-value quads
   * @param listItemQuads subject-index-item quads for list-like subjects
   * @returns a single subject compacted against the given context
   */
  async toApiSubject(
    propertyQuads: Quad[], listItemQuads: Quad[], context: Context): Promise<GraphSubject> {
    const subjects = await SubjectGraph.fromRDF(propertyQuads).withContext(context);
    const subject = { ...subjects[0] };
    if (listItemQuads.length) {
      const ctx = await activeCtx(context);
      // Sort the list items lexically by index
      // TODO: Allow for a list implementation-specific ordering
      const indexes = new Set(listItemQuads.map(iq => iq.predicate.value).sort()
        .map(index => compactIri(index, ctx)));
      // Create a subject containing only the list items
      const list = await this.toApiSubject(listItemQuads, [], context);
      subject['@list'] = [...indexes].map(index => <Value>list[index]);
    }
    return subject;
  }

  genSubValue(parentValue: Term, subVarName: jrql.SubVarName) {
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

  async toObjectTerm(value: Atom | Reference, context: Context): Promise<Quad_Object> {
    return new SubjectQuads('match', await activeCtx(context), this.graph).objectTerm(value);
  }
}

function isSelected(results: Result[] | Result, key: string) {
  return results === '*' || key.startsWith('@') ||
    (isArray(results) ? results.includes(key) : results === key);
}
