import { Context } from './jrql-support';
import { namedNode } from '@rdfjs/data-model';
import { NamedNode } from 'rdf-js';

export const BASE_CONTEXT: Context = {
  qs: 'http://qs.m-ld.org/'
}

export function qsName(name: string): NamedNode {
  return namedNode(BASE_CONTEXT.qs + name);
}

export function toPrefixedId(prefix: string, ...path: string[]) {
  return `${prefix}:${path.map(encodeURIComponent).join('/')}`;
}