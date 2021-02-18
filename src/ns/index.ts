import { DataFactory, NamedNode } from 'rdf-js';

export type Names<N extends { [key: string]: string }> =
  { readonly [key in keyof N]: NamedNode };

/**
 * Utility to create a namespace containing RDF named nodes for each name in names
 */
export function ns<N extends string>(
  names: { [key in N]: string }, rdf: DataFactory): Names<typeof names> {
  const nameDef: PropertyDescriptorMap = {};
  for (let key in names)
    nameDef[key] = { value: rdf.namedNode(names[key]) };
  return Object.defineProperties({}, nameDef);
}

export * as jrql from './json-rql';
export * as mld from './m-ld';
export * as rdf from './rdf';
export * as xs from './xs';
export * as qs from './quadstore';