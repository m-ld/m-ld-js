import { Iri } from "jsonld/jsonld-spec";
import { toArray } from '../util';

export type Variable = string;

export interface Pattern {
  '@context'?: Context
}

export type TermDef = Iri | ExpandedTermDef;

export interface ExpandedTermDef {
  '@id'?: Iri;
  '@reverse'?: TermDef;
  '@type'?: Iri;
  '@language'?: string;
  '@container'?: '@list' | '@set' | '@language' | '@index';
}

export interface Context {
  '@base'?: Iri;
  '@vocab'?: Iri;
  '@language'?: string;
  [key: string]: TermDef | undefined;
}

export interface ValueObject {
  '@value': number | string | boolean;
  '@type'?: Iri;
  '@language'?: string;
  '@index'?: string;
}

export function isValueObject(value: JrqlValue): value is ValueObject {
  return typeof value == 'object' && '@value' in value;
}

export interface Reference {
  '@id': Iri;
}

export function isReference(value: JrqlValue): value is Reference {
  return typeof value == 'object' && Object.keys(value).every(k => k === '@id');
}

export type JrqlValue = number | string | boolean | Subject | Reference | ValueObject;

export interface Subject extends Pattern {
  '@id'?: Iri;
  '@type'?: Iri;
  [key: string]: JrqlValue | JrqlValue[] | Context | undefined;
}

export function isSubject(p: Pattern): p is Subject {
  return !isGroup(p) && !isQuery(p);
}

export interface Group extends Pattern {
  '@graph': Subject[] | Subject;
  //'@filter': TODO Expressions
}

export function isGroup(p: Pattern): p is Group {
  return '@graph' in p || '@filter' in p;
}

export type GroupLike = Subject[] | Subject | Group;

export function isGroupLike(pattern: Pattern[] | Pattern): pattern is GroupLike {
  return Array.isArray(pattern) ? pattern.every(isGroupLike) : !isQuery(pattern);
}

export function asGroup(g: GroupLike, context?: Context): Group {
  let group: Group;
  if ('@graph' in g) {
    group = g as Group;
  } else if (Array.isArray(g)) {
    // Cannot promote contexts
    group = { '@graph': g };
  } else {
    // Promote the subject's context to the group level
    const { '@context': subjectContext, ...subject } = g;
    context = { ...subjectContext, ...context };
    group = { '@graph': subject };
  }
  return context ? { '@context': context, ...group } : group;
}

export function asSubjects(g: GroupLike, context?: Context): Subject[] {
  return toArray(asGroup(g)['@graph']);
}

export interface Query extends Pattern {
  '@where'?: Pattern[] | Pattern
}

export function isQuery(p: Pattern): p is Query {
  return isRead(p) || isUpdate(p);
}

export interface Read extends Query {
  orderBy?: string, // TODO: Operators & Functions
  limit?: number,
  offset?: number
}

export function isRead(p: Pattern): p is Read {
  return isDescribe(p) || isConstruct(p) || isDistinct(p) || isSelect(p);
}

export interface Describe extends Read {
  '@describe': Iri | Variable
}

export function isDescribe(p: Pattern): p is Describe {
  return '@describe' in p;
}

export interface Construct extends Read {
  '@construct': GroupLike
}

export function isConstruct(p: Pattern): p is Construct {
  return '@construct' in p;
}

export type Result = '*' | Variable

export interface Distinct extends Read {
  '@distinct': Result[] | Result
}

export function isDistinct(p: Pattern): p is Distinct {
  return '@distinct' in p;
}

export interface Select extends Read {
  '@select': Result[] | Result
}

export function isSelect(p: Pattern): p is Select {
  return '@select' in p;
}

export interface DeleteInsert<T extends GroupLike = GroupLike> {
  '@insert': T;
  '@delete': T;
}

export interface Update extends Query, Partial<DeleteInsert<GroupLike>> {
}

export function isUpdate(p: Pattern): p is Update {
  return '@insert' in p || '@delete' in p;
}
