import { MeldConstraint, MeldUpdate, MeldReadState, asSubjectUpdates, updateSubject, InterimUpdate } from '..';
import { Iri } from 'jsonld/jsonld-spec';
import { map, filter, take, mergeMap, defaultIfEmpty, concatMap } from 'rxjs/operators';
import { Subject, Select, Value, isValueObject } from '../jrql-support';
import { Observable, EMPTY, concat, defer, from } from 'rxjs';

/**
 * Configuration for a `SingleValued` constraint. The configured property should
 * have only one value.
 */
export interface SingleValuedConfig {
  '@type': 'single-valued';
  /**
   * The property can be given in unexpanded form, as it appears in JSON
   * subjects when using the API, or as its full IRI reference.
   */
  property: string;
}

/** @internal */
function isMultiValued(value: Subject['any']): value is Array<Value> {
  return Array.isArray(value) && value.length > 1;
}

/** @internal */
function comparable(value: Value): Value {
  return isValueObject(value) ? value['@value'] : value;
}

/** @internal */
export class SingleValued implements MeldConstraint {
  constructor(
    readonly property: Iri) {
  }

  protected async resolve(values: Value[]): Promise<Value> {
    return values.reduce((maxValue, value) =>
      comparable(value) > comparable(maxValue) ? value : maxValue);
  }

  async check(state: MeldReadState, update: MeldUpdate): Promise<unknown> {
    // Fail early and report the first failure
    const failed = await this.affected(state, update, 'failEarly').pipe(
      filter(subject => isMultiValued(subject[this.property])),
      take(1)).toPromise();
    return failed != null ? Promise.reject(this.failure(failed)) : Promise.resolve();
  }

  apply(state: MeldReadState, update: InterimUpdate): Promise<unknown> {
    return this.affected(state, update).pipe(
      concatMap(async subject => {
        const values = subject[this.property];
        if (isMultiValued(values)) {
          const resolvedValue = await this.resolve(values);
          await update.assert({
            '@delete': {
              '@id': subject['@id'],
              [this.property]: values.filter(v => v !== resolvedValue)
            }
          });
        }
      })).toPromise();
  }

  private affected(state: MeldReadState, update: MeldUpdate, failEarly?: 'failEarly'): Observable<Subject> {
    const hasProperty = (subject: Subject): boolean => subject[this.property] != null;
    const propertyInserts = update['@insert'].filter(hasProperty);
    // 'Fail early' means we pipe the raw inserts through the filter first,
    // in case they trivially contain an array for the property
    return !propertyInserts.length ? EMPTY :
      concat(failEarly ? from(propertyInserts) : EMPTY, defer(() => {
        // Reformulate the update per-(subject with the target property)
        const subjectUpdates = asSubjectUpdates({
          '@delete': update['@delete'].filter(hasProperty),
          '@insert': propertyInserts
        });
        return from(Object.keys(subjectUpdates)).pipe(
          mergeMap(sid => state.read<Select>({
            '@select': '?o', '@where': { '@id': sid, [this.property]: '?o' }
          }).pipe(
            defaultIfEmpty({ '@id': '_:b0', '?o': [] }),
            map(selectResult => {
              // Weirdness to construct a subject from the select result
              // TODO: Support `@construct`
              const subject = { '@id': sid, [this.property]: selectResult['?o'] };
              updateSubject(subject, subjectUpdates[sid]);
              // Side-effect to prevent duplicate processing
              delete subjectUpdates[sid];
              return subject;
            }))));
      }));
  }

  private failure(subject: Subject) {
    return `Multiple values for ${subject['@id']}: ${this.property}
    ${subject[this.property]}`;
  }
}
