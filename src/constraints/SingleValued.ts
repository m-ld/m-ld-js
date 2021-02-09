import {
  MeldConstraint, MeldUpdate, MeldReadState, asSubjectUpdates, updateSubject, InterimUpdate
} from '..';
import { Iri } from 'jsonld/jsonld-spec';
import { map, filter, take, concatMap } from 'rxjs/operators';
import { Subject, Select, Value, isValueObject, Reference } from '../jrql-support';
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
    // Report the first failure
    const failed = await this.affected(state, update).pipe(
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
          update.assert({
            '@delete': {
              '@id': subject['@id'],
              [this.property]: values.filter(v => v !== resolvedValue)
            }
          });
        }
      })).toPromise();
  }

  private affected(state: MeldReadState, update: MeldUpdate): Observable<Subject> {
    const propertyInserts = update['@insert'].filter(this.hasProperty);
    // Fail earliest if there are no inserts for the property
    return !propertyInserts.length ? EMPTY :
      // Fail early by piping the raw inserts through the filter first,
      // in case they trivially contain an array for the property
      concat(from(propertyInserts), defer(() => {
        // Reformulate the update per-(subject with the target property)
        const subjectUpdates = asSubjectUpdates({
          '@delete': update['@delete'].filter(this.hasProperty),
          '@insert': propertyInserts
        });
        const sids = Object.keys(subjectUpdates);
        return state.read<Select>({
          '@select': ['?s', '?o'],
          '@where': {
            '@graph': { '@id': '?s', [this.property]: '?o' },
            '@values': sids.map(sid => ({ '?s': { '@id': sid } }))
          }
        }).pipe(map(selectResult => {
          const sid = (<Reference>selectResult['?s'])['@id'];
          // Weirdness to construct a subject from the select result
          // TODO: Support `@construct`
          const subject = { '@id': sid, [this.property]: selectResult['?o'] };
          return updateSubject(subject, subjectUpdates[sid]);
        }));
      }));
  }

  private failure(subject: Subject) {
    return `Multiple values for ${subject['@id']}: ${this.property}
    ${subject[this.property]}`;
  }

  private hasProperty = (subject: Subject): boolean => subject[this.property] != null;
}
