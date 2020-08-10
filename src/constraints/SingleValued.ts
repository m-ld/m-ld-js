import { MeldConstraint, MeldUpdate, MeldReader } from '../m-ld';
import { Iri } from 'jsonld/jsonld-spec';
import { Subject, Select } from '..';
import { MeldApi, DeleteInsert } from '../m-ld/MeldApi';
import { map, filter, take, reduce } from 'rxjs/operators';
import { Reference, Update, Value, isValueObject } from '../dataset/jrql-support';
import { Observable, EMPTY, concat, defer, from, pairs } from 'rxjs';

function isMultiValued(value: Subject['any']): value is Array<Value> {
  return Array.isArray(value) && value.length > 1;
}

export class SingleValued implements MeldConstraint {
  constructor(
    readonly property: Iri) {
  }

  protected async resolve(values: Value[]): Promise<Value> {
    return values.reduce((maxValue, value) =>
      comparable(value) > comparable(maxValue) ? value : maxValue);
  }

  async check(update: MeldUpdate, read: MeldReader): Promise<unknown> {
    // Fail early and report the first failure
    const failed = await this.affected(update, read, 'failEarly').pipe(
      filter(subject => isMultiValued(subject[this.property])),
      take(1)).toPromise();
    return failed != null ? Promise.reject(this.failure(failed)) : Promise.resolve();
  }

  async apply(update: MeldUpdate, read: MeldReader): Promise<Update | null> {
    return await this.affected(update, read).pipe(
      reduce<Subject, Promise<DeleteInsert<Subject[]> | null>>(async (accPattern, subject) => {
        const values = subject[this.property];
        if (isMultiValued(values)) {
          const resolvedValue = await this.resolve(values);
          const pattern = await accPattern ?? { '@insert': [], '@delete': [] };
          pattern['@delete'].push({
            '@id': subject['@id'],
            [this.property]: values.filter(v => v !== resolvedValue)
          });
          return pattern;
        }
        return accPattern;
      }, Promise.resolve(null))).toPromise();
  }

  private affected(update: MeldUpdate, read: MeldReader, failEarly?: 'failEarly'): Observable<Subject> {
    const hasProperty = (subject: Subject): boolean => subject[this.property] != null;
    const propertyInserts = update['@insert'].filter(hasProperty);
    if (propertyInserts.length) {
      // 'Fail early' means we pipe the raw inserts through the filter first,
      // in case they trivially contain an array for the property
      return concat(failEarly ? from(propertyInserts) : EMPTY, defer(() => {
        // Reformulate the update per-(subject with the target property)
        const subjectUpdates = MeldApi.asSubjectUpdates({
          '@delete': update['@delete'].filter(hasProperty),
          '@insert': propertyInserts
        });
        return concat(read<Select>({
          '@select': ['?s', '?o'],
          '@where': {
            '@union': Object.keys(subjectUpdates).map(sid => [
              // Slight weirdness to say "subject id is both ?s and sid"
              // TODO: Support InlineFilters
              { '@id': '?s' }, { '@id': sid, [this.property]: '?o' }
            ])
          }
        }).pipe(map(select => {
          // Validate the final value of the property
          const subject = <Subject & Reference>select['?s'];
          const sid = subject['@id'];
          // Weirdness to construct a subject from the select result
          // TODO: Support `@construct`
          Object.assign(subject, { [this.property]: select['?o'] });
          MeldApi.update(subject, subjectUpdates[sid]);
          // Side-effect to prevent duplicate processing
          delete subjectUpdates[sid];
          return subject;
        })), from(pairs(subjectUpdates)).pipe(map(([sid, update]) => {
          const subject: Subject = { '@id': sid };
          MeldApi.update(subject, <DeleteInsert<Subject>>update);
          return subject;
        })));
      }));
    } else {
      return EMPTY;
    }
  }

  private failure(subject: Subject) {
    return `Multiple values for ${subject['@id']}: ${this.property}`;
  }
}

function comparable(value: Value): Value {
  return isValueObject(value) ? value['@value'] : value;
}
