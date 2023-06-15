import { ExtensionSubject, OrmUpdating } from '../orm';
import { GraphSubject, MeldPlugin, SharedDatatype, UUID } from '../api';
import { Constraint, Expression, isConstraint, Subject, VocabReference } from '../jrql-support';
import { M_LD, SH, XS } from '../ns';
import { Iri } from '@m-ld/jsonld';
import { TSeq, TSeqOperation } from './TSeq';
import { array, uuid } from '../util';
import { MeldAppContext } from '../config';
import { ExtensionSubjectInstance } from '../orm/ExtensionSubject';
import { JsProperty } from '../js-support';

export class TSeqText
  implements MeldPlugin, SharedDatatype<TSeq, TSeqOperation[]>, ExtensionSubjectInstance {
  static declare = (priority: number, ...properties: Iri[]): Subject => ({
    '@id': M_LD.extensions,
    '@list': {
      [priority]: ExtensionSubject.declareMeldExt(
        'tseq', 'TSeqText', {
          [SH.targetObjectsOf]: properties.map(p => ({ '@vocab': p }))
        })
    }
  });

  private pid: string;
  properties: Set<string>;

  /**
   * @param properties the fully-expanded property IRIs to target
   */
  constructor(...properties: string[]) {
    this.properties = new Set(properties);
  }

  setExtensionContext({ config }: MeldAppContext) {
    this.pid = config['@id'];
  }

  /** @internal */
  initFromData(src: GraphSubject, orm: OrmUpdating, ext: ExtensionSubject<this>) {
    ext.initSrcProperty(src, [this, 'properties'], // not used due to get/set
      JsProperty.for(SH.targetObjectsOf, Array, VocabReference), {
        get: () => [...this.properties].map(p => ({ '@vocab': p })),
        set: (v: VocabReference[]) => this.properties = new Set(v.map(ref => ref['@vocab']))
      });
  }

  /** @implements MeldExtensions#indirectedData */
  indirectedData(property: Iri, datatype: Iri) {
    if (datatype === this['@id'] ||
      (this.properties.has(property) && datatype === XS.string))
      return this;
  }

  readonly '@id' = M_LD.EXT.extensionType('tseq', 'TSeq');

  validate(value: unknown) {
    if (typeof value == 'string') {
      const tSeq = new TSeq(this.pid);
      tSeq.splice(0, 0, value);
      return tSeq;
    }
  }

  toValue(data: TSeq) {
    return { '@type': XS.string, '@value': data.toString() };
  }

  getDataId(): UUID {
    return uuid();
  }

  sizeOf(data: TSeq): number {
    // Every character:
    // - object = 50 bytes
    // - 2 fields = 2*8 bytes
    // - _char = 2 bytes + string overhead = 10 bytes
    // - _tick = 2 bytes (optimised as a short integer)
    // Tree overhead, ideally O(log length)
    // - object = 50 bytes
    // - 2-3 fields = 20 bytes
    // @see https://www.mattzeunert.com/2016/07/24/javascript-array-object-sizes.html
    // TODO: Verify these claims!
    const strLen = data.charLength;
    return strLen * 70 + strLen.toString(2).length * 70;
  }

  fromJSON(json: any): TSeq {
    return TSeq.fromJSON(this.pid, json);
  }

  toJSON(data: TSeq): any {
    return data.toJSON();
  }

  update(state: TSeq, update: Expression): [TSeq, TSeqOperation[]] {
    if (isConstraint(update)) {
      for (let [key, args] of Object.entries(update)) {
        switch (key) {
          case '@concat':
            // noinspection SuspiciousTypeOfGuard
            if (typeof args == 'string')
              return [state, state.splice(Infinity, 0, args)];
            break;
          case '@splice':
            if (Array.isArray(args)) {
              const [index, deleteCount, content] = args;
              // noinspection SuspiciousTypeOfGuard
              if (typeof index == 'number' &&
                typeof deleteCount == 'number' &&
                (content == null || typeof content == 'string')) {
                return [state, state.splice(index, deleteCount, content)];
              }
            }
        }
      }
    }
    throw new RangeError(`Invalid update expression: ${update}`);
  }

  apply(state: TSeq, operation: TSeqOperation[]): [TSeq, Expression[]] {
    const splices = state.apply(operation)
      .map<Constraint>(splice => ({ '@splice': array(splice) }));
    return [state, splices];
  }

  revert(state: TSeq, operation: TSeqOperation[], revert: null): [TSeq, Expression] {
    // @ts-ignore
    return [undefined, undefined];
  }
}