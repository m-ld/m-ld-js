import { ExtensionSubject } from '../orm';
import { MeldExtensions, SharedDatatype, UUID } from '../api';
import { Expression, isConstraint, Subject } from '../jrql-support';
import { M_LD } from '../ns';
import { Iri } from '@m-ld/jsonld';
import { TSeq, TSeqOperation } from './TSeq';
import { uuid } from '../util';
import { MeldAppContext } from '../config';

export class TSeqDatatype implements MeldExtensions, SharedDatatype<TSeq, TSeqOperation[]> {
  static declare = (priority: number): Subject => ({
    '@id': M_LD.extensions,
    '@list': {
      [priority]: ExtensionSubject.declareMeldExt(
        'tseq', 'TSeqDatatype')
    }
  });

  private pid: string;

  setExtensionContext({ config }: MeldAppContext) {
    this.pid = config['@id'];
  }

  datatypes(id: Iri) {
    if (id === this['@id'])
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

  toValue(data: TSeq): string {
    return data.toString();
  }

  getDataId(): UUID {
    return uuid();
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

  apply(state: TSeq, operation: TSeqOperation[]): [TSeq, Expression] {
    // TODO: Return the splice(s). Requires support from TSeq
    state.apply(operation);
    return [state, {
      '@type': this['@id'],
      '@value': state.toString()
    }];
  }
}