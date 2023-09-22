import { ExtensionSubject, OrmUpdating } from '../orm';
import { GraphSubject, MeldPlugin, SharedDatatype, UUID } from '../api';
import {
  Constraint, Expression, isConstraint, isTextSplice, Subject, VocabReference
} from '../jrql-support';
import { M_LD, SH, XS } from '../ns';
import { Iri } from '@m-ld/jsonld';
import { TSeq, TSeqOperation } from '.';
import { array, uuid } from '../util';
import { MeldAppContext } from '../config';
import { ExtensionSubjectInstance } from '../orm/ExtensionSubject';
import { JsProperty } from '../js-support';
import { TSeqOperable } from './TSeqOperable';

/**
 * This extension allows an app to embed collaborative text in a domain.
 *
 * When the extension is declared for a certain property with a string value,
 * the string should be updated using the `@splice` operator; and updates coming
 * from the domain will also provide precise changes using `@splice`, as
 * follows. The overall effect is that the string property can be manipulated
 * concurrently by multiple users with the result being a merge of their edits
 * (rather than an array of conflicting string values, as would otherwise be the
 * case).
 *
 * The extension should be declared at runtime in the data using {@link declare},
 * or provided (combined with other plugins) during clone initialisation, e.g.
 * for a hypothetical `docText` property:
 *
 * ```javascript
 * const meld = await clone(new MemoryLevel, IoRemotes, config, combinePlugins([
 *   new TSeqText('docText'), ...
 * ]));
 * ```
 *
 * Once installed, a new document with text could be inserted:
 *
 * ```javascript
 * await meld.write({ '@id': 'myDoc', docText: 'Initial text' });
 * ```
 *
 * Changes to the document should be written using splice expressions in an `@update`:
 *
 * ```javascript
 * await meld.write({ '@update': { '@id': 'myDoc', docText: { '@splice': [0, 7, 'My'] } });
 * ```
 *
 * This update will be echoed by the local clone, also using the `@splice` operator.
 *
 * This update changes the text "Initial" to "My". If a remote user updates
 * "text" at position 8 to "words", at the same time, the update notification at
 * _this_ clone will correctly identify the change as happening at index
 * position 3. Thus both clones will converge to "My words".
 *
 * To generate splices, applications may consider the utility function {@link textDiff}.
 *
 * To apply splices, applications may consider using {@link updateSubject}.
 *
 * [[include:live-code-setup.script.html]]
 * [[include:how-to/domain-setup.md]]
 * [[include:how-to/text.md]]
 *
 * @see TSeq
 * @category Experimental
 * @experimental
 * @noInheritDoc
 */
export class TSeqText
  implements MeldPlugin, SharedDatatype<TSeq, TSeqOperation>, ExtensionSubjectInstance {
  /**
   * Extension declaration. Insert into the domain data to install the
   * extension. For example (assuming a **m-ld** `clone` object and a property
   * `docText` in the domain):
   *
   * ```typescript
   * clone.write(TSeqText.declare(0, 'docText'));
   * ```
   *
   * @param priority the preferred index into the existing list of extensions
   * (lower value is higher priority).
   * @param properties the properties to which to apply TSeq text behaviour
   */
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
  /** @internal */
  properties: Set<string>;

  /**
   * The class constructor should only be used if declaring the extension in the
   * call to `clone`, i.e. at design time. To apply the extension at runtime,
   * declare it in the data using {@link declare}.
   *
   * @param properties the properties to target
   */
  constructor(...properties: Iri[]) {
    this.properties = new Set(properties);
  }

  /** @internal */
  setExtensionContext({ config, context }: MeldAppContext) {
    this.pid = config['@id'];
    // Expand any property terms given in the constructor
    this.properties = new Set([...this.properties].map(
      p => context.expandTerm(p, { vocab: true })));
  }

  /** @internal */
  initFromData(src: GraphSubject, _orm: OrmUpdating, ext: ExtensionSubject<this>) {
    ext.initSrcProperty(src, [this, 'properties'], // not used due to get/set
      JsProperty.for(SH.targetObjectsOf, Array, VocabReference), {
        get: () => [...this.properties].map(p => ({ '@vocab': p })),
        set: (v: VocabReference[]) => this.properties = new Set(v.map(ref => ref['@vocab']))
      });
  }

  /** @internal */
  indirectedData(datatype: Iri, property: Iri) {
    if (datatype === this['@id'] ||
      (this.properties.has(property) && datatype === XS.string))
      return this;
  }

  /** @internal */
  readonly '@id' = M_LD.EXT.extensionType('tseq', 'TSeq');

  /** @internal */
  validate(value: unknown) {
    if (typeof value == 'string') {
      const tSeq = new TSeq(this.pid);
      tSeq.splice(0, 0, value);
      return tSeq;
    }
  }

  /** @internal */
  toValue(data: TSeq) {
    return { '@type': XS.string, '@value': data.toString() };
  }

  /** @internal */
  getDataId(): UUID {
    return uuid();
  }

  /** @internal */
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

  /** @internal */
  fromJSON(json: any): TSeq {
    return TSeq.fromJSON(this.pid, json);
  }

  /** @internal */
  toJSON(data: TSeq): any {
    return data.toJSON();
  }

  /** @internal */
  update(state: TSeq, update: Expression): [TSeq, TSeqOperation] {
    if (isConstraint(update)) {
      for (let [key, args] of Object.entries(update)) {
        switch (key) {
          case '@concat':
            // noinspection SuspiciousTypeOfGuard
            if (typeof args == 'string')
              return [state, state.splice(Infinity, 0, args)];
            break;
          case '@splice':
            if (isTextSplice(args)) {
              const [index, deleteCount, content] = args;
              return [state, state.splice(index, deleteCount, content)];
            }
        }
      }
    }
    throw new RangeError(`Invalid update expression: ${update}`);
  }

  /** @internal */
  apply(
    state: TSeq,
    reversions: [TSeqOperation][], // TODO
    operation: TSeqOperation
  ): [TSeq, Expression[]] {
    const splices = state.apply(operation)
      .map<Constraint>(splice => ({ '@splice': array(splice) }));
    return [state, splices];
  }

  /** @internal */
  fuse([op1]: [TSeqOperation], [op2]: [TSeqOperation]): [TSeqOperation] {
    return [TSeqOperable.concat(op1, op2)];
  }

  /** @internal */
  cut(prefix: TSeqOperation, operation: TSeqOperation): TSeqOperation | undefined {
    return TSeqOperable.rmPrefix(prefix, operation);
  }
}