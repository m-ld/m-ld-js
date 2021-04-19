import { SingleValued, SingleValuedConfig } from './SingleValued';
import { Context, Reference } from '../jrql-support';
import { MeldConstraint } from '..';
import { compact } from 'jsonld';

/**
 * Configuration of the clone data constraint. The supported constraints are:
 * - `single-valued`: the given property should have only one value. The
 *   property can be given in unexpanded form, as it appears in JSON subjects
 *   when using the API, or as its full IRI reference.
 *
 * > 🚧 *Please [contact&nbsp;us](https://m-ld.org/hello/) to discuss data
 * > constraints required for your use-case.*
 */
export type ConstraintConfig = SingleValuedConfig;

/** @internal */
const PROPERTY_CONTEXT = {
  'mld:#property': { '@type': '@vocab' },
  property: { '@id': 'mld:#property', '@type': '@vocab' }
}

/** @internal */
export async function constraintFromConfig(config: ConstraintConfig, context: Context): Promise<MeldConstraint> {
  switch (config['@type']) {
    case 'single-valued':
      // This resolves the 'property' property against the vocabulary of the
      // context, allowing for an existing override of 'property'.
      // See https://tinyurl.com/y3gaf6ak
      const resolved = <{ 'mld:#property': Reference }>
        await compact({ ...config, '@context': [PROPERTY_CONTEXT, context] }, {});
      return new SingleValued(resolved['mld:#property']['@id']);

    default:
      throw new Error('Constraint configuration cannot be read');
  }
}