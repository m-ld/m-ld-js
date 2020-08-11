import { CheckList } from './CheckList';
import { SingleValued } from './SingleValued';
import { Context, Reference } from '../dataset/jrql-support';
import { MeldConstraint } from '../m-ld';
import { compact } from 'jsonld';

export type ConstraintConfig = {
  '@type': 'checklist';
  list: ConstraintConfig[];
} | {
  '@type': 'single-valued';
  property: string;
}

export const NO_CONSTRAINT = new CheckList([]);

const PROPERTY_CONTEXT = {
  'mld:#property': { '@type': '@vocab' },
  property: { '@id': 'mld:#property', '@type': '@vocab' }
}

export async function constraintFromConfig(config: ConstraintConfig, context: Context): Promise<MeldConstraint> {
  switch (config['@type']) {
    case 'checklist':
      const list = await Promise.all(config.list.map(item => constraintFromConfig(item, context)));
      return new CheckList(list);

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