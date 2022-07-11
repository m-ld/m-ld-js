/**
 * This declaration file is stored as .ts so that it is emitted
 */

import { Context, Url } from 'jsonld/jsonld-spec';
import { ActiveContext } from 'jsonld/lib/context';

declare module 'jsonld' {
  namespace Options {
    export interface CompactIri {
      value?: any, // TODO
      relativeTo?: { vocab: boolean; },
      reverse?: boolean,
      base?: Url
    }
  }

  function processContext(
    activeCtx: ActiveContext,
    localCtx: Context,
    options: Options.DocLoader):
    Promise<ActiveContext>;
}