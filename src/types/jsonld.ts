/**
 * This declaration file is stored as .ts so that it is emitted
 */

import { Context, Url } from '@m-ld/jsonld';
import { ActiveContext } from '@m-ld/jsonld/lib/context';

declare module '@m-ld/jsonld' {
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