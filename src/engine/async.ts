/**
 * Library override
 */

import * as async from 'asynciterator';

/** @see https://github.com/RubenVerborgh/AsyncIterator/issues/96 */
async.setTaskScheduler(require('queue-microtask'));

// This awkward export is to prevent WebStorm from flagging import
// simplification warnings that would bypass this module
export default async;