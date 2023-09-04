import { expect } from '@jest/globals';
import { SubjectGraph } from '../src/engine/SubjectGraph';

expect.addEqualityTesters([function (a: unknown, b: unknown, customTesters) {
  if (a instanceof SubjectGraph)
    return this.equals([...a], b, customTesters);
  else if (b instanceof SubjectGraph)
    return this.equals(a, [...b], customTesters);
}]);