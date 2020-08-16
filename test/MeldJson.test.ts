import { hashTriple } from '../src/engine/MeldJson';
import { namedNode, triple, literal } from '@rdfjs/data-model';
import { Quad } from 'rdf-js';

  ////////////////////////////////////////////////////////////////////////////////////////////////////
  // Canonical hash tests - see m-ld/m-ld-jena/src/test/java/org/m_ld/jena/JenaDeltaTest.java

test('Hashing a canonical triple with named node object', () => {
  expect(hashTriple(triple<Quad>(
    namedNode('http://test.m-ld.org/subject'),
    namedNode('http://test.m-ld.org/predicate'),
    namedNode('http://test.m-ld.org/object'))).encode())
    .toBe('nKUIBgeO+imiKAsdGDAp6eYlEa4XW2xs7OW9rCdz0EM=');
});

test('Hashing a canonical triple with literal object', () => {
  expect(hashTriple(triple<Quad>(
    namedNode('http://test.m-ld.org/subject'),
    namedNode('http://test.m-ld.org/predicate'),
    literal('value'))).encode())
    .toBe('AM5pP6j9FiIYvpCEa0CAM2yUxAPVQoUMyxgTjmsSyro=');
});

test('Hashing a canonical triple with literal language object', () => {
  expect(hashTriple(triple<Quad>(
    namedNode('http://test.m-ld.org/subject'),
    namedNode('http://test.m-ld.org/predicate'),
    literal('value', 'en-us'))).encode())
    .toBe('Bt3A7zDDbNFnyQd0+06w943yr9NL/OIY2vA4v91w3O8=');
});

test('Hashing a canonical triple with typed literal object', () => {
  expect(hashTriple(triple<Quad>(
    namedNode('http://test.m-ld.org/subject'),
    namedNode('http://test.m-ld.org/predicate'),
    literal('1', namedNode('http://www.w3.org/2001/XMLSchema#integer')))).encode())
    .toBe('VWCmmv3R9k/9orxE+g45S1bg3TivVAdxg8ftMwWUQ0w=');
});