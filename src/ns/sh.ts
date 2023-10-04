export const $vocab = 'http://www.w3.org/ns/shacl#';

export const targetClass = `${$vocab}targetClass`;
export const targetObjectsOf = `${$vocab}targetObjectsOf`;
export const path = `${$vocab}path`;
export const name = `${$vocab}name`;
export const minCount = `${$vocab}minCount`;
export const maxCount = `${$vocab}maxCount`;

export enum ConstraintComponent {
  MinCount = 'http://www.w3.org/ns/shacl#MinCountConstraintComponent',
  MaxCount = 'http://www.w3.org/ns/shacl#MaxCountConstraintComponent'
}