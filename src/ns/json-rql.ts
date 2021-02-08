export const $base = 'http://json-rql.org/';

export const item = 'http://json-rql.org/#item'; // Slot item property
export const index = `${$base}#index`; // Entailed slot index property
export const blank = `${$base}#blank`; // Temporary use for processing
export const hiddenVar = (name: string) => `${$base}var#${name}`;
