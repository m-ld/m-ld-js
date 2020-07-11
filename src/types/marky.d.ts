/**
 * JavaScript timer based on performance.mark() and performance.measure(),
 * providing high-resolution timings as well as nice Dev Tools visualizations.
 *
 * For browsers that don't support performance.mark(), it falls back to
 * performance.now() or Date.now(). In Node, it uses process.hrtime().
 * 
 * @see https://github.com/nolanlawson/marky#api
 */
declare module 'marky' {
  /**
   * begins recording
   */
  export function mark(name: string): void;
  /**
   * finishes recording
   */
  export function stop(name: string): PerformanceEntry;
}