declare module 'mqtt-pattern' {
  interface TopicParams {
    [key: string]: string | string[];
  }

  /**
   * Validates that topic fits the pattern and parses out any parameters.
   * If the topic doesn't match, it returns null.
   * @param pattern topic pattern with optionally named wildcards
   * @param topic observed topic to match
   */
  function exec(pattern: string, topic: string): TopicParams | null;
  /**
   * Validates whether topic fits the pattern. Ignores parameters.
   * @param pattern topic pattern with optionally named wildcards
   * @param topic observed topic to match
   */
  function matches(pattern: string, topic: string): boolean;
  /**
   * Traverses the pattern and attempts to fetch parameters from the topic.
   * Useful if you know in advance that your topic will be valid and want to extract data.
   * If the topic doesn't match, or the pattern doesn't contain named wildcards, returns an empty object.
   * Do not use this for validation.
   * @param pattern topic pattern with optionally named wildcards
   * @param topic observed topic to match
   */
  function extract(pattern: string, topic: string): TopicParams;
  /**
   * Reverse of extract, traverse the pattern and fill in params with keys in an object.
   * Missing keys for + params are set to undefined.
   * Missing keys for # params yeid empty strings.
   * @param pattern topic pattern with optionally named wildcards
   * @param params topic parameters to fill into the pattern
   */
  function fill(pattern: string, params: TopicParams): string;
  /**
   * Removes the parameter names from a pattern.
   * @param pattern topic pattern with optionally named wildcards
   */
  function clean(pattern: string): string;
}