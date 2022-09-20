import { getLogger, getLoggers, LogLevelDesc } from 'loglevel';

export function getIdLogger(ctor: Function, id: string, logLevel: LogLevelDesc = 'info') {
  const loggerName = `${ctor.name}.${id}`;
  const loggerInitialised = loggerName in getLoggers();
  const log = getLogger(loggerName);
  if (!loggerInitialised) {
    const originalFactory = log.methodFactory;
    log.methodFactory = (methodName, logLevel, loggerName) => {
      const method = originalFactory(methodName, logLevel, loggerName);
      return (...msg: any[]) => method.apply(
        undefined, [new Date().toISOString(), id, ctor.name].concat(msg));
    };
  }
  log.setLevel(logLevel);
  return log;
}