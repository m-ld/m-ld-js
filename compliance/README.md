# To Run Compliance Checks
- `npm run compliance` (in project top-level)

DEBUG logging:
- `LOG_LEVEL=debug npm run compliance`

Logs are output to `./compliance/.log`.

If `LOG_LEVEL=trace`, per-clone performance timings will also be output to `./logs` as csv files.

Specific compliance specs by glob (see [m-ld-spec/compliance](https://github.com/m-ld/m-ld-spec/tree/master/compliance)):
- `npm run compliance -- "2-*/2-*"` or just `npm run compliance -- "2/2"`

For IDE integration, `compliance/test.js` can also be run with the Jasmine 
command line options `reporter` and `filter` e.g.
```
node compliance/test.js 2/2 "--filter=at least one" "--reporter=jasmine-ts-console-reporter"
```

Experimental features in this engine may have tests in the `./test` folder. Run these using `.` as the root path, i.e.
- `npm run compliance -- "./*"`