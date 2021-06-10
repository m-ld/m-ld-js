# To Run Compliance Checks
- `npm run compliance` (in project top-level)

DEBUG logging:
- `LOG_LEVEL=debug npm run compliance`

Logs are output to `./compliance/.log`.

Specific compliance specs by glob (see [m-ld-spec/compliance](https://github.com/m-ld/m-ld-spec/tree/master/compliance)):
- `npm run compliance -- "2-*/2-*"` or just `npm run compliance -- "2/2"`

