## Security

To mitigate integrity, confidentiality and availability threats to **m-ld** domain data, we recommend the following baseline security controls for your app.

1. Apply secure application development practices, for example as recommended in the UK [National Cyber Security Centre Guidelines](https://www.ncsc.gov.uk/collection/developers-collection).
2. Apply operating-system access control to the storage location used for [persistence](#data-persistence), restricting access to authorised users of the app and administrators.
3. Apply authorisation controls to [remote connections](#connecting-to-other-clones), restricting access to authorised users of the app. This typically requires users to be authenticated in the app prior to connecting the **m-ld** clone. Authentication credentials can be passed to the remotes implementation via the configuration object.
4. Encrypt remote connection transports, for example by applying transport layer security (TLS) to server connections, if applicable.

A more general discussion of security considerations in **m-ld** can be found on the [website](https://m-ld.org/doc/#security).

> 🧪 This library additionally includes an experimental extension for controlling access based on an Access Control List (ACL) in the domain data. Please see our [Security Project](https://github.com/m-ld/m-ld-security-spec) for more details of our ongoing security research, and [contact us](https://m-ld.org/hello/) to discuss your security requirements!