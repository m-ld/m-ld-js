## Transactions
<!-- Note this section is duplicated from the Pattern type in src/jrql-support.ts -->
A m-ld transaction is a **json-rql** pattern, which represents a data read or a
data write. See the **m-ld**
[specification](https://spec.m-ld.org/#transactions) for a walk-through of the syntax.

Supported pattern types for this engine are (follow the links for examples):
- [Describe](/interfaces/describe.html)
- [Select](/interfaces/select.html)
- [Group](/interfaces/group.html) or [Subject](/interfaces/subject.html) (the shorthand way to insert data)
- [Update](/interfaces/update.html) (the longhand way to insert or delete data)

> 🚧 *If you have a requirement for an unsupported pattern, please
> [contact&nbsp;us](mailto:info@m-ld.io) to discuss your use-case.* You can
> browse the full **json-rql** syntax at
> [json-rql.org](http://json-rql.org/).

The [MeldClone](/classes/meldclone.html) object returned by the `clone` function
also augments the basic clone API with convenience methods to simplify common
cases.
