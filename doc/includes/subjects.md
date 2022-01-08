## Subjects

Subjects in the Javascript engine are accepted and presented as plain Javascript objects whose content is JSON-LD (see the **m-ld** [Specification](https://spec.m-ld.org/#subjects)). Utilities are provided to help the app use and produce valid subjects.

- [includeValues](#includevalues) includes one or more given value in a subject property.
- [includesValue](#includesvalue) determines whether the given subject property contains the given value.
- [propertyValue](#propertyvalue) gets a property value from the given subject, casted as a strict type.

### Handling Updates

Clone [updates](interfaces/meldupdate.html) obtained from a read handler specify the exact Subject property values that have been deleted or inserted during the update. Since apps often maintain subjects in memory, for example in a user interface, utilities are provided to help update these in-memory subjects based on updates:

- [updateSubject](#updatesubject) applies an update to the given subject in-place.
- [SubjectUpdater](/classes/subjectupdater.html) applies an update to more than one subject.
- [asSubjectUpdates](#assubjectupdates) provides an alternate view of the update deletes and inserts, by Subject, for custom processing.
