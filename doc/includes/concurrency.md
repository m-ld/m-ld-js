## Concurrency
A **m-ld** clone contains realtime domain data
[in&nbsp;principle](https://m-ld.org/doc/#realtime). This means that any clone
operation may be occurring [concurrently](https://m-ld.org/doc/#concurrency)
with operations on other clones, and these operations combine to realise the
final convergent state of every clone.

The Javascript clone engine API supports bounded procedures on immutable state,
for situations where a query or other data operation may want to operate on a
state that is unaffected by concurrent operations. In general this means that in
order to guarantee data consistency, it is not necessary for the app to use the
clone's local clock ticks (which nevertheless appear in places in the API for
consistency with other engines).

An immutable state can be obtained using the
[read](interfaces/meldclone.html#read) and
[write](interfaces/meldclone.html#write) methods. The state is passed to a
procedure which operates on it. In both cases, the state remains immutable until
the procedure's returned Promise resolves or rejects.

In the case of `write`, the state can be transitioned to a new state from within
the procedure using its own [write](interfaces/meldstate.html#write) method,
which returns a new immutable state.

In the case of `read`, changes to the state following the procedure can be
listened to using the second parameter, a handler for new states. As well as
each update in turn, this handler also receives an immutable state following the
given update.

### Example: Initialising an App
```typescript
await clone.read(async (state: MeldReadState) => {
  // State in a procedure is locked until sync complete or returned promise resolves
  let currentData = await state.read(someQuery);
  populateTheUi(currentData);
}, async (update: MeldUpdate, state: MeldReadState) => {
  // The handler happens for every update after the proc
  // State in a handler is locked until sync complete or returned promise resolves
  updateTheUi(update); // See §Handling Updates, below
});
```

### Example: UI Action Handler
```typescript
ui.on('action', async () => {
  clone.write(async (state: MeldState) => {
    let currentData = await state.read(something);
    let externalStuff = await doExternals(currentData);
    let todo = decideWhatToDo(externalStuff);
    // Writing to the current state creates a new live state
    state = await state.write(todo);
    await checkStuff(state);
  });
});
```

### Example: UI Show Handler
```typescript
ui.on('show', () => {
  clone.read(async (state: MeldReadState) => {
    let currentData = await state.read(something);
    showTheNewUi(currentData);
  });
});
```
