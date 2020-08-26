## Concurrency
A **m-ld** clone contains realtime domain data
[in&nbsp;principle](https://m-ld.org/doc/#realtime). This means that any clone
operation may be occurring [concurrently](https://m-ld.org/doc/#concurrency)
with operations on other clones, and these operations combine to realise the
final convergent state of every clone.

JavaScript's
[concurrency&nbsp;model](https://developer.mozilla.org/en-US/docs/Web/JavaScript/EventLoop)
is "based on an event loop, which is responsible for executing the code,
collecting and processing events, and executing queued sub-tasks." This means
that during execution of a Javascript function, there is a general guarantee
that no other Javascript function is running at the same time and able to affect
the current function's view of the appliction state. In addition, the event
loop's queueing mechanisms can also extend similar properties to the execution
of some callbacks.

The Javascript clone engine takes advantage of this model in its API, in
situations where a query or other data operation may want to operate on a state
that is unaffected by concurrent operations. In general this means that in order
to guarantee data consistency, it is not necessary for the app to use the
clone's local clock `ticks` (which nevertheless appear in places in the API for
consistency with other engines).

Specifically:
- On receipt of a clone [status](/classes/meldapi.html#status) update, no
  further data operations will have occurred until the receiver yields back to
  the event loop. This means that after `await`ing a specific status, e.g.
  ```typescript
  await meld.status.becomes({ outdated: false })
  ```
  it is valid to immediately call
  ```typescript
  meld.follow().subscribe(update => { /* ... */ })
  ```
  with the assurance that no live updates will be missed.
- On receipt of a data update from the [follow](/classes/meldapi.html#follow)
  method, no further data operations will have occurred until the receiver
  yields back to the event loop. This means that an immediate call to
  [transact](/classes/meldapi.html#transact) with a *read* (see below) is
  assured to operate on domain data with the update (and no subsequent update)
  applied.
- Transactions operate atomically on a consistent state. However:
  1. For a *write*, there may be a delay between the call to the `transact`
     method and the execution tick of the transaction, and
  1. Results of a *read* transaction are streamed to the caller asynchronously.
  
  During both of these asychronous phases, other transactions can occur, arising
  from other clones. In some cases it is necessary for an app to react to the
  precise state of the transaction outcome. The return type of the
  [transact](/classes/meldapi.html#transact) method includes a `tick` Promise,
  on resolution of which the state is assured to be unchanged.

  A common use for this is to start following clone updates after some initial
  query, like this:
  ```typescript
  const result = meld.transact({
    '@describe': '?s', '@where': { '@id': '?s', '@type': 'Booking' }
  });
  // No new Bookings will be missed by the follow subscription
  result.tick.then(() => meld.follow().subscribe(
    update => { /* react to booking changes */ }));
  result.subscribe(booking => { /* add the booking to the UI */ });
  ```