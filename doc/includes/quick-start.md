## Quick Start

Let's create a domain of live shared information.

```js
import { clone, uuid } from 'https://js.m-ld.org/ext/index.mjs';
import { MemoryLevel } from 'https://js.m-ld.org/ext/memory-level.mjs';
import { IoRemotes } from 'https://js.m-ld.org/ext/socket.io.mjs';

const domainName = `${uuid()}.public.gw.m-ld.org`;

const meld = await clone(new MemoryLevel(), IoRemotes, {
  '@id': uuid(),
  '@domain': domainName,
  genesis: true,
  io: { uri: "https://gw.m-ld.org" }
});

successDiv.removeAttribute('hidden');
domainInput.value = domainName;

meld.follow(update => {
  for (let { name } of update['@insert'])
    successDiv.insertAdjacentHTML('beforeend',
      `<p>Welcome ${name}!</p>`);
});
```
```html
<div id="successDiv" hidden>
  <p>🎉 Your new domain is at</p>
  <input id="domainInput" type="text" onfocus="this.select()" style="width:100%;"/>
</div>
```
<script>liveCode(document.currentScript);</script>

The new domain's information is stored in memory here (and only here). It's . But there aren't any yet. Let's make one.

```js
import { clone, uuid } from 'https://js.m-ld.org/ext/index.mjs';
import { MemoryLevel } from 'https://js.m-ld.org/ext/memory-level.mjs';
import { IoRemotes } from 'https://js.m-ld.org/ext/socket.io.mjs';

cloneButton.addEventListener('click', async () => {
  const meld = await clone(new MemoryLevel(), IoRemotes, {
    '@id': uuid(),
    '@domain': domainInput.value,
    io: { uri: 'https://gw.m-ld.org' }
  });
  
  playgroundAnchor.setAttribute('href', `https://edge.m-ld.org/playground/#domain=${domainInput.value}&txn=%7B%22name%22%3A%22George%22%7D`);
  clonedDiv.removeAttribute('hidden');
  
  nameInput.addEventListener('keydown', e => {
    if (e.key === 'Enter')
      meld.write({ name: nameInput.value });
  });
});
```
```html
<div>
  <p>Paste the domain name here:</p>
  <input id="domainInput" type="text" style="width:100%;"/>
  <button id="cloneButton">Clone</button>
</div>
<div id="clonedDiv" hidden>
  <p>🎉 You have cloned the domain!</p>
  <p>Please enter your name: <input id="nameInput" type="text"/></p>
  <p>You can also interact with this domain in the <a id="playgroundAnchor" target="_blank"><b>m-ld</b> playground</a>!</p>
</div>
```
<script>liveCode(document.currentScript);</script>

These two code blocks are sandboxed – they are only sharing the domain, using **m-ld**. Because the domain is using a [public Gateway](https://gw.m-ld.org) (gw.m-ld.org) to connect to other clones, the **m-ld** playground can also see it.

> 💡 **m-ld** domain names look like IETF internet domains, and have the same rules. The internet doesn't know how to look them up yet though, so you can't just paste one into a browser.
