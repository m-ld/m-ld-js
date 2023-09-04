```js
import { clone, uuid } from 'https://js.m-ld.org/ext/index.mjs';
import { MemoryLevel } from 'https://js.m-ld.org/ext/memory-level.mjs';
import { IoRemotes } from 'https://js.m-ld.org/ext/socket.io.mjs';

// m-ld extensions are loaded using their package identity (@m-ld/m-ld/ext/..).
// In a real app, this redirection should be done with an import map.
globalThis.require = module => import(module
  .replace(/@m-ld\/m-ld\/ext\/(\w+)/, 'https://js.m-ld.org/ext/$1.mjs'));

async function changeDomain(domainName) {
  const genesis = !domainName;
  if (genesis)
    domainName = `${uuid()}.public.gw.m-ld.org`;
  if (window.model)
    await window.model.state.close();
  const state = await clone(new MemoryLevel(), IoRemotes, {
    '@id': uuid(),
    '@domain': domainName,
    genesis,
    io: { uri: "https://gw.m-ld.org" }
  });
  domainInput.value = domainName;
  playgroundAnchor.setAttribute('href', `https://m-ld.org/playground/#domain=${domainName}`);
  // Store the "model" as a global for access by other scripts, and tell them
  window.model = { state, genesis };
  document.dispatchEvent(new Event('domainChanged'));
}

joinDomainButton.addEventListener('click', () => changeDomain(domainInput.value));
newDomainButton.addEventListener('click', () => changeDomain());
```
```html
<div>
  <a id="playgroundAnchor" target="_blank" title="go to playground">🛝</a>
  <input id="domainInput" type="text" placeholder="domain name" onfocus="this.select()"/>
  <button id="joinDomainButton">Join</button>
  <button id="newDomainButton">New ⭐️</button>
  <hr/>
</div>
```
<script title="domain-setup">new LiveCode(document.currentScript).declare();</script>
