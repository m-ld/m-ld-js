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

document.querySelectorAll('.help').forEach(helpTemplate =>
  helpDetails.appendChild(helpTemplate.content.cloneNode(true)));

/**
 * Utility to populate a template. Returns an object containing the cloned
 * children of the template indexed by tagName and their first classname.
 */
globalThis.templated = function (template) {
  const index = { content: template.content.cloneNode(true) };
  (function addChildren(node) {
    for (let child of node.children) {
      if (child.classList.length) index[child.classList[0]] ??= child;
      addChildren(index[child.tagName.toLowerCase()] ??= child);
    }
  })(index.content);
  return index;
}
```
```html
<div>
  <a id="playgroundAnchor" target="_blank" title="go to playground">🛝</a>
  <input id="domainInput" type="text" placeholder="domain name" onfocus="this.select()"/>
  <button id="joinDomainButton">Join</button>
  <button id="newDomainButton">New ⭐️</button>
  <details id="helpDetails">
    <summary>🔢 help...</summary>
    <p>This live code demo shows how to share live information with <b>m-ld</b>.</p>
    <p>To get started with a new set of information (a "domain"), click New ⭐️ above. You can then interact with the mini-application below to create some information.</p>
    <p>To share the information with a duplicate of this page:<ol><li>copy the domain name above</li><li>duplicate the browser tab</li><li>paste the domain name into the new page's domain input</li><li>click Join</li></ol></p>
    <p>You can also share with the <b>m-ld</b> playground using the 🛝 button.</p>
  </details>
  <hr/>
</div>
```
<script title="domain-setup">new LiveCode(document.currentScript).declare();</script>
