```js
import { clone, uuid } from 'https://js.m-ld.org/ext/index.mjs';
import { MemoryLevel } from 'https://js.m-ld.org/ext/memory-level.mjs';
import { IoRemotes } from 'https://js.m-ld.org/ext/socket.io.mjs';

// m-ld extensions are loaded using their package identity (@m-ld/m-ld/ext/..).
// In a real app, this redirection should be done with an import map.
globalThis.require = module => import(module
  .replace(/@m-ld\/m-ld\/ext\/(\w+)/, 'https://js.m-ld.org/ext/$1.mjs'));

globalThis.changeDomain = async function (domain) {
  const genesis = !domain;
  if (genesis)
    domain = `${uuid()}.public.gw.m-ld.org`;
  if (window.model) {
    await window.model.state.close();
    delete window.model;
  }
  const state = await clone(new MemoryLevel(), IoRemotes, {
    '@id': uuid(),
    '@domain': domain,
    genesis,
    io: { uri: "https://gw.m-ld.org" }
  });
  // Uncomment the next line to log individual updates as they come in
  //state.follow(update => console.info('UDPATE', JSON.stringify(update)));
  domainInput.value = domain;
  appDiv.hidden = false;
  playgroundAnchor.setAttribute('href', `https://m-ld.org/playground/#domain=${domain}`);
  // Store the "model" as a global for access by other scripts, and tell them
  window.model = { domain, state, genesis };
  document.dispatchEvent(new Event('domainChanged'));
}

/**
 * Utility to populate a template. Returns an object containing the cloned
 * children of the template, also indexed by tagName and classname.
 */
globalThis.templated = template => new Proxy({ $: template.content.cloneNode(true) }, {
  get: (t, p) => t[p] ?? t.$.querySelector(p) ?? t.$.querySelector(`.${p}`)
});

document.querySelectorAll('.help').forEach(help => helpDetails.appendChild(templated(help).$));
```
```html
<div>
  <a id="playgroundAnchor" target="_blank" title="go to playground">🛝</a>
  <input id="domainInput" type="text" placeholder="domain name" onfocus="this.select()"/>
  <button onclick="changeDomain(domainInput.value)">Join</button>
  <button onclick="changeDomain()">New ⭐️</button>
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
