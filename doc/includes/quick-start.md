## Quick Start

The live code app below creates a domain of shared information, and then follows updates to it. In its user interface, it just presents the domain name. (You can see and edit the code by clicking the <svg viewBox="0 0 1024 1024" style="height: 1em;"><path d="M295 590.5l-96 246.6 242.3-100.3L295 590.5zM998.5 63.8l-30.7-30.7c-33.9-34-89-34-123 0l-61.5 61.5L937 248.4l61.5-61.5c34-34 34-89.1 0-123.1zM299.9 578l22-22 153.7 153.7 430.6-430.6-153.7-153.7 30.7-30.8L299.9 578zM831 929.6l-740.5.3.1-740.5 518.5-.2 81.2-86.1H46.4c-24 0-43.5 19.4-43.5 43.5V973c0 24.1 19.5 43.5 43.5 43.5h826.4c24.1 0 43.5-19.4 43.5-43.5V361.5l-87.6 86 2.3 482.1z"></path></svg> button, left-middle.)

```js
import { clone, uuid } from 'https://js.m-ld.org/ext/index.mjs';
import { MemoryLevel } from 'https://js.m-ld.org/ext/memory-level.mjs';
import { IoRemotes } from 'https://js.m-ld.org/ext/socket.io.mjs';

const domainName = `${uuid()}.public.gw.m-ld.org`;

const state = await clone(new MemoryLevel(), IoRemotes, {
  '@id': uuid(),
  '@domain': domainName,
  genesis: true,
  io: { uri: "https://gw.m-ld.org" }
});

successDiv.hidden = false;
domainInput.value = domainName;

for await (let [update] of state.follow()) {
  for (let { name } of update['@insert'])
    successDiv.insertAdjacentHTML('beforeend',
      `<h2>Welcome, ${name}!</h2>`);
}
```
```html
<div id="successDiv" hidden>
  <p>🎉 Your new domain is at</p>
  <input id="domainInput" type="text" onfocus="this.select()" style="width:100%;"/>
</div>
```
<script>new LiveCode(document.currentScript).inline({ middle: 0 });</script>

The new domain's information is stored in memory here (and only here). The next live code app, below, allows us to make another clone of the domain. These two code blocks are **sandboxed** – they do not have access to each other's state via this browser.

> 💡 You can confirm this by trying it in another window (use the <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1024 1024" style="height: 1em;"><path d="M462.9 192.5v504.3h100V192.5L739.5 369l70.7-70.7L512.9 1 215.6 298.3l70.7 70.7z"></path><path d="M924 450v405.3c0 37.9-30.8 68.7-68.7 68.7H168.7c-37.9 0-68.7-30.8-68.7-68.7V450H0v405.3C0 948.5 75.5 1024 168.7 1024h686.6c93.2 0 168.7-75.5 168.7-168.7V450H924z"></path></svg> button, top right), another browser, or another device... anywhere in the world!

```js
import { clone, uuid } from 'https://js.m-ld.org/ext/index.mjs';
import { MemoryLevel } from 'https://js.m-ld.org/ext/memory-level.mjs';
import { IoRemotes } from 'https://js.m-ld.org/ext/socket.io.mjs';

cloneButton.addEventListener('click', async () => {
  const state = await clone(new MemoryLevel(), IoRemotes, {
    '@id': uuid(),
    '@domain': domainInput.value,
    io: { uri: 'https://gw.m-ld.org' }
  });
  
  playgroundAnchor.setAttribute('href', `https://edge.m-ld.org/playground/#domain=${domainInput.value}&txn=%7B%22name%22%3A%22George%22%7D`);
  clonedDiv.removeAttribute('hidden');
  
  const submitName = () => state.write({ name: nameInput.value });
  submitNameButton.addEventListener('click', submitName);
  nameInput.addEventListener('keydown', e => {
    if (e.key === 'Enter')
      submitName();
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
  <p>Please enter your name:
    <input id="nameInput" type="text"/>
    <button id="submitNameButton">Submit</button>
  </p>
  <p>You can also interact with this domain in the <a id="playgroundAnchor" target="_blank"><b>m-ld</b> playground</a>!</p>
</div>
```
<script>new LiveCode(document.currentScript).inline({ middle: 0 });</script>

The domain is using a [public Gateway](https://gw.m-ld.org) (gw.m-ld.org) to connect clones together. This means the **m-ld** playground can also see it.

> 💡 **m-ld** domain names look like IETF internet domains, and have the same rules. The internet doesn't know how to look them up yet though, so you can't just paste one into a browser.
