```js
import { updateSubject } from 'https://js.m-ld.org/ext/index.mjs';
import { ShapeConstrained, PropertyShape } from 'https://js.m-ld.org/ext/shacl.mjs';

document.addEventListener('domainChanged', async () => {
  if (window.model.genesis && false) { // 1️⃣
    await window.model.state.write(
      ShapeConstrained.declare(0, PropertyShape.declare({
        path: 'name', count: 1
      }))
    );
  }
  const author = {
    '@id': 'author',
    // Naive UI ↔︎ data mapping, don't do this! 2️⃣
    set name(name) { nameInput.value = name; },
    get name() { return nameInput.value.split(',').filter(v => v); }
  };
  await window.model.state.read(
    async state => updateSubject(author, await state.get('author')),
    update => updateSubject(author, update)
  );
  beginEditSession();
});

function beginEditSession() {
  window.model.state.write(state => new Promise(release => { // 3️⃣
    const oldName = nameInput.value;
    nameInput.readOnly = false;
    nameInput.focus();
    editButton.innerText = 'Enter';
    editButton.addEventListener('click', async function enter() {
      if (nameInput.value)
        await state.write({ '@update': { '@id': 'author', name: nameInput.value } });
      else
        nameInput.value = oldName; // Revert
      nameInput.readOnly = true;
      editButton.innerText = 'Edit';
      editButton.removeEventListener('click', enter);
      release();
    });
  }));
}

editButton.addEventListener('click', () => {
  if (nameInput.readOnly)
    beginEditSession();
});
```
```html

<div id="appDiv" hidden>
  <h2>Author</h2>
  <label for="nameInput">Name:</label>
  <input id="nameInput" type="text" readonly/>
  <button id="editButton">Edit</button>
</div>
<template class="help">
  <p>
    This example shows how "conflicts" can arise in user sessions, and one way to change the
    default behaviour of <b>m-ld</b>, using <i>Shapes</i>.
  </p>
  <p>
    In our app, we intend that the "author" subject should have only one "name" property value. Our
    user interface presents the name, and allows us to change it using the Edit button. However, if
    another user simultaneously changes the name, it's possible for the author to end up with
    <i>both</i> entered names. (Try it by following the instructions above to duplicate this tab,
    and beginning an edit in both tabs.)
  </p>
  <ul>
    <li>
      1️⃣ Here we declare that the "name" property should have only one value. When you have
      explored the behaviour without this fix, change <code>false</code> to <code>true</code>
      in this line, and try again with a new domain.
    </li>
    <li>
      2️⃣ Here we are relying on the behaviour of an HTML text input element – if you set its value
      to an array, it will separate the array values with a comma. This won't work as expected if
      the name you enter has a comma in it, so a more robust approach would be needed in a real app.
    </li>
    <li>
      3️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldstatemachine.html#write">"state procedure"</a>
      allows us to prevent <b>m-ld</b> from accepting remote updates until the returned promise
      settles. This means that we don't see the effect of a concurrent edit until our editing
      "session" is finished.
    </li>
  </ul>
</template>
```
```css
#nameInput[readonly] {
  border: none;
}
```
<script>new LiveCode('domain-setup', document.currentScript).link();</script>
