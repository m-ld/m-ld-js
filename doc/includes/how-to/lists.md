```js
import { updateSubject } from 'https://js.m-ld.org/ext/index.mjs';

document.addEventListener('domainChanged', async () => {
  shoppingList.innerHTML = '';
  const { state, genesis } = window.model;
  if (genesis) {
    // Write some initial shopping items to the state
    state.write({
      '@id': 'shopping',
      '@list': ['bread', 'milk']
    });
  }
  // To use updateSubject for updating the DOM, we use a Javascript object-like
  // proxy pattern over the relevant Elements.
  const shopping = {
    '@id': 'shopping',
    '@list': {
      // For a List, updateSubject can apply the update to anything with a
      // `length` and an Array-like `splice` method.
      get length() {
        return shoppingList.childElementCount;
      },
      splice(index, deleteCount, ...items) {
        for (let i = 0; i < deleteCount; i++)
          shoppingList.children[index]?.remove();
        const { el, position } = index < this.length ?
          { el: shoppingList.children[index], position: 'beforebegin' } :
          { el: shoppingList, position: 'beforeend' };
        for (let item of items)
          el.insertAdjacentHTML(position, `<li>${item}</li>`);
      }
    }
  };

  state.read(
    async state => updateSubject(shopping, await state.get('shopping')),
    update => updateSubject(shopping, update)
  );
});

addItem.addEventListener('click', () => {
  window.model.state.write({
    '@id': 'shopping',
    // When writing list items, we can use an object with integer keys instead
    // of an array. Here we're inserting at the end of the list.
    '@list': { [shoppingList.childElementCount]: itemToAdd.value }
  });
});

removeItem.addEventListener('click', () => {
  window.model.state.write({
    '@delete': {
      '@id': 'shopping',
      // When deleting list items, we can pattern-match using variables. Here,
      // we want to delete the removed item wherever it appears in the list.
      '@list': { '?': itemToRemove.value }
    }
  });
});
```
```html
<div>
  <h2>Shopping</h2>
  <ol id="shoppingList"></ol>
  <p>
    <input id="itemToAdd" type="text" placeholder="new shopping item"/>
    <button id="addItem">+ Add</button>
  </p>
  <p>
    <input id="itemToRemove" type="text" placeholder="shopping item to remove"/>
    <button id="removeItem">- Remove</button>
  </p>
  <hr/>
</div>
```
<script>new LiveCode('domain-setup', document.currentScript).link();</script>
