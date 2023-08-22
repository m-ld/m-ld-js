```js
import { updateSubject } from 'https://js.m-ld.org/ext/index.mjs';

document.addEventListener('clone', () => {
  shoppingList.innerHTML = '';
  const { state, genesis } = window.model;
  if (genesis) {
    state.write({
      '@id': 'shopping',
      '@list': ['bread', 'milk']
    });
  }
  state.follow(update => {
    updateSubject({
      '@id': 'shopping',
      '@list': {
        // The updateSubject utility can apply the `update` for a List to
        // anything with a `length` and a `splice` method. So we use a proxy-
        // like object that modifies the shopping list element's children.
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
    }, update);
  });
});

addItem.addEventListener('click', () => {
  const { state } = window.model;
  state.write({
    '@id': 'shopping',
    '@list': { [shoppingList.childElementCount]: itemToAdd.value }
  });
});

removeItem.addEventListener('click', () => {
  const { state } = window.model;
  state.write({
    '@delete': {
      '@id': 'shopping',
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
    <input id="itemToAdd" type="text"/>
    <button id="addItem">+ Add</button>
  </p>
  <p>
    <input id="itemToRemove" type="text"/>
    <button id="removeItem">- Remove</button>
  </p>
</div>
```
<script>new LiveCode('domain-setup', document.currentScript).link('Work With Lists ↗');</script>
