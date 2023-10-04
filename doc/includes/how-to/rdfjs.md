```js
document.addEventListener('domainChanged', async () => {
  updatesDiv.innerHTML = '';

  for await (let [update] of window.model.state.follow()) {
    const { details, summary, deleteTextarea, insertTextarea } =
      templated(updateTemplate);
    summary.innerText = `Update ${update['@ticks']}`;

    const deleted = update['@delete'].quads;
    const inserted = update['@insert'].quads;

    deleteTextarea.value = await toTurtle(deleted);
    insertTextarea.value = await toTurtle(inserted);
    updatesDiv.insertAdjacentElement('afterbegin', details);
  }
});

updateButton.addEventListener('click', async () => {
  const parser = new N3.Parser();
  window.model.state.updateQuads({
    delete: await parser.parse(deleteTextarea.value),
    insert: await parser.parse(insertTextarea.value)
  });
});

function toTurtle(quads) {
  return new Promise((resolve, reject) => {
    const writer = new N3.Writer();
    for (let quad of quads)
      writer.addQuad(quad);
    writer.end((err, result) => err ? reject(err) : resolve(result));
  });
}
```
```html
<div id="appDiv" hidden>
  <label for="deleteTextarea">DELETE triples</label>
  <textarea id="deleteTextarea" rows="5"></textarea>
  <label for="insertTextarea">INSERT triples</label>
  <textarea id="insertTextarea" rows="5">
PREFIX c: <http://example.org/cartoons#>
   c:Tom a c:Cat.
   c:Jerry a c:Mouse;
           c:smarterThan c:Tom.
  </textarea>
  <button id="updateButton">
    Do Update
  </button>
  <hr/>
  <div id="updatesDiv"></div>
  <template id="updateTemplate">
    <details class="updateDetails">
      <summary>Update</summary>
      <hr/>
      <label>DELETED triples</label>
      <textarea class="deleteTextarea" rows="5"></textarea>
      <label>INSERTED triples</label>
      <textarea class="insertTextarea" rows="5"></textarea>
    </details>
  </template>
</div>
<!-- https://www.npmjs.com/package/n3 -->
<script src="https://unpkg.com/n3/browser/n3.min.js"></script>
```
```css
textarea {
    width: 100%;
}
.updateDetails {
  border: 1px solid #aaa;
  border-radius: 4px;
  padding: 0.5em;
}
```
<script>new LiveCode('domain-setup', document.currentScript).link();</script>
