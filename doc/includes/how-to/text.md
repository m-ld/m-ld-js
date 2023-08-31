```js
import { TSeqText } from 'https://js.m-ld.org/ext/tseq.mjs';
import { updateSubject } from 'https://js.m-ld.org/ext/index.mjs';
import { ElementSpliceText } from 'https://js.m-ld.org/ext/html.mjs';

document.addEventListener('domainChanged', () => {
  if (window.model.genesis) {
    window.model.state.write(TSeqText.declare(0, 'text'))
      // Write some initial document content
      .then(() => window.model.state.write({
        '@id': 'document',
        'text': `Document created ${new Date().toLocaleString()}`
      }));
  }
  let documentTextProxy = null;
  const doc = {
    '@id': 'document',
    set text(content) {
      documentTextProxy = new ElementSpliceText(
        documentTextDiv,
        content,
        async splices => {
          window.model.state.write(async state => {
            for await (let splice of splices) {
              state = await state.write({
                '@update': { '@id': 'document', text: { '@splice': splice } }
              });
            }
          });
        }
      );
    },
    get text() {
      return documentTextProxy;
    }
  };
  window.model.state.read(async state => {
    updateSubject(doc, await state.get('document'));
  }, update => {
    updateSubject(doc, update);
  });
});
```
```html
<div>
  <h2>Document</h2>
  <div contenteditable="plaintext-only" id="documentTextDiv"></div>
</div>
```
```css
div[contenteditable] {
    border: 1px inset #ccc;
    padding: 5px;
    background-color: white;
    font-family: monospace;
    height: 20em;
}
```
<script>new LiveCode('domain-setup', document.currentScript).link('live code ↗');</script>
