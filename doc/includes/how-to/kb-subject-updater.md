```js
import { array, SubjectUpdater } from 'https://js.m-ld.org/ext/index.mjs';

const knowledgeViews = new Map();

document.addEventListener('domainChanged', async () => {
  window.model.state.read(
    async state => {
      // Populate the UI from scratch when the domain has changed.
      knowledgeBaseDiv.innerHTML = '';
      const subjectUpdater = new SubjectUpdater(await state.read({ // 1️⃣
        '@describe': '?id', '@where': { '@id': '?id' }
      }));
      for (let kid of subjectUpdater.affectedIds)
        subjectUpdater.update(new KnowledgeView(kid));
    },
    update => {
      const subjectUpdater = new SubjectUpdater(update); // 2️⃣
      for (let kid of subjectUpdater.affectedIds)
        subjectUpdater.update(knowledgeViews.get(kid) ?? new KnowledgeView(kid));
    }
  );
});

class KnowledgeView {
  constructor(kid) {
    this['@id'] = kid;
    const {
      knowledgeDiv, questionText, answerList, answerInput, addAnswerButton
    } = templated(knowledgeTemplate);
    this.questionText = questionText;
    this.answerList = answerList;
    addAnswerButton.addEventListener('click', () => {
      answerInput.value && window.model.state.write({
        '@id': kid, answer: answerInput.value
      });
    });
    knowledgeViews.set(kid, this);
    knowledgeBaseDiv.insertAdjacentElement('beforeend', knowledgeDiv);
  }

  get question() {
    return this.questionText.innerText || undefined;
  }

  set question(question) {
    this.questionText.innerText = question;
  }

  get answer() {
    return [...this.answerList.children].map(li => li.innerText);
  }

  set answer(answer) {
    this.answerList.innerHTML = '';
    for (let ans of array(answer))
      this.answerList.insertAdjacentHTML('beforeend', `<li>${ans}</li>`);
  }
}
```
```html
<template class="help">
  <p>As your app's features and data model scale, it becomes more complex to deconstruct updates and apply them to selected UI elements, as in the previous example. In this code, we declaratively model the the knowledge UI as a class having question and answer properties. Then, we use a <a href="https://js.m-ld.org/classes/subjectupdater.html"><code>SubjectUpdater</code></a> to apply new information. This utility correctly handles all the possible changes that might apply in an update, taking into account <a href="https://spec.m-ld.org/#data-semantics"><b>m-ld</b> data semantics</a> – so, for example, it will still work when we start to allow deletion of answers.</p>
  <p>The code demonstrates:</p>
  <ul>
    <li>1️⃣ Constructing a <code>SubjectUpdater</code> with newly-loaded subjects, and then using it to apply the loaded property values to new knowledge views.</li>
    <li>2️⃣ Constructing a <code>SubjectUpdater</code> from an update, and then using it to apply the changed property values to knowledge views (which may be new, or already exist).</li>
  </ul>
</template>
```
<script>new LiveCode('kb-setup', document.currentScript).link('Advanced: using <code>updateSubject</code>');</script>
