```js
import { array } from 'https://js.m-ld.org/ext/index.mjs';

document.addEventListener('domainChanged', async () => {
  window.model.state.read(
    state => {
      // Populate the UI from scratch when the domain has changed.
      knowledgeBaseDiv.innerHTML = '';
      return state.read({
        '@describe': '?id', '@where': { '@id': '?id' }
      }).each(addQuestion);
    },
    update => {
      // Our app's updates are always either one question or one answer
      const { '@insert': [knowledge] } = update; // 1️⃣
      const { '@id': kid, answer } = knowledge;
      if (answer) // A new answer
        addAnswer(document.querySelector(`#${kid} .answerList`), answer);
      else // A new knowledge item
        addQuestion(knowledge);
    }
  );
});

function addAnswer(answerList, answer) {
  answerList.insertAdjacentHTML('beforeend', `<li>${answer}</li>`);
}

function addQuestion({ '@id': kid, question, answer }) {
  const {
    knowledgeDiv, questionText, answerList, answerInput, addAnswerButton
  } = templated(knowledgeTemplate);
  questionText.innerText = question;
  for (let ans of array(answer))
    addAnswer(answerList, ans);
  addAnswerButton.addEventListener('click', () => {
    answerInput.value && window.model.state.write({
      '@id': kid, answer: answerInput.value
    });
  });
  // Set the element id so we can find it later
  knowledgeDiv.id = kid;
  knowledgeBaseDiv.insertAdjacentElement('beforeend', knowledgeDiv);
}
```
```html
<template class="help">
  <p>Instead of just reloading the whole UI state when an update comes in (as in the previous example), it might be better to inspect the update to see what has changed, and act upon only the changed information. This actually fixes a bug you might have noticed with the reloading example: an answer box can get replaced while you're typing into it, if someone else makes a change.</p>
  <p>The code demonstrates:</p>
  <ul>
    <li>1️⃣ Inspecting the contents of a <a href="https://js.m-ld.org/interfaces/meldupdate.html"><b>m-ld</b> update</a> to target specific app UI components.</li>
  </ul>
</template>
```
<script>new LiveCode('kb-setup', document.currentScript).link('Intermediate: handling known updates');</script>
