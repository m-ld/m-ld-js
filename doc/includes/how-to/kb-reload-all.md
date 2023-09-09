```js
import { array } from 'https://js.m-ld.org/ext/index.mjs';

document.addEventListener('domainChanged', async () => {
  window.model.state.read( // 1️⃣
    // Populate the UI as soon as the domain has been changed.
    state => reloadAll(state),
    // In this example, we just refresh the whole UI state on every update.
    (update, state) => reloadAll(state)
  )
});

async function reloadAll(state) {
  knowledgeBaseDiv.innerHTML = '';
  return state.read({ // 2️⃣
    '@describe': '?id', '@where': { '@id': '?id' } // 3️⃣
  }).each(({ '@id': kid, question, answer }) => { // 4️⃣
    const {
      knowledgeDiv, questionText, answerList, answerInput, addAnswerButton
    } = templated(knowledgeTemplate);
    questionText.innerText = question;
    // The `answer` field can be one string or an array
    for (let ans of array(answer))
      answerList.insertAdjacentHTML('beforeend', `<li>${ans}</li>`);
    addAnswerButton.addEventListener('click', () => {
      answerInput.value && window.model.state.write({ // 5️⃣
        '@id': kid, answer: answerInput.value
      });
    });
    knowledgeBaseDiv.insertAdjacentElement('beforeend', knowledgeDiv);
  });
}
```
```html
<template class="help">
  <p>This example shows how an app can establish some user interface state from a <b>m-ld</b> domain, and then follow updates to the domain; in this case, just refreshing the UI every time. Changes arising in the app are pushed to the domain, where they are 'echoed' back as updates so that the UI presents the local user's changes, as well as any remote users, in the same way.</p>
  <p>The code demonstrates:</p>
  <ul>
    <li>1️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldstatemachine.html#read">read procedure</a> with a follow handler.</li>
    <li>2️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldreadstate.html#read">read request</a> to issue a structured query.</li>
    <li>3️⃣ Using a <a href="https://js.m-ld.org/interfaces/describe.html"><code>@describe</code> query</a> to obtain all the properties of all subjects in the domain.</li>
    <li>4️⃣ Using <code>each()</code> on the <a href="https://js.m-ld.org/interfaces/readresult.html">read result</a> to process each subject in turn.</li>
    <li>5️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldstatemachine.html#write">write request</a> to add new information to the domain.</li>
  </ul>
</template>
```
<script>new LiveCode('kb-setup', document.currentScript).link('Basic: refreshing the UI on update');</script>
