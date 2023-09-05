```js
import { shortId, array } from 'https://js.m-ld.org/ext/index.mjs';

document.addEventListener('domainChanged', async () => {
  knowledgeBaseDiv.innerHTML = '';
  answerButton.disabled = false;
  
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
  }).each(knowledge => { // 4️⃣
    const content = templated(knowledgeTemplate);
    content.questionText.innerText = knowledge.question;
    for (let answer of array(knowledge.answer))
      content.answerList.insertAdjacentHTML('beforeend', `<li>${answer}</li>`);
    content.addAnswer.addEventListener('click', () => {
      const answer = content.answerInput.value;
      if (answer)
        window.model.state.write({ '@id': knowledge['@id'], answer }); // 5️⃣
    });
    knowledgeBaseDiv.insertAdjacentElement('beforeend', content.knowledgeDiv);
  });
}

answerButton.addEventListener('click', async () => {
  const { state } = window.model;
  const question = questionInput.value;
  if (question) {
    const knowledgeId = shortId();
    await state.write({ '@id': knowledgeId, question }); // 5️⃣
    const botAnswer = await askBot(question);
    if (botAnswer)
      state.write({ '@id': knowledgeId, answer: botAnswer }); // 5️⃣
  }
});

async function askBot(question) {
  const response = await fetch(
    `https://question-gx8raknaiaaa.runkit.sh/?q=${encodeURIComponent(question)}`);
  return response.text();
}
```
```html
<template class="help">
  This example demonstrates:
  <ul>    
    <li>1️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldstatemachine.html#read">read procedure</a> with a follow handler.</li>
    <li>2️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldreadstate.html#read">read request</a> to issue a structured query.</li>
    <li>3️⃣ Using a <a href="https://js.m-ld.org/interfaces/describe.html"><code>@describe</code> query</a> to obtain all the properties of all subjects in the domain.</li>
    <li>4️⃣ Using <code>each()</code> on the <a href="https://js.m-ld.org/interfaces/readresult.html">read result</a> to process each subject in turn.</li>
    <li>5️⃣ Using a <a href="https://js.m-ld.org/interfaces/meldstatemachine.html#write">write request</a> to add new information to the domain.</li>
  </ul>
</template>
<div>
  <p>
    <label for="questionInput">Ask me anything!</label>
    <input id="questionInput" type="text" value="Who is Elvis?"/>
    <button id="answerButton" disabled>Answer</button>
  </p>
  <hr/>
  <template id="knowledgeTemplate">
    <div class="knowledgeDiv">
      <p class="questionText"></p>
      <ul class="answerList"></ul>
      <input class="answerInput" type="text" placeholder="Your answer"/>
      <button class="addAnswer">Add</button>
    </div>
  </template>
  <div id="knowledgeBaseDiv"></div>
</div>
```
<script>new LiveCode('domain-setup', document.currentScript).link('Refresh UI on update');</script>
