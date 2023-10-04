```js
import { shortId } from 'https://js.m-ld.org/ext/index.mjs';

answerButton.addEventListener('click', async () => {
  const { state } = window.model;
  const question = questionInput.value;
  if (question) {
    const knowledgeId = shortId();
    await state.write({ '@id': knowledgeId, question });
    const botAnswer = await askBot(question);
    if (botAnswer)
      state.write({ '@id': knowledgeId, answer: botAnswer });
  }
});

async function askBot(question) {
  const response = await fetch(
    `https://runkit.io/gsvarovsky/question/0.1.2?q=${encodeURIComponent(question)}`);
  return response.text();
}
```
```html
<div id="appDiv" hidden>
  <h2>Collaborative Knowledge Base</h2>
  <p>
    <label for="questionInput">Ask anything!</label>
    <input id="questionInput" type="text" value="Who is Elvis?"/>
    <button id="answerButton">Answer</button>
  </p>
  <hr/>
  <template id="knowledgeTemplate">
    <div class="knowledgeDiv">
      <p class="questionText"></p>
      <ul class="answerList"></ul>
      <input class="answerInput" type="text" placeholder="Your answer"/>
      <button class="addAnswerButton">Add</button>
    </div>
  </template>
  <div id="knowledgeBaseDiv"></div>
</div>
```
<script title="kb-setup">new LiveCode('domain-setup', document.currentScript).declare();</script>
