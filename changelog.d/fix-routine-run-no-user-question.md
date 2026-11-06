### Fixed

- A routine run, a webhook run, another agent's message or a channel task that no person requested no longer waits forever on a question that nobody can answer.
  In such a turn, Claude's `AskUserQuestion`, `ask_user`, `ask_ui` and a provider's request for user input are refused at once, and the agent gets the reason. A person's message in the agent's own chat, a side thread the person opened, and a person's request in a channel still show the question.
