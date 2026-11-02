### Fixed

- A turn that a provider outage stopped, such as Codex "Selected model is at capacity" or a Claude 529 overload, now continues on the agent's automatic backup model, also after the turn did work and also in a task or a channel task. The backup gets the captured work steps and checks the saved work before it repeats an action. Before, the turn failed with the error, and a channel task waited for Resume.
- With no ready backup, the turn runs once more on the same model after a short wait, then fails as before.
