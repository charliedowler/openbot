### Added

- Apply a 300,000 token session cap to all agents. Set a server default or an agent override. Compact or rotate at the lower of the cap and 80 percent of the model window.
- Preserve native work IDs, signed dispatch and claim links, recent messages and a short saved summary when a session rotates.
- Add an operator control-socket route to compact or rotate an idle agent session. Refuse active work.
- Fail a turn at 40 distinct model calls or 20 million input tokens. Set server defaults and an agent token override. Do not retry the failed delivery in place. Count cache reads and writes. Bound channel summaries to 40 recent messages. Keep all source history and the saved summary. Reuse routing's five-message window. Keep the saved work for review and use incremental Claude request usage to stop repeated reads.
