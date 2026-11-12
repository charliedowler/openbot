### Added

- Apply a 200,000 token session cap to all agents. Set a server default or an agent override. Compact or rotate at the lower of the cap and 80 percent of the model window.
- Preserve native work IDs, signed dispatch and claim links, recent messages and a short saved summary when a session rotates.
- Add an operator control-socket route to compact or rotate an idle agent session. Refuse active work.
- Stop a turn once it reaches its input token budget. Count cache reads and writes. Keep the saved work for review and use incremental Claude request usage to stop repeated reads.
