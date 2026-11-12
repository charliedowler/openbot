### Fixed

- Interrupt a turn after 30 minutes without provider output. Terminate its provider session after two more minutes if it does not end. Preserve live tools that keep producing output.
- Settle a processless orphan on the first reaper pass, including an orphan that retains its in-memory turn entry.
- Report whether an operator interrupt actually ends the native turn. A provider acknowledgement alone does not report a completed stop.
- Retire the killed provider session so the next delivery opens a fresh process. Store terminal hung-provider stop details after escalation.
