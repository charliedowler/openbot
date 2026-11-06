### Fixed

- An agent in a channel task can now tell an agent outside the channel about a result, such as a coordinator.
  `send_message` from a channel task works for a one-way message (`expectsReply` false, no `replyToMessageId`) when no recipient is a member of the channel. Channel work still goes through `channel_assign` and `channel_transfer`.
