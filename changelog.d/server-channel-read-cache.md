### Fixed

- A server with long channels no longer stalls its main thread when it pumps the channel queues and lists the channels. Each pump read and parsed every task and assignment of a channel many times for each wake, and the channel list counted the unread messages of every channel again on each call. The channel store now keeps the decoded rows of each channel until one of its writes changes them, and the list reads only the messages that changed. The answers are the same as before.
