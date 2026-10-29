# Shared folder browsing

The optional `shared-directory-v1` capability adds `POST /v1/shared-directory`.
The request has a `path` string. The response has `name`, `path`, `root`,
`parentPath`, `entries` and `truncated`. The wire fixtures record both shapes.

Any authenticated member can list Shared files, as they can already download
Shared files. The host resolves paths under its real Shared root. Missing and
outside paths return the same public error. Links outside that root are absent.
Each entry path is a literal `Shared/` reference that the existing file download
route accepts. It keeps spaces, percent signs and Unicode without URI decoding.
The root has no parent. The existing 500-entry bound and directories-first order
apply. Workspace directory routes and their frozen encoding stay unchanged.

The desktop and web clients ask for a directory before requesting file bytes.
They reuse the file preview panel for entries and individual file previews.
Back returns to the containing folder or its parent. A generation guard drops
stale responses after a newer request or conversation switch. The new capability
is independent of application version. An older host falls back to its existing
file preview or ZIP download behavior.

The folder toolbar can download a ZIP on a remote host. Normal browsing does not
create or fetch an archive. Local folders use the same list and preview controls;
the remote ZIP action is absent there. Nothing pulls a path from another machine.
Files must exist under the host Shared root. No cloud service stores the listing
or file contents.

Files opened from a directory keep that bounded listing in renderer state. Left and Right arrows and the preview toolbar move through its files, wrap at either end, and skip directories. A pending navigation cursor and the existing preview generation guard prevent rapid key presses from accepting stale results. Keys are scoped to the focused preview and leave editable fields, media controls and panel resizing alone. Images fit the available pane in both dimensions.
