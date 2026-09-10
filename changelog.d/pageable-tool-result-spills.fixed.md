- Oversized tool results that spill to a workspace file are now written
  re-indented when they serialize as single-line JSON, so the file has lines
  for `workspace--read` to page through. A one-line spill was effectively
  unreadable past the first screenful: `offset`/`limit` count lines and there
  was only one, and an unbounded read of the whole line went over the inline
  cap and spilled again. Spills that already contain newlines, and spills that
  are not JSON, are still written byte-for-byte. The truncation notice says
  when a file was re-indented.
