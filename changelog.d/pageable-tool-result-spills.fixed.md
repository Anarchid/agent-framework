- Oversized tool-result spill files can now be read in bounded character
  ranges with `workspace--read` (`offsetChars`/`limitChars`). Spill notices
  include a read command sized for the inline cap and explain how to continue.
  This makes single-line JSON, large string values, and non-JSON long lines
  recoverable without rewriting the stored result or expanding it past the
  workspace file-size limit. Existing line-based reads keep their behavior.
