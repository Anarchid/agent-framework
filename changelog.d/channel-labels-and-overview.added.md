- `HistoryModule` gains a fourth tool, `overview`, for browsing conversation
  history when the caller doesn't already know a specific channel/date/search
  term: returns existing compression summaries as a table of contents (zero
  new LLM calls), falling back to raw message/channel counts for spans not
  yet summarized. `stats`/`extract`/`search`/`overview` now all accept a
  `channelId` as either a channel label (e.g. `#general`, `@name`, `<@id>`)
  or the raw internal channel id — resolved via a new durable
  `ChannelRegistry` label-history log, so a channel is still addressable by
  name even after the bot disconnects from it (the common case for browsing
  history on a quiet/old channel, where the live channel registry has
  nothing).
