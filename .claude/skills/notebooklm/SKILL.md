---
name: notebooklm
description: |
  Query Google NotebookLM notebooks via gstack browse daemon. List notebooks,
  select a notebook, ask questions, get citation-backed answers, manage sources.
  No MCP needed — uses the same browser automation that already works on this machine.
  Use when asked to "check notebooklm", "ask notebooklm", "research in notebooklm",
  or "query my notebooks".
allowed-tools:
  - Bash
  - Read
  - AskUserQuestion
---

# NotebookLM via gstack browse

Interact with Google NotebookLM (notebooklm.google.com) through the gstack browse
daemon already running on this machine. No MCP server needed.

## Prerequisites

- gstack browse daemon running (port 21144)
- Google account authenticated in the browse session (cookies imported or manual login done)

## Setup (run once per session)

```bash
# Resolve browse binary
_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
B=""
[ -n "$_ROOT" ] && [ -x "$_ROOT/.claude/skills/gstack/browse/dist/browse" ] && B="$_ROOT/.claude/skills/gstack/browse/dist/browse"
[ -z "$B" ] && B=~/.claude/skills/gstack/browse/dist/browse
if [ -x "$B" ]; then
  echo "READY: $B"
else
  echo "NEEDS_SETUP: run /browse first to initialize gstack browse"
fi
```

## Authentication

NotebookLM requires a Google login. Two approaches:

### Option A: Import cookies from your regular Chrome session
```bash
# Use the setup-browser-cookies skill to import Google cookies
# This avoids manual login entirely
```
Invoke `/setup-browser-cookies` and select google.com / notebooklm.google.com domains.

### Option B: Manual login via handoff
```bash
$B goto https://notebooklm.google.com
$B handoff "Please log in to your Google account for NotebookLM"
# Wait for user to complete login
$B resume
$B snapshot -i
```

### Verify auth
```bash
$B goto https://notebooklm.google.com
$B snapshot -c
# If you see notebook list: authenticated
# If you see login page: need to auth first
```

---

## Core Operations

### 1. List notebooks

```bash
$B goto https://notebooklm.google.com
# Wait for page to load
$B snapshot -i -c
```

The notebook list shows as cards/tiles. Each notebook has a title and last-modified date.
Read the snapshot output to enumerate notebooks for the user.

### 2. Select / open a notebook

```bash
# From the notebook list, find the notebook by title text
$B snapshot -i
# Click the notebook card — identify by @e ref from snapshot
$B click @eN
# Wait for notebook to load
$B snapshot -i -c
```

### 3. Ask a question (core research feature)

```bash
# With a notebook open, find the chat/query input
$B snapshot -i
# The chat input is typically at the bottom of the notebook view
$B fill @eN "Your question here"
# Submit the question (Enter key or click send button)
$B key Enter
# Wait for response to generate (NotebookLM can take 5-15 seconds)
$B wait 8000
$B snapshot -c
# Read the response text — it includes inline citations [1], [2], etc.
$B text
```

**Important:** NotebookLM responses include citation markers like [1], [2] that
reference specific sources in the notebook. Always include these in your output
to the user.

If the response is still loading (spinner visible), wait and re-check:
```bash
$B wait 5000
$B text
```

### 4. Ask follow-up questions

The chat maintains context within a session. Just send another message:
```bash
$B snapshot -i
$B fill @eN "Follow-up question"
$B key Enter
$B wait 8000
$B text
```

### 5. List sources in a notebook

```bash
# With a notebook open, the sources panel is on the left side
$B snapshot -i -c
# Sources are listed in the left panel — read their titles from the snapshot
```

### 6. Add a source (URL)

```bash
# With a notebook open, click the "Add source" / "+" button
$B snapshot -i
$B click @eN  # the add source button
$B wait 2000
$B snapshot -i
# Select "Website" or "Link" option
$B click @eN
$B snapshot -i
# Paste the URL
$B fill @eN "https://example.com/article"
$B click @eN  # Insert / Add button
$B wait 10000  # source processing takes time
$B snapshot -c
```

### 7. Add a source (paste text)

```bash
# Click add source -> "Copied text" option
$B snapshot -i
$B click @eN  # add source
$B wait 2000
$B snapshot -i
$B click @eN  # "Copied text" option
$B snapshot -i
# Fill title
$B fill @eN "Source Title"
# Fill body (use js for long text to avoid shell escaping issues)
$B js "document.querySelector('textarea, [contenteditable]').innerText = 'your text here'"
$B click @eN  # Insert button
$B wait 5000
$B snapshot -c
```

### 8. Create a new notebook

```bash
$B goto https://notebooklm.google.com
$B snapshot -i
# Click "New notebook" / "Create" button
$B click @eN
$B wait 3000
$B snapshot -i -c
```

### 9. Delete / remove a notebook

```bash
# From the notebook list, find the notebook's overflow menu (three dots)
$B goto https://notebooklm.google.com
$B snapshot -i -C
# Click the three-dot menu on the target notebook
$B click @eN  # or @cN for cursor-interactive
$B snapshot -i
# Click "Delete" option
$B click @eN
$B snapshot -i
# Confirm deletion
$B click @eN
```

### 10. Generate Audio Overview (podcast)

```bash
# With a notebook open, find the "Audio Overview" section
$B snapshot -i -C
# Click "Generate" or the Audio Overview button
$B click @eN
$B wait 5000
$B snapshot -c
# Audio generation takes several minutes — check back
```

### 11. Navigate back to notebook list

```bash
$B goto https://notebooklm.google.com
$B snapshot -i -c
```

---

## Workflow: Research a topic

Typical end-to-end research flow:

1. **Auth check** — goto notebooklm.google.com, verify logged in
2. **Find or create notebook** — list notebooks, select or create new
3. **Add sources** — URLs, documents, pasted text
4. **Ask questions** — iterative Q&A with citations
5. **Extract answers** — read responses, preserve citations
6. **Report** — summarize findings with source references

## Tips

- **Wait times matter.** NotebookLM is slow — responses take 5-15 seconds.
  Always `$B wait` after submitting a question before reading.
- **Citations are the value.** NotebookLM's answers are grounded in your sources.
  Always preserve [1], [2] citation markers in output.
- **Session persists.** The browse daemon keeps cookies, so once logged in you
  stay logged in across skill invocations until cookies expire.
- **Use `$B text` for long responses.** `snapshot` gives structure; `text` gives
  the full readable text content which is better for long-form answers.
- **If elements aren't found**, use `$B snapshot -i -C` to catch cursor-interactive
  elements that don't have proper ARIA roles.
- **For stuck pages**, try `$B wait 3000` then `$B snapshot -D` to see what changed.

## Error handling

| Problem | Fix |
|---------|-----|
| Login page shown | Run auth flow (Option A or B above) |
| "Unable to process" | Source may be too large or format unsupported |
| Spinner won't stop | `$B wait 15000` then `$B snapshot -c` — if still loading, `$B reload` |
| Empty response | `$B text` may capture what `snapshot` misses |
| Wrong notebook open | `$B goto https://notebooklm.google.com` to go back to list |
