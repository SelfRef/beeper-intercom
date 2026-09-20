# beeper-intercom

Your own stack, as a chat network inside [Beeper](https://www.beeper.com/):
announcements one way, an agent the other.

It registers itself as a self-hosted appservice on your Beeper account, creates
the rooms you declare, and then does two things:

- **Announcements.** Read-only rooms where bot identities post what your
  services have to say — synced to every device, with history, threads and
  reactions. Existing ntfy topics can be mirrored without touching a single
  publisher.
- **Control.** Conversational rooms where you talk to an agent, and where a
  notification can be *acted on*: react to it and a webhook fires, reply in a
  thread and the agent answers with the notification as context, answer a poll
  and the thing that asked gets your decision.

The bridge holds no intelligence of its own. The agent lives wherever you keep
it (Open WebUI, anything OpenAI-compatible, or a sidecar you write); the
automation lives wherever you keep that.

Nothing is published: it speaks Beeper's appservice websocket, so there is no
port to forward, no TLS to terminate and no public hostname.

---

## What it looks like

```
        Beeper clients (phone, desktop, web)
                      │  sync
              hungryserv (beeper.local)
                      │  appservice websocket
   ┌──────────────────┴────────────────────────────────┐
   │  beeper-intercom                                  │
   │                                                   │
   │  Matrix side               HTTP side              │
   │  • ghosts, rooms           • POST /v1/notify      │
   │  • send / react / poll     • ntfy topic mirror    │
   │  • typing, send status     • agents               │
   │  • per-room power levels   • actions → webhooks   │
   │                            • /mcp, /v1/* admin    │
   └───────────────────────────────────────────────────┘
```

## Quick start

1. Get your Beeper account token. If you use
   [bbctl](https://github.com/beeper/bridge-manager), it is in
   `~/.config/bbctl/config.json`; otherwise log in once with bbctl and take it
   from there. The bridge uses it to register itself and to set room tags and
   mute — both of which are your account's state, not the appservice's.

2. Copy `config.example.yaml`, change the names, and pick a registration name.
   It must match `^sh-[a-z0-9-]+$`: that prefix is Beeper's requirement for
   self-hosted appservices, and it becomes the prefix of every identity the
   bridge speaks as (`@sh-example_alerts:beeper.local`).

3. Run it:

```yaml
services:
  intercom:
    image: ghcr.io/selfref/beeper-intercom:latest
    restart: unless-stopped
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - ./data:/data
    environment:
      MATRIX_ACCESS_TOKEN: <your Beeper account token>
      INTERCOM_INGEST_TOKEN: <a token your scripts will use>
      INTERCOM_ADMIN_TOKEN: <a token you will use>
```

On first start it registers the appservice with Beeper, creates the rooms and
joins the ghosts. There is no second manual step; the rooms appear on every
device as their own network.

`beeper-intercom -check -config config.yaml` validates a config without
touching the network, which is worth wiring into your own deploy.

## Sending a notification

```sh
curl -X POST http://intercom:8080/v1/notify \
  -H "Authorization: Bearer $INTERCOM_INGEST_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
        "room": "alerts",
        "ghost": "home",
        "title": "Front door — person",
        "text": "Person detected at 21:14, 87 %.",
        "priority": "high",
        "url": "https://frigate.example/events/123",
        "tags": ["door", "warning"],
        "media": [{"url": "https://frigate.example/snapshot.jpg"}],
        "source": {"kind": "frigate", "id": "1758-abc", "payload": {"camera": "front"}},
        "dedupe": "frigate:1758-abc",
        "actions": {"👍": "ack", "🔕": "snooze"}
      }'
```

| Field | Meaning |
| --- | --- |
| `room` | a room key from your config (or a raw `!room` ID the bridge owns) |
| `ghost` | which identity speaks; defaults to the room's first |
| `title` / `text` / `html` | bold first line; body as Markdown; or HTML as given |
| `priority` | `min`…`urgent`; `high` and above mention the account owner in a room with `urgent_mentions`, which notifies **even if the room is muted** (Beeper never fires the `@room` rule, so the mention is by name) |
| `url` | rendered as a link after the body |
| `tags` | known ntfy shortcodes become emoji, the rest become hashtags |
| `media` | downloaded and re-uploaded, so the client never reaches into your network |
| `source` | kept with the message, so a later reaction or thread has the payload |
| `thread` | an event ID, or `kind:id` naming an earlier notification's source. If no earlier notification matches, the message becomes a root — so a publisher that always sends `thread: "frigate:<camera>"` gets one thread per camera without tracking event IDs |
| `dedupe` | idempotency key; a repeat returns the first event ID |
| `actions` | reaction key → action name (see below) |
| `profile` | `{displayname, avatar_url}` — one ghost speaking as many authors |

The response is `{"event_id": "$…", "duplicate": false}`.

### Mirroring ntfy

ntfy has no outbound webhooks, but a topic is a JSON stream, so the bridge
subscribes to it like any other client:

```yaml
sources:
  - type: ntfy
    url: http://ntfy
    topics:
      cloud-updates: { room: news,   ghost: updates }
      cloud-alerts:  { room: alerts, ghost: alerts, min_priority: 3 }
```

Nothing on the publishing side changes. Existing scripts keep posting to ntfy,
ntfy keeps working for the alarms that have to arrive when *this* is down, and
the same messages also show up in Beeper. Titles, priorities, tags, click URLs
and attachments all carry over; the ntfy message id is the dedupe key and the
cursor is persisted, so a restart does not replay or drop.

Publishers move to `/v1/notify` only when they want threads, actions or media
— never because they have to.

## Talking to an agent

A room with an `agent:` is conversational. Type in it and the agent answers:
typing indicator while it works, a reaction on your message saying which phase
it is in, Markdown rendered into the Matrix HTML subset, long answers split at
paragraph boundaries and also attached as a `.md` file. A failure marks **your**
message as failed (`com.beeper.message_send_status`) instead of adding an
apology to the conversation.

**Progress** (`progress:` in the config) decides what a long turn looks like.

| `mode` | What you see |
| --- | --- |
| `off` (default) | nothing until the answer is finished; the ghost types the whole time |
| `edits` | the first chunk is posted and rewritten with the answer so far every `interval`, each partial marked with `suffix` (`…`) |
| `stream` | Beeper's `com.beeper.stream`: the first chunk opens a message carrying a stream descriptor and later chunks go to subscribed devices as to-device updates |

`stream` is the protocol-correct rendering and costs one to-device event per
chunk instead of one room event, but **no client painted it when this was last
measured** (2026-09-20: every delta shape was delivered to subscribed devices
and dropped before the UI). `edits` is what works today. Either way the
finished answer is committed as an edit of that first message — the copy every
other device and every reload sees. `no_stream: true` on an agent turns the
backend's own streaming off, which forces `off` behaviour for that agent.

The reaction tracks the phase: ⏳ accepted (queued, and the prompt being
processed), 🧠 reasoning, 🛠️ a tool call, 🌐 a tool call that goes out to the
network, ✍️ writing the answer, ❓ waiting for an answer from you, 🗜️
compacting. 🧠 means reasoning specifically — a model with thinking off never
shows it, and the wait before the first token stays ⏳. Every emoji is configurable and `""` leaves a
phase unmarked. How many of them you see depends on the backend — Open WebUI
reports all five, a plain OpenAI endpoint only the first and last.

**Questions from the model.** Open WebUI's `ask_user` tool does not run and
return — it stops the turn with its questions staged on the message, expecting
a UI to draw a card. The bridge asks each one as a **poll** instead and
resolves the call with the answers, which resumes the same turn where it
stopped.

Once answered, the card is a control that can no longer do anything, so by
default it is removed and a single line — `question: answer` — is left in its
place: the record reads in the transcript where a dead card only takes up room.
`questions.after_answer` picks another ending — `keep` leaves the poll as well
(ended, so the answer cannot be changed to one the model never saw) and
`delete` removes it and says nothing. A question that goes unanswered past
`questions.timeout` (10 m by default) is only ended, never removed: it is the
record of a question that went nowhere, and the model carries on without it.

A Matrix poll takes one of its options and nothing else, so when the model says
a free-form answer is acceptable (`allow_other`), **a message typed into the
room while the question is open is taken as the answer** instead of starting a
new turn — and the question says `(or type answer)`, because nobody would guess
it otherwise. When the model wants one of its fixed options and you type
something else anyway, the question is **dropped**: the backend is told the
call was cancelled, whatever it says about that is discarded, and your message
becomes the next turn. A question nobody answers within `questions.timeout`
says so in the room and the model carries on without it.

**Attachments.** Send a photo and a vision model sees it; send a document and
it goes through the backend's file path (Open WebUI uploads it and runs it
through RAG or full context, as the web UI would); send a **voice message**
and it is transcribed first — the transcript is posted back as a notice, so a
misheard word explains a strange answer, and the text becomes the turn. A
caption becomes the question; without one the bridge asks the obvious
("What is in this image?").

**Tools.** `tool_ids` are the tools an agent always has. `toolsets` are groups
that can be switched on and off in the room with `/tools`, and their state
belongs to the **conversation**: it starts at the configured default, `/tools`
changes it, and the next conversation starts from the default again — so an
escalation cannot outlive what it was granted for. A toolset with `idle:` also
switches itself off after that long without a turn and says so in the room.

```yaml
toolsets:
  rw:
    description: Write access to my files, photos and repos
    tools: [server:mcp:mcphub-me-rw]
    default: false
    idle: 15m
```

`features:` names Open WebUI's own capabilities (`web_search`,
`code_interpreter`, `image_generation`, `memory`). They are not tools the
bridge can pass: Open WebUI injects them itself, and only for a request that
carries a session id, because anything else is "an API caller that does not
expect hidden tools" (`utils/middleware.py`). Naming a feature therefore also
puts the turn on Open WebUI's background-task path, which the adapter handles.

**Reasoning effort** (`reasoning:` in the config) is `/think`. Most
self-hosted catalogues publish one model id per effort rather than a parameter,
by convention a suffix on a shared base — `qwen38`, `qwen38:t`, `qwen38:m` — so
the bridge is told the convention and `/think medium` becomes `/model` with the
base held fixed. The bare id, with no suffix, is always `off`.

```yaml
reasoning:
  enabled: true
  levels:
    - { name: Think, ids: [think, t], suffix: ":t", description: thinking on }
    - { name: Low, ids: [low, l], suffix: ":l" }
    - { name: Medium, ids: [medium, m], suffix: ":m" }
    - { name: High, ids: [high, h], suffix: ":h" }
    - { name: Extra High, ids: [extra, x], suffix: ":x" }
```

`name` is what the level is called, `ids` are the words you may type for it —
`/think extra` and `/think x` are the same thing — and the first id is the one
hints and errors suggest.

Which levels exist is not assumed: the backend's model list decides, so
`/think` offers the variants that are actually served and says so when a model
has none. Unlike `/model`, the change belongs to the **conversation** — a
question that needs more thought is a question, not a new setting — and the
next one is back to the room's model. Asked before a conversation exists
(`/new`, then `/think high`), it waits for the one the next message opens.

**The bridge's own voice.** A conversational room has one ghost and two
speakers in it: the agent answering, and the bridge reporting on itself
("Nothing to undo yet.", "`rw` switched off after 15 min idle"). Beeper renders
`m.notice` from a ghost exactly like an ordinary message, so by default these
are sent by the **bridge bot** instead, whose notices render as dim centred
text with no bubble — in any room, not only a bridge-bot room. That rendering
centres the whole message, so bridge messages are laid out as **tables**, never
as bullet lists (a list's markers stay at the left margin while its text
centres), and they use `data-mx-color` to mark state. A notice caused by a
command is sent as a reply to it; autonomous ones, like a toolset going idle,
are not replies to anything.

```yaml
notices:
  sender: bot        # or ghost, which then uses the per-message profile below
  name: System
  id: system
  reply: true
```

With `sender: ghost` the notice comes from the room's ghost carrying a
`com.beeper.per_message_profile` — its own name and avatar for that one
message, without a second room member, which would turn a dm into a group.

**Deleting.** A redaction normally leaves a *"This message has been deleted"*
tombstone, which for the bridge bot renders as a left-aligned bubble with a raw
MXID — louder than the dim notice it replaced. The bridge declares
`com.beeper.room_features` on every room with `delete_hide_placeholder`, so its
own cleanup (`/undo`, a command taking back its output) leaves nothing behind.
Set `delete_placeholder: true` on a room to get the tombstones back, which is
worth doing where somebody acts on what is posted and a silent disappearance
would be worse than a marker.

That is what makes clearing up cheap. `/clear` removes the last bridge message
and `/clear all` every one in the current conversation (`/clean` is an alias for
both). There are two ways to do it without typing anything, and both exist
because **a bridge message cannot be reacted to at all** — only your own can:

- **the delete button.** The bot puts a 🗑️ on every command you type. Tapping
  it sends the same reaction from you, which is the press: the command and
  everything the bridge answered go together. The button goes with them.
- **deleting the command message.** The question and the answer are one
  exchange, so removing the question removes the answer.

```yaml
notices:
  clear_button: true     # the bot's reaction on your commands
  clear_emoji: "🗑️"      # which reaction that is
  format_commands: true  # rewrite your `/command` as inline code
```

**Deleting one of your own messages** does what deleting a message in the web
UI does: it takes the answer with it. For a question already answered that
means every event the answer occupies leaves the room and the exchange is
deleted from the backend's conversation, so the next turn is not answering
something you have taken back — `/undo`, without the command. Deleting the
newest question also rewinds the conversation to before it; deleting an older
one only takes it out of the record, because the tip has not moved. A question
still being answered is cancelled instead, which is the one way to stop a model
that is answering the wrong thing.

Each answer is sent as a **reply** to the question that caused it, as the web
UI pairs the two. In a thread the same relation is the thread's reply fallback,
so nothing changes there.

**Editing a command** you already sent: if it is the newest one and the
conversation is still the same, the edit is a correction — whatever it produced
is swept out of the room and the new text runs in its place. Anything older is
refused and the message is edited back to what it said, because its answer has
already been read and there is no way to un-read it. Matrix cannot forbid an
edit, so this is the bridge deciding what an edit means and putting the room
back to match.

`format_commands` edits the message **you** typed so a command renders as
code. A bridge cannot edit somebody else's event — an `m.replace` from another
sender is not an edit — but the message is yours and the bridge holds your
account token, so it sends the edit as you. The client marks it "Edited",
which is the price.

Commands, handled before anything reaches the backend. A message that starts
with `/` is always a command — an unknown one is an error, never a question
for the model — and `//text` sends a message that really does start with a
slash:

| Command | Effect |
| --- | --- |
| `/` | status, and a pointer to `/help` |
| `/new [toolset …]` | start a new conversation, with those toolsets already on |
| `/tools [+name\|-name\|off]` | list this conversation's toolsets, switch them, or disarm all of them |
| `/btw <question>` | answer something outside the conversation; nothing is stored and the thread is untouched |
| `/undo` | take back the last exchange — deletes it from the backend's conversation too, and removes both messages from the room (every event the answer occupies: the anchor, its progressive edits, extra parts, the attached file) |
| `/summary` | recap the conversation so far, as a notice |
| `/compact` | summarise the conversation in place, keeping its id and recent turns (Open WebUI's own compaction); backends without one fall back to closing it and seeding the next |
| `/history` | the room's recent conversations, numbered from 0 (the most recent), with links |
| `/resume [n]` · `/continue` | pick up where a finished conversation left off: the one before this by default, or the `/history` number |
| `/share` | publish the conversation as a link anyone can open (Open WebUI share + an `anyone` read grant); set `public_url` on the agent or the link points at the compose hostname |
| `/model [id\|reset]` | list the backend's models, or switch |
| `/think [level\|off]` | list the reasoning levels this model has, or switch for this conversation |
| `/agent [name\|reset]` | list the configured agents, or switch |
| `/stop`, `/cancel` | cancel the answer being generated |
| `/retry` | ask the last question again |
| `/link` | open this conversation in the backend's web UI |
| `/status` | room, agent, model and reasoning level, last turn's tokens and speed, tools, transport |
| `/clear [all]` · `/clean` | remove the last bridge message, or every one in this conversation |
| `/help` | the list |

A new conversation starts when you ask for one, when the current one has been
idle past `idle_minutes`, when it hits `max_turns`, or whenever you reply in a
thread — a thread is a side conversation and gets its own. With
`carry_summary: true` the outgoing conversation is asked to summarise itself
and the new one starts from that summary.

None of that throws anything away, which is what makes `/resume` cheap: the
backend conversation is still there, so the bridge only has to make its row the
live one again and the next message continues that chat from the same branch
tip, with the model it was using. `/resume` takes the most recent one that is
not the one you are in; `/resume 3` takes the fourth line of `/history`. A room
and thread have exactly one live conversation, so whatever was current is
closed as the old one comes back — resuming it again is another `/resume`.
Resuming also counts as activity, so an old conversation is not immediately
rotated away for being idle; one that already hit `max_turns` still is, and the
reply says so.

### Agent adapters

| Type | For | History |
| --- | --- | --- |
| `openwebui` | Open WebUI: its tools, memory, RAG and compaction | server-side, one chat per conversation, visible and continuable in the web UI; streams by polling the chat while the turn runs as a background task |
| `openai` | anything speaking `/v1/chat/completions` | the bridge keeps a transcript and replays a rolling window; streams over SSE |
| `webhook` | a sidecar in any language | whatever the sidecar wants; streams if it answers with `text/event-stream` |

**The webhook contract**, so you can write your own:

```
POST <url>/new     {"seed": {"kind": "notification"|"summary", "text": "…", "room": "…"}}
                   → {"conv_id": "…"}          (optional endpoint; 404 is fine)
POST <url>/turn    {"conv_id": "…", "text": "…", "internal": false, "turns": 3, "stream": true}
                   → {"text": "…", "link": "…"}
                   or a text/event-stream of  data: {"delta": "…"}  events,
                   optionally ending with     data: {"text": "…", "link": "…"}
                   and                        data: [DONE]
POST <url>/cancel  {"conv_id": "…"}            (optional endpoint; 404 is fine)
POST <url>/transcribe {"name": "…", "mime": "audio/ogg", "data_base64": "…"}
                   → {"text": "…"}              (optional; 404 = no voice support)

Attachments arrive on /turn as "attachments": [{"name", "mime", "size", "data_base64"}].
```

Replacing your agent later means pointing the `openai` adapter at the new thing
or writing one of those sidecars. Rooms, sessions, rendering and actions do not
change.

Note on Open WebUI: the bridge needs `ENABLE_API_KEYS=true` and an API key for
the account whose chats, tools and memory you want the agent to use. The chats
land in that account's sidebar, in the folder named in `folder:`.

For live streaming from Open WebUI it also needs a **session JWT** in
`socket_token_env`. A streaming request against a saved chat runs as a
background task whose tokens are only emitted on the user's socket.io room,
and that socket verifies tokens with the session secret, so the API key does
not open it. Mint one inside the container:

```sh
docker compose exec openwebui python -c "from open_webui.utils.auth import create_token; \
  print(create_token(data={'id': '<your user id>'}, expires_delta=None))"
```

It is a full session for that user — same blast radius as the API key — and
is revoked by a password change or an OIDC back-channel logout. Without it the
bridge falls back to polling, which on 0.11.3 only ever sees the finished
answer (the partial-output overlay is keyed by a task id the server never
records), so the bubble fills in one go.

## Making notifications actionable

**Reactions are commands.** A notification may declare `actions` mapping a
reaction key to an action name. React with one and the bridge POSTs to the
configured webhook:

```json
{ "schema": 1, "kind": "reaction", "action": "ack", "key": "👍",
  "room": "alerts", "event_id": "$…", "source": { "kind": "frigate", "id": "…", "payload": {…} },
  "actor": "@you:beeper.com", "at": "2026-09-20T21:15:03Z" }
```

With `report_undeclared: true`, a reaction nothing declared is reported too,
with `"action": null` — so new conventions can be invented on the receiving
side without redeploying anything. Broadcast rooms keep `m.reaction` at power
level 0 precisely so this works in a room you cannot type in.

**Threads are follow-up.** Replying in a thread under a notification opens a
conversation seeded with that notification, so "why did this restart?" is a
question the agent can answer.

**Polls are confirmation.** `POST /v1/poll` (or the `ask_confirmation` MCP
tool) posts a real poll and, with `wait_seconds`, blocks until you answer. This
is the pattern for anything irreversible.

Deliveries go through an outbox: the row exists before the HTTP call, so an
action survives the receiver being restarted. `GET /v1/deliveries` and
`POST /v1/deliveries/{id}/retry` are there when one does not.

## The status room

Name one with `network.status_room` and the bridge creates a bridge-bot room
where it reports on itself: startup, outages longer than a minute, config
reloads, deliveries that gave up. Shorter drops are not reported: the
appservice websocket is pinged every 180 s to stop the homeserver closing it
as idle, and the reconnects that still happen take about a second, during
which inbound events are buffered by the server rather than lost. These render as the dim centred notices Beeper uses
for bridge login prompts — that style needs both the room flag
(`com.beeper.is_bridge_bot_room`) and the bridge bot as sender, so nothing
else in the bridge can use it and the room never turns into a chat.

## Surfaces

| Path | Who | Token |
| --- | --- | --- |
| appservice websocket | Beeper | the registration's own |
| `POST /v1/notify`, `/v1/reply`, `/v1/poll` | your scripts and automations | ingest |
| `GET /v1/rooms`, `/ghosts`, `/sessions`, `/events`, `/deliveries`, `/status` | you | admin |
| `POST /v1/deliveries/{id}/retry`, `/v1/reload` | you | admin |
| `/mcp` | agents | admin |
| `GET /health` | your healthcheck | none |

Two tokens because they leak differently: the ingest token ends up in every
script and workflow that posts a notification, so it can only post. Reading
stored notification bodies, rewriting rooms and reloading config need the admin
one.

**MCP tools** — reads: `list_rooms`, `list_ghosts`, `list_sessions`,
`get_session`, `list_recent_notifications`, `test_render` (dry-run the
rendering without sending). Writes: `send_notification`, `send_reply`,
`ask_confirmation`, `new_conversation`, `set_room_muted`. The naming is
deliberate so a hub can split them into read-only and read-write sets.

## Configuration notes

Rooms are reconciled on every start: created if missing, patched if the config
drifted, never deleted. The room **key** is its identity, so renaming a room in
config renames the room rather than creating a second one.

`kind` decides the power levels:

- `broadcast` — the composer is locked (`events_default` is set *above* your
  own power level; equal is not enough) and `m.reaction` stays at 0.
- `urgent_mentions: true` — `priority: high/urgent` mentions you by name. The
  intended use is a muted room: routine alerts stay quiet, urgent ones still
  buzz. (Hungryserv evaluates `.m.rule.is_user_mention` but not
  `.m.rule.is_room_mention`, so `@room` alone would do nothing.)
- `chat` — an ordinary conversational room.
- `dm` — the same, rendered as a direct message. Needs exactly one ghost.

Config is reloaded on `SIGHUP` or `POST /v1/reload`: rooms reconcile, ghosts
refresh, agents are rebuilt, sessions survive. Two things do not reload —
**sources** (an ntfy subscription is a live stream with a cursor, so a changed
topic list needs a restart) and the **registration name**, which decides which
appservice this process *is*.

`${VAR}` is expanded anywhere in the config file. Secrets should not be in it
at all: the fields that need one name an environment variable instead.

## Encryption

Bridge rooms are **not encrypted**, deliberately. Your notification and agent
text passes through Beeper's servers in the clear. That is a real trade-off,
made because unencrypted rooms are readable by tooling you already have, need
no room-key management, and — for announcements about your own infrastructure
and conversations with your own agent — carry about as much as the metadata
would anyway. If a room needs to carry something that should not go that way,
the answer is a different room, not encrypting everything.

## Building

```sh
go test ./...
go build ./cmd/beeper-intercom
docker build -t beeper-intercom .
```

Pure Go, no cgo (SQLite included), so the image is a static binary on
distroless. CI runs the tests, then smoke-tests the built image: config
loading, the auth split between the two tokens, and the MCP endpoint.

## Licence

Apache-2.0.
