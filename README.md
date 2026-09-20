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
| `priority` | `min`…`urgent`; `high` and above become an `@room` mention in a room with `urgent_mentions` |
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
typing indicator while it thinks, a ⚙️ reaction on your message while it runs,
**the answer streamed live into the bubble** as it is generated, Markdown
rendered into the Matrix HTML subset, long answers split at paragraph
boundaries and also attached as a `.md` file. A failure marks **your** message
as failed (`com.beeper.message_send_status`) instead of adding an apology to
the conversation.

Streaming uses Beeper's `com.beeper.stream`: the first token opens a message
carrying a stream descriptor, later tokens go to your devices as to-device
updates, and the finished answer is committed as an edit — the copy every
other device and every reload sees. Clients without stream support show `…`
until that edit lands. Set `no_stream: true` on an agent to get one message
per answer instead.

**Attachments.** Send a photo and a vision model sees it; send a document and
it goes through the backend's file path (Open WebUI uploads it and runs it
through RAG or full context, as the web UI would); send a **voice message**
and it is transcribed first — the transcript is posted back as a notice, so a
misheard word explains a strange answer, and the text becomes the turn. A
caption becomes the question; without one the bridge asks the obvious
("What is in this image?").

Commands, handled before anything reaches the backend:

| Command | Effect |
| --- | --- |
| `/new [title]` | start a new conversation |
| `/agent <name>` | switch the backend for this room |
| `/model <id>` | switch the model for this room |
| `/status` | room, agent, model, session, transport |
| `/help` | the list |

A new conversation starts when you ask for one, when the current one has been
idle past `idle_minutes`, when it hits `max_turns`, or whenever you reply in a
thread — a thread is a side conversation and gets its own. With
`carry_summary: true` the outgoing conversation is asked to summarise itself
and the new one starts from that summary.

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
where it reports on itself: startup, reconnects after a drop, config reloads,
deliveries that gave up. These render as the dim centred notices Beeper uses
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
