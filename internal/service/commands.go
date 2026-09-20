package service

import (
	"context"
	"errors"
	"fmt"
	"html"
	"sort"
	"strings"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// Commands are handled by the bridge and never reach a backend.
//
// The rule is the slash: a message that starts with one is addressed to the
// bridge, and an unknown command is an error rather than a question for the
// model — guessing which slash was meant for whom is exactly the ambiguity
// this avoids. `//` escapes it, for the rare message that really does start
// with a slash.
var helpText = "**Commands** — everything starting with `/` is handled here and never reaches the agent; `//text` sends a message that really does start with a slash.\n\n" +
	table([]string{"Command", "What it does"}, [][]string{
		{"`/`", "status, and a pointer here"},
		{"`/new [toolset …]`", "start a new conversation, optionally with toolsets already on"},
		{"`/tools [+name -name off]`", "list the tool groups, or switch them"},
		{"`/think [level off]`", "reasoning effort, for this conversation"},
		{"`/model [id reset]`", "list the backend's models, or switch"},
		{"`/agent [name reset]`", "list the configured agents, or switch"},
		{"`/btw <question>`", "ask outside this conversation; nothing is stored"},
		{"`/undo`", "take back the last exchange, here and in the backend"},
		{"`/retry`", "ask the last question again"},
		{"`/stop` · `/cancel`", "cancel the answer being generated"},
		{"`/summary`", "recap the conversation so far"},
		{"`/compact`", "summarise it in place and keep going"},
		{"`/history`", "recent conversations in this room, numbered"},
		{"`/resume [n]` · `/continue`", "pick up the conversation before this one, or one by its `/history` number"},
		{"`/share`", "publish it as a link anyone can open"},
		{"`/link`", "open this conversation in the web UI"},
		{"`/status`", "room, agent, model, context, tools, transport"},
		{"`/bridge [reload restart]`", "re-read the config file, or restart the bridge itself"},
		{"`/clear [all]` · `/clean`", "remove the last bridge message, or every one in this conversation"},
		{"`/help`", "this table"},
	}) +
	"\nReplying in a thread starts a side conversation; reacting to a notification runs its action."

func (s *Service) handleCommand(ctx context.Context, msg *bridge.Message, room config.Room) {
	body := strings.TrimSpace(msg.Body)
	fields := strings.Fields(body)
	if len(fields) == 0 {
		return
	}
	command := strings.ToLower(fields[0])
	args := strings.TrimSpace(strings.TrimPrefix(body, fields[0]))

	// An edit of a command I typed earlier. Re-running the newest one is what
	// an edit obviously means; re-running an old one is not, because its
	// answer has already been read — and there is no way to un-read it.
	if msg.Edits != "" && !s.rerunEdited(ctx, msg, room, body) {
		return
	}

	// Make it look like a command. This edits MY message as me (see
	// EditMine), so it has to happen before anything that might redact it.
	if s.conf().Notices.FormatCommands {
		if err := s.bridge.EditMine(ctx, msg.RoomID, msg.EventID, body,
			"<code>"+html.EscapeString(body)+"</code>"); err != nil {
			s.log.Debug().Err(err).Msg("Could not reformat the command")
		}
	}

	// Remembered so that an edit of it can be judged later, and so /clear can
	// find what it caused.
	sessionID := int64(0)
	if live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String()); err == nil && live != nil {
		sessionID = live.ID
	}
	if err := s.store.PutCommand(ctx, &store.Command{
		EventID:    msg.EventID.String(),
		RoomKey:    msg.RoomKey,
		ThreadRoot: msg.ThreadRoot.String(),
		RoomID:     msg.RoomID.String(),
		Body:       body,
		SessionID:  sessionID,
	}); err != nil {
		s.log.Debug().Err(err).Msg("Could not record a command")
	}

	var reply string
	switch command {
	case "/":
		reply = s.commandStatus(ctx, msg, room) + "\n`/help` for the commands."
	case "/help":
		reply = helpText
	case "/new":
		reply = s.commandNew(ctx, msg, room, args)
	case "/tools":
		reply = s.commandTools(ctx, msg, room, args)
	case "/agent":
		reply = s.commandAgent(ctx, msg, args)
	case "/model":
		reply = s.commandModel(ctx, msg, room, args)
	case "/think":
		reply = s.commandThink(ctx, msg, room, args)
	case "/stop", "/cancel":
		reply = s.commandStop(ctx, msg)
	case "/btw":
		reply = s.commandBtw(ctx, msg, room, args)
	case "/undo":
		reply = s.commandUndo(ctx, msg, room)
	case "/summary":
		reply = s.commandSummary(ctx, msg, room)
	case "/compact":
		reply = s.commandCompact(ctx, msg, room)
	case "/history":
		reply = s.commandHistory(ctx, msg, room)
	case "/resume", "/continue":
		reply = s.commandResume(ctx, msg, room, args)
	case "/share":
		reply = s.commandShare(ctx, msg)
	case "/retry":
		reply = s.commandRetry(ctx, msg, room)
	case "/link":
		reply = s.commandLink(ctx, msg)
	case "/status":
		reply = s.commandStatus(ctx, msg, room)
	case "/bridge":
		reply = s.commandBridge(ctx, msg, room, args)
	case "/clear", "/clean":
		reply = s.commandClear(ctx, msg, room, args)
	default:
		reply = fmt.Sprintf("`%s` is not a command. `/help` for the list; `//%s` sends it as a message.",
			command, strings.TrimPrefix(body, "/"))
	}
	if reply != "" {
		s.postNotice(ctx, msg, room, reply)
		// Now that there is something to take back, the command gets the
		// button that takes it.
		s.addClearButton(ctx, msg, room)
	}
}

// postNotice answers a message: the notice is a reply to it, so the answer is
// attached to the question that caused it.
func (s *Service) postNotice(ctx context.Context, msg *bridge.Message, room config.Room, text string) {
	replyTo := id.EventID("")
	if s.conf().Notices.RepliesToCommands() {
		replyTo = msg.EventID
	}
	s.postNoticeReply(ctx, msg.RoomID, room.Ghosts[0], msg.ThreadRoot, replyTo, text)
}

// notice is how the bridge speaks for itself when nobody asked: a toolset
// going idle, a delivery giving up. There is no message to reply to.
func (s *Service) notice(ctx context.Context, roomID id.RoomID, ghostKey string, thread id.EventID, text string) {
	s.postNoticeReply(ctx, roomID, ghostKey, thread, "", text)
}

// postNoticeReply is the one place a bridge message is built: its own name and
// avatar where the client understands them, an emoji and italics where it does
// not, and a reply when something asked for it.
func (s *Service) postNoticeReply(ctx context.Context, roomID id.RoomID, ghostKey string, thread, replyTo id.EventID, text string) {
	plain, formatted := s.renderNotice(text)
	opts := bridge.SendOptions{Notice: true, ThreadRoot: thread, ReplyTo: replyTo}
	// The bridge bot is what makes a notice look like the bridge: its m.notice
	// renders as dim centred text with no bubble, in any room. A ghost's does
	// not, so in ghost mode the per-message profile is the only thing left to
	// tell the two voices apart.
	sender := ghostKey
	if s.conf().Notices.Sender == config.NoticeSenderBot {
		sender = bridge.BotKey
	} else {
		opts.Profile = s.noticeProfile(ctx)
	}
	eventID, err := s.bridge.SendText(ctx, roomID, sender, plain, formatted, opts)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to post a notice")
		return
	}
	// Remembered so it can be taken back: /clear, or a reaction on the command
	// that caused it. A bridge message cannot be reacted to, so the command is
	// the only handle the user has.
	roomKey, known := s.bridge.RoomKey(roomID)
	if !known {
		return
	}
	if err := s.store.PutNotice(ctx, &store.Notice{
		EventID:      eventID.String(),
		RoomKey:      roomKey,
		ThreadRoot:   thread.String(),
		RoomID:       roomID.String(),
		Ghost:        sender,
		CommandEvent: replyTo.String(),
	}); err != nil {
		s.log.Debug().Err(err).Msg("Could not record a notice")
	}
}

// renderNotice renders a bridge message. HTML is allowed here — unlike an
// agent's answer, this text is the bridge's own, and it needs the parts of the
// Matrix subset Markdown cannot express (see ui.go).
func (s *Service) renderNotice(text string) (plain, formatted string) {
	return bridge.MarkdownHTML(text)
}

// noticeProfile is the identity a bridge message speaks under. It is not a
// second room member: a per-message profile keeps the dm a dm.
func (s *Service) noticeProfile(ctx context.Context) *notify.Profile {
	cfg := s.conf().Notices
	if cfg.Name == "" {
		return nil
	}
	profile := &notify.Profile{ID: cfg.ID, Displayname: cfg.Name}
	if cfg.Avatar == "" {
		return profile
	}
	s.noticeMu.Lock()
	cached, path := s.noticeAvatar, s.noticeAvatarPath
	s.noticeMu.Unlock()
	if cached != "" && path == cfg.Avatar {
		profile.AvatarURL = cached
		return profile
	}
	uri, err := s.bridge.UploadAvatar(ctx, cfg.Avatar)
	if err != nil {
		// A missing avatar is not a reason to swallow the message; the client
		// generates one from the name.
		s.log.Warn().Err(err).Str("avatar", cfg.Avatar).Msg("Could not upload the notice avatar")
		return profile
	}
	s.noticeMu.Lock()
	s.noticeAvatar, s.noticeAvatarPath = uri, cfg.Avatar
	s.noticeMu.Unlock()
	profile.AvatarURL = uri
	return profile
}

// agentConfigFor resolves which agent a room is currently talking to, override
// included. Commands need it to know which toolsets and models are on offer.
func (s *Service) agentConfigFor(ctx context.Context, room config.Room, roomKey string) (string, config.Agent, bool) {
	name := room.Agent
	if override, _ := s.store.GetKV(ctx, kvAgentOverride+roomKey); override != "" {
		if _, ok := s.conf().Agents[override]; ok {
			name = override
		}
	}
	cfg, ok := s.conf().Agents[name]
	return name, cfg, ok
}

func (s *Service) commandNew(ctx context.Context, msg *bridge.Message, room config.Room, args string) string {
	agentName, agentCfg, haveAgent := s.agentConfigFor(ctx, room, msg.RoomKey)
	_ = agentName

	// A named toolset is on from the first message of the new conversation,
	// which is the difference between escalating and remembering to escalate.
	// `/new rw`, not `/new +rw`: there is nothing here to switch off, so the
	// sign would carry no information.
	var wanted []string
	var unknown []string
	for _, field := range strings.Fields(args) {
		name := strings.TrimPrefix(field, "+")
		if name == "" || !haveAgent {
			unknown = append(unknown, field)
			continue
		}
		if _, ok := agentCfg.Toolsets[name]; !ok {
			unknown = append(unknown, field)
			continue
		}
		wanted = append(wanted, name)
	}
	if len(unknown) > 0 {
		return fmt.Sprintf("`/new` takes toolsets only: %s. Usage: `/new [toolset …]`%s",
			strings.Join(quoteAll(unknown), ", "), toolsetHint(agentCfg))
	}

	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the current session: " + err.Error()
	}
	var out strings.Builder
	if live == nil {
		out.WriteString("Already starting fresh — the next message opens a new conversation.")
	} else {
		if err := s.store.CloseSession(ctx, live.ID); err != nil {
			return "Could not close the conversation: " + err.Error()
		}
		out.WriteString("New conversation.")
		if backend, ok := s.agentFor(live.Agent); ok {
			if link := backend.Link(live.ConvID); link != "" {
				fmt.Fprintf(&out, " Previous: %s", link)
			}
		}
	}

	// The new session does not exist yet — it is created by the first message
	// — so these rows are written for whichever session comes next.
	for _, name := range wanted {
		set := agentCfg.Toolsets[name]
		if err := s.setToolset(ctx, room, msg.RoomKey, msg.RoomID, msg.ThreadRoot, "", name, set, true); err != nil {
			fmt.Fprintf(&out, "\nCould not switch `%s` on: %s", name, err)
		}
	}
	if len(wanted) > 0 {
		fmt.Fprintf(&out, " Starting with %s on.", strings.Join(codeAll(wanted), ", "))
	}
	return out.String()
}

// commandTools lists or switches the toolsets of this conversation.
func (s *Service) commandTools(ctx context.Context, msg *bridge.Message, room config.Room, args string) string {
	_, agentCfg, ok := s.agentConfigFor(ctx, room, msg.RoomKey)
	if !ok {
		return "This room has no agent, so it has no tools."
	}
	if len(agentCfg.Toolsets) == 0 {
		if len(agentCfg.ToolIDs) > 0 {
			return "No switchable toolsets here; the agent's fixed tools are always on."
		}
		return "This agent has no tools configured."
	}

	sessionID := ""
	if live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String()); err == nil && live != nil {
		sessionID = fmt.Sprint(live.ID)
	}
	fields := strings.Fields(args)

	if len(fields) == 0 {
		statuses, _ := s.toolsetStatuses(ctx, agentCfg, msg.RoomKey, msg.ThreadRoot, sessionID)
		rows := make([][]string, 0, len(statuses))
		for _, st := range statuses {
			state := no("off")
			switch {
			case st.On && !st.Expires.IsZero():
				state = yes("on") + colour(colourWarn, fmt.Sprintf(" · %s left", humanDuration(time.Until(st.Expires).Round(time.Minute))))
			case st.On:
				state = yes("on")
			}
			rows = append(rows, []string{code(st.Name), state, st.Description})
		}
		return table([]string{"Toolset", "State", "Grants"}, rows) +
			"\n`/tools +name` on, `-name` off, `off` all of them. Resets with the conversation."
	}

	// `off` disarms everything switchable in one word — the command you want
	// when you are done, not the one where you have to remember what you
	// turned on.
	if len(fields) == 1 && strings.EqualFold(fields[0], "off") {
		var out strings.Builder
		for _, name := range config.SortedKeys(agentCfg.Toolsets) {
			set := agentCfg.Toolsets[name]
			if set.Default {
				continue
			}
			if err := s.setToolset(ctx, room, msg.RoomKey, msg.RoomID, msg.ThreadRoot, sessionID, name, set, false); err != nil {
				fmt.Fprintf(&out, "Could not switch `%s` off: %s\n", name, err)
			}
		}
		if out.Len() > 0 {
			return strings.TrimSpace(out.String())
		}
		return "Everything switchable is off."
	}

	// `+name` switches on, `-name` off, a bare name toggles; several in one
	// command are applied in order, so `/tools +rw -other` is one message.
	var out strings.Builder
	statuses, _ := s.toolsetStatuses(ctx, agentCfg, msg.RoomKey, msg.ThreadRoot, sessionID)
	current := make(map[string]bool, len(statuses))
	for _, st := range statuses {
		current[st.Name] = st.On
	}
	for _, field := range fields {
		name := strings.TrimLeft(field, "+-")
		set, known := agentCfg.Toolsets[name]
		if name == "" || !known {
			fmt.Fprintf(&out, "No toolset called `%s`.%s\n", strings.TrimLeft(field, "+-"), toolsetHint(agentCfg))
			continue
		}
		on := !current[name]
		switch {
		case strings.HasPrefix(field, "+"):
			on = true
		case strings.HasPrefix(field, "-"):
			on = false
		}
		if err := s.setToolset(ctx, room, msg.RoomKey, msg.RoomID, msg.ThreadRoot, sessionID, name, set, on); err != nil {
			fmt.Fprintf(&out, "Could not change `%s`: %s\n", name, err)
			continue
		}
		current[name] = on
		switch {
		case !on:
			fmt.Fprintf(&out, "`%s` is off.\n", name)
		case time.Duration(set.Idle) > 0:
			fmt.Fprintf(&out, "`%s` is on for this conversation, and switches off after %s idle.\n",
				name, humanDuration(time.Duration(set.Idle)))
		default:
			fmt.Fprintf(&out, "`%s` is on for this conversation.\n", name)
		}
	}
	return strings.TrimSpace(out.String())
}

func (s *Service) commandAgent(ctx context.Context, msg *bridge.Message, name string) string {
	agents := config.SortedKeys(s.conf().Agents)
	switch name {
	case "":
		override, _ := s.store.GetKV(ctx, kvAgentOverride+msg.RoomKey)
		rows := make([][]string, 0, len(agents))
		for _, key := range agents {
			cfg := s.conf().Agents[key]
			name := code(key)
			if key == override {
				name = yes(key)
			}
			rows = append(rows, []string{name, cfg.Type, code(cfg.Model)})
		}
		return table([]string{"Agent", "Type", "Model"}, rows) +
			"\n`/agent <name>` switches; `/agent reset` returns to the room's own."
	case "reset", "default":
		if err := s.store.SetKV(ctx, kvAgentOverride+msg.RoomKey, ""); err != nil {
			return "Could not reset the agent: " + err.Error()
		}
		return "Back to the room's configured agent. The next message starts a new conversation."
	}
	if _, ok := s.conf().Agents[name]; !ok {
		return fmt.Sprintf("No agent called `%s`. Configured: %s", name, strings.Join(codeAll(agents), ", "))
	}
	if err := s.store.SetKV(ctx, kvAgentOverride+msg.RoomKey, name); err != nil {
		return "Could not switch agent: " + err.Error()
	}
	return fmt.Sprintf("This room now talks to `%s`. The next message starts a new conversation.", name)
}

func (s *Service) commandModel(ctx context.Context, msg *bridge.Message, room config.Room, model string) string {
	switch model {
	case "":
		return s.modelList(ctx, msg, room)
	case "reset", "default":
		if err := s.store.SetKV(ctx, kvModelOverride+msg.RoomKey, ""); err != nil {
			return "Could not reset the model: " + err.Error()
		}
		return "Model reset to the agent's default."
	}
	if err := s.store.SetKV(ctx, kvModelOverride+msg.RoomKey, model); err != nil {
		return "Could not switch model: " + err.Error()
	}
	return fmt.Sprintf("Model for this room is now `%s`. It applies to the next conversation.", model)
}

// modelList asks the backend what it serves. A backend that cannot say so is
// not an error: the command still takes an id, it just cannot offer a menu.
func (s *Service) modelList(ctx context.Context, msg *bridge.Message, room config.Room) string {
	agentName, agentCfg, ok := s.agentConfigFor(ctx, room, msg.RoomKey)
	if !ok {
		return "This room has no agent, so it has no models."
	}
	current := agentCfg.Model
	if override, _ := s.store.GetKV(ctx, kvModelOverride+msg.RoomKey); override != "" {
		current = override
	}
	backend, built := s.agentFor(agentName)
	if !built {
		return fmt.Sprintf("Current model: `%s`.", current)
	}
	listCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	models, err := backend.Models(listCtx)
	switch {
	case errors.Is(err, agent.ErrUnsupported):
		return fmt.Sprintf("Current model: `%s`. This backend does not publish a model list — `/model <id>` still works.", current)
	case err != nil:
		return fmt.Sprintf("Current model: `%s`. Could not read the list: %s", current, err)
	case len(models) == 0:
		return fmt.Sprintf("Current model: `%s`. The backend returned no models.", current)
	}
	sort.Strings(models)
	// A long catalogue is a wall of one-cell rows; three columns of ids is
	// both shorter and easier to scan.
	const cols = 3
	rows := make([][]string, 0, (len(models)+cols-1)/cols)
	for i := 0; i < len(models); i += cols {
		row := make([]string, 0, cols)
		for _, m := range models[i:min(i+cols, len(models))] {
			if m == current {
				row = append(row, yes(m))
				continue
			}
			row = append(row, code(m))
		}
		rows = append(rows, row)
	}
	return fmt.Sprintf("**Models** — current is %s\n\n", yes(current)) +
		table([]string{"", "", ""}, rows) +
		"\n`/model <id>` switches for the next conversation; `/model reset` returns to the agent's default."
}

// commandStop cancels the turn running in this conversation. The turn's own
// context is what the backend is waiting on, so cancelling it is enough; the
// backend is also told, for the ones that charge for an abandoned run.
func (s *Service) commandStop(ctx context.Context, msg *bridge.Message) string {
	key := sessionKey(msg.RoomKey, msg.ThreadRoot)
	s.turnMu.Lock()
	turn, running := s.turns[key]
	if running {
		turn.cancel()
		delete(s.turns, key)
	}
	s.turnMu.Unlock()
	if !running {
		return "Nothing is running."
	}
	if live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String()); err == nil && live != nil {
		if backend, ok := s.agentFor(live.Agent); ok {
			if err := backend.Cancel(ctx, live.ConvID); err != nil && !errors.Is(err, agent.ErrUnsupported) {
				s.log.Debug().Err(err).Msg("Backend would not cancel the turn")
			}
		}
	}
	return "Stopped."
}

// commandRetry asks the last question again, from the same point in the
// conversation — the same thing an edit does, without retyping it.
func (s *Service) commandRetry(ctx context.Context, msg *bridge.Message, room config.Room) string {
	if room.Agent == "" {
		return "This room has no agent."
	}
	last, err := s.store.LastTurn(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the last turn: " + err.Error()
	}
	if last == nil {
		return "Nothing to retry yet."
	}
	retry := *msg
	retry.EventID = id.EventID(last.EventID)
	retry.Body = last.Question
	retry.Attachment = nil
	// Edits re-run a turn from the same parent, which is what a retry is.
	retry.Edits = id.EventID(last.EventID)
	go s.handleTurn(detach(ctx), &retry, room)
	return ""
}

func (s *Service) commandLink(ctx context.Context, msg *bridge.Message) string {
	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the session: " + err.Error()
	}
	if live == nil {
		return "No conversation yet — the next message starts one."
	}
	backend, ok := s.agentFor(live.Agent)
	if !ok {
		return "This conversation's agent is no longer configured."
	}
	link := backend.Link(live.ConvID)
	if link == "" {
		return "This backend has nowhere to open."
	}
	return link
}

func (s *Service) commandStatus(ctx context.Context, msg *bridge.Message, room config.Room) string {
	// Two columns, one row per fact: the same content as a bullet list in
	// about half the height, and left-aligned inside the centred notice.
	rows := make([][]string, 0, 8)
	add := func(label, value string) { rows = append(rows, []string{label, value}) }

	agentName, agentCfg, haveAgent := s.agentConfigFor(ctx, room, msg.RoomKey)
	switch {
	case !haveAgent:
		add("Agent", no("none — announcements only"))
	case agentName != room.Agent:
		add("Agent", code(agentName)+colour(colourWarn, " · override"))
	default:
		add("Agent", code(agentName))
	}

	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	sessionID := ""
	switch {
	case err != nil:
		add("Session", colour(colourBad, "unreadable")+" — "+err.Error())
	case live == nil:
		add("Session", no("none yet — the next message starts one"))
	default:
		sessionID = fmt.Sprint(live.ID)
		model := code(live.Model)
		if note := reasoningNote(s.conf().Reasoning, live.Model); note != "" {
			model += " · " + note
		}
		add("Model", model)
		add("Session", fmt.Sprintf("%d turns · last active %s ago",
			live.Turns, time.Since(time.UnixMilli(live.LastActive)).Round(time.Second)))
	}

	if live != nil {
		if backend, ok := s.agentFor(live.Agent); ok {
			usageCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			if usage, err := backend.ContextUsage(usageCtx, live.ConvID); err == nil && usage != "" {
				add("Context", usage)
			}
			cancel()
		}
	}
	if last, err := s.store.LastTurn(ctx, msg.RoomKey, msg.ThreadRoot.String()); err == nil && last != nil {
		if line := usageLine(last.Usage); line != "" {
			add("Last turn", line)
		}
	}

	if haveAgent && len(agentCfg.Toolsets) > 0 {
		statuses, _ := s.toolsetStatuses(ctx, agentCfg, msg.RoomKey, msg.ThreadRoot, sessionID)
		var on []string
		for _, st := range statuses {
			if st.On {
				on = append(on, yes(st.Name))
			}
		}
		if len(on) == 0 {
			add("Tools", no("fixed set only")+" · `/tools` to add")
		} else {
			add("Tools", strings.Join(on, ", ")+" · `/tools` to change")
		}
	}

	if s.bridge.Connected() {
		add("Transport", colour(colourOn, "connected")+fmt.Sprintf(" · up %s", time.Since(s.startedAt).Round(time.Second)))
	} else {
		add("Transport", colour(colourBad, "**disconnected**")+", retrying")
	}
	return table([]string{room.Name, fmt.Sprintf("`%s` · %s", msg.RoomKey, room.Kind)}, rows)
}

func toolsetHint(agentCfg config.Agent) string {
	if len(agentCfg.Toolsets) == 0 {
		return ""
	}
	return " Available: " + strings.Join(codeAll(config.SortedKeys(agentCfg.Toolsets)), ", ") + "."
}

func codeAll(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, "`"+item+"`")
	}
	return out
}

func quoteAll(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, fmt.Sprintf("%q", item))
	}
	return out
}
