package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
)

// Commands are handled by the bridge before anything reaches a backend.
// There is deliberately no other command syntax: everything that is not one
// of these five words is conversation, and a model should never have to guess
// which is which.
const helpText = "**Commands**\n\n" +
	"- `/new [title]` — start a new conversation\n" +
	"- `/agent <name>` — switch the backend for this room\n" +
	"- `/model <id>` — switch the model for this room\n" +
	"- `/status` — room, agent, model, session\n" +
	"- `/help` — this list\n\n" +
	"Anything else is a message to the agent. Replying in a thread starts a side conversation; " +
	"reacting to a notification runs its action."

func (s *Service) handleCommand(ctx context.Context, msg *bridge.Message, room config.Room) {
	fields := strings.Fields(strings.TrimSpace(msg.Body))
	if len(fields) == 0 {
		return
	}
	command := strings.ToLower(fields[0])
	args := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(msg.Body), fields[0]))

	var reply string
	switch command {
	case "/help":
		reply = helpText
	case "/new":
		reply = s.commandNew(ctx, msg, args)
	case "/agent":
		reply = s.commandAgent(ctx, msg, args)
	case "/model":
		reply = s.commandModel(ctx, msg, args)
	case "/status":
		reply = s.commandStatus(ctx, msg, room)
	default:
		// Not a command we know. Treat it as conversation rather than
		// refusing it — a message can legitimately start with a slash.
		if room.Agent != "" {
			s.handleTurn(ctx, msg, room)
		}
		return
	}
	s.postNotice(ctx, msg, room, reply)
}

func (s *Service) postNotice(ctx context.Context, msg *bridge.Message, room config.Room, text string) {
	plain, formatted := bridge.Markdown(text)
	if _, err := s.bridge.SendText(ctx, msg.RoomID, room.Ghosts[0], plain, formatted, bridge.SendOptions{
		Notice:     true,
		ThreadRoot: msg.ThreadRoot,
	}); err != nil {
		s.log.Warn().Err(err).Msg("Failed to answer a command")
	}
}

func (s *Service) commandNew(ctx context.Context, msg *bridge.Message, title string) string {
	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the current session: " + err.Error()
	}
	if live == nil {
		return "Already starting fresh — the next message opens a new conversation."
	}
	if err := s.store.CloseSession(ctx, live.ID); err != nil {
		return "Could not close the conversation: " + err.Error()
	}
	out := "New conversation."
	if backend, ok := s.agentFor(live.Agent); ok {
		if link := backend.Link(live.ConvID); link != "" {
			out += fmt.Sprintf(" Previous: %s", link)
		}
	}
	if title != "" {
		// The title is remembered so the next session can be named, which
		// only matters for backends that show a title anywhere.
		if err := s.store.SetKV(ctx, "next_title:"+msg.RoomKey, title); err == nil {
			out += fmt.Sprintf(" Next one is called “%s”.", title)
		}
	}
	return out
}

func (s *Service) commandAgent(ctx context.Context, msg *bridge.Message, name string) string {
	if name == "" {
		return "Usage: `/agent <name>`. Configured: " + strings.Join(config.SortedKeys(s.conf().Agents), ", ")
	}
	if _, ok := s.conf().Agents[name]; !ok {
		return fmt.Sprintf("No agent called `%s`. Configured: %s", name, strings.Join(config.SortedKeys(s.conf().Agents), ", "))
	}
	if err := s.store.SetKV(ctx, kvAgentOverride+msg.RoomKey, name); err != nil {
		return "Could not switch agent: " + err.Error()
	}
	return fmt.Sprintf("This room now talks to `%s`. The next message starts a new conversation.", name)
}

func (s *Service) commandModel(ctx context.Context, msg *bridge.Message, model string) string {
	if model == "" {
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

func (s *Service) commandStatus(ctx context.Context, msg *bridge.Message, room config.Room) string {
	var out strings.Builder
	fmt.Fprintf(&out, "**%s** (`%s`, %s)\n", room.Name, msg.RoomKey, room.Kind)

	agentName := room.Agent
	if override, _ := s.store.GetKV(ctx, kvAgentOverride+msg.RoomKey); override != "" {
		agentName = override + " (override)"
	}
	if agentName == "" {
		fmt.Fprintf(&out, "- Agent: none — this room only carries announcements\n")
	} else {
		fmt.Fprintf(&out, "- Agent: `%s`\n", agentName)
	}

	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	switch {
	case err != nil:
		fmt.Fprintf(&out, "- Session: unreadable (%s)\n", err)
	case live == nil:
		fmt.Fprintf(&out, "- Session: none yet\n")
	default:
		idle := time.Since(time.UnixMilli(live.LastActive)).Round(time.Second)
		fmt.Fprintf(&out, "- Session: #%d, %d turns, model `%s`, last active %s ago\n",
			live.ID, live.Turns, live.Model, idle)
		if backend, ok := s.agentFor(live.Agent); ok {
			if link := backend.Link(live.ConvID); link != "" {
				fmt.Fprintf(&out, "- Open: %s\n", link)
			}
		}
	}

	if s.bridge.Connected() {
		fmt.Fprintf(&out, "- Transport: connected, up %s\n", time.Since(s.startedAt).Round(time.Second))
	} else {
		fmt.Fprintf(&out, "- Transport: **disconnected**, retrying\n")
	}
	return out.String()
}
