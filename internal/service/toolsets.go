package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// Switchable tool groups, per conversation.
//
// A room's tools are two things at once: what it is allowed to reach, and what
// this particular question should be allowed to do. The first is config
// (agent.tool_ids and agent.toolsets), the second is state — and it belongs to
// the conversation, not the room, so that an escalation cannot outlive the
// thing it was granted for.
//
// The state is one KV row per conversation slot and toolset:
//
//	toolset:<room>\x00<thread>:<name>  ->  <session|next>:<on|off>:<enabled ms>
//
// The session id in the value is what makes the reset automatic: a row written
// for session 12 means nothing to session 13, so a new conversation — /new, an
// idle rotation, a turn ceiling — starts from the configured defaults without
// anything having to remember to clear it. A row written before the first
// message of a conversation exists (`/new +write`, or /tools in a fresh room)
// says `next` and is adopted by whichever session comes first.
const kvToolsetPrefix = "toolset:"

const (
	toolsetNextSession = "next"
	toolsetOn          = "on"
	toolsetOff         = "off"
)

// toolsetStatus is one toolset as it stands right now, for /tools and /status.
type toolsetStatus struct {
	Name        string
	Description string
	On          bool
	Default     bool
	Idle        time.Duration
	// Expires is when an idle timeout will turn this off, if one is running.
	Expires time.Time
	Tools   []string
}

func toolsetKey(roomKey string, thread id.EventID, name string) string {
	return kvToolsetPrefix + roomKey + "\x00" + thread.String() + ":" + name
}

// timerKey is the same slot without the toolset name, for the idle timers.
func toolsetTimerKey(roomKey string, thread id.EventID, name string) string {
	return roomKey + "\x00" + thread.String() + ":" + name
}

// parseToolsetRow reads a stored row. An unparseable row is treated as absent,
// because a toolset that cannot be read is a toolset that is off.
func parseToolsetRow(raw string) (session string, on bool, since time.Time, ok bool) {
	parts := strings.SplitN(raw, ":", 3)
	if len(parts) != 3 {
		return "", false, time.Time{}, false
	}
	ms, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return "", false, time.Time{}, false
	}
	return parts[0], parts[1] == toolsetOn, time.UnixMilli(ms), true
}

func formatToolsetRow(session string, on bool, since time.Time) string {
	state := toolsetOff
	if on {
		state = toolsetOn
	}
	return fmt.Sprintf("%s:%s:%d", session, state, since.UnixMilli())
}

// toolsetStatuses resolves every toolset of an agent for one conversation.
// sessionID is "" when the conversation has not started yet, in which case
// only rows marked `next` count.
//
// It is also where an expired toolset is noticed after a restart, when the
// in-memory timer that should have closed it is gone: the row is dropped, the
// toolset reads as off, and the caller is told so it can say so in the room.
func (s *Service) toolsetStatuses(ctx context.Context, agentCfg config.Agent, roomKey string, thread id.EventID, sessionID string) ([]toolsetStatus, []string) {
	var expired []string
	out := make([]toolsetStatus, 0, len(agentCfg.Toolsets))
	for _, name := range config.SortedKeys(agentCfg.Toolsets) {
		set := agentCfg.Toolsets[name]
		status := toolsetStatus{
			Name:        name,
			Description: set.Description,
			On:          set.Default,
			Default:     set.Default,
			Idle:        time.Duration(set.Idle),
			Tools:       set.Tools,
		}
		key := toolsetKey(roomKey, thread, name)
		raw, err := s.store.GetKV(ctx, key)
		if err == nil && raw != "" {
			rowSession, on, since, ok := parseToolsetRow(raw)
			switch {
			case !ok:
				_ = s.store.SetKV(ctx, key, "")
			case rowSession != toolsetNextSession && rowSession != sessionID:
				// Written for a conversation that has ended.
				_ = s.store.SetKV(ctx, key, "")
			case on && status.Idle > 0 && time.Since(since) >= status.Idle:
				_ = s.store.SetKV(ctx, key, "")
				expired = append(expired, name)
			default:
				status.On = on
				if on && status.Idle > 0 {
					status.Expires = since.Add(status.Idle)
				}
			}
		}
		out = append(out, status)
	}
	return out, expired
}

// activeTools is the tool id list for a turn: everything the agent always has,
// plus every toolset that is on.
func (s *Service) activeTools(ctx context.Context, agentCfg config.Agent, roomKey string, thread id.EventID, sessionID string) ([]string, []string) {
	statuses, expired := s.toolsetStatuses(ctx, agentCfg, roomKey, thread, sessionID)
	var tools []string
	for _, status := range statuses {
		if status.On {
			tools = append(tools, status.Tools...)
		}
	}
	return tools, expired
}

// setToolset switches one toolset on or off for a conversation, and arms or
// cancels its idle timer. sessionID is "" for a conversation that has not
// started; the row then waits for the next one.
func (s *Service) setToolset(ctx context.Context, room config.Room, roomKey string, roomID id.RoomID, thread id.EventID, sessionID, name string, set config.Toolset, on bool) error {
	session := sessionID
	if session == "" {
		session = toolsetNextSession
	}
	key := toolsetKey(roomKey, thread, name)
	if on == set.Default {
		// Back to the configured state: no row is the same thing, and an empty
		// store is easier to reason about than a store full of "same as
		// default".
		if err := s.store.SetKV(ctx, key, ""); err != nil {
			return err
		}
	} else if err := s.store.SetKV(ctx, key, formatToolsetRow(session, on, time.Now())); err != nil {
		return err
	}
	s.armToolsetTimer(room, roomKey, roomID, thread, name, set, on)
	return nil
}

// armToolsetTimer (re)starts the idle countdown for a toolset that is on, and
// stops it for one that is off. The timer is what makes the auto-off visible:
// without it the toolset would only be noticed as expired by the next turn,
// which is exactly the turn that should not have had it.
func (s *Service) armToolsetTimer(room config.Room, roomKey string, roomID id.RoomID, thread id.EventID, name string, set config.Toolset, on bool) {
	timerKey := toolsetTimerKey(roomKey, thread, name)
	s.toolsetMu.Lock()
	if old := s.toolsetTimers[timerKey]; old != nil {
		old.Stop()
		delete(s.toolsetTimers, timerKey)
	}
	idle := time.Duration(set.Idle)
	if !on || idle <= 0 {
		s.toolsetMu.Unlock()
		return
	}
	s.toolsetTimers[timerKey] = time.AfterFunc(idle, func() {
		s.expireToolset(room, roomKey, roomID, thread, name, idle)
	})
	s.toolsetMu.Unlock()
}

// expireToolset turns an idle toolset off and says so in the room.
func (s *Service) expireToolset(room config.Room, roomKey string, roomID id.RoomID, thread id.EventID, name string, idle time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	timerKey := toolsetTimerKey(roomKey, thread, name)
	s.toolsetMu.Lock()
	delete(s.toolsetTimers, timerKey)
	s.toolsetMu.Unlock()

	key := toolsetKey(roomKey, thread, name)
	raw, err := s.store.GetKV(ctx, key)
	if err != nil || raw == "" {
		return
	}
	if _, on, _, ok := parseToolsetRow(raw); !ok || !on {
		return
	}
	if err := s.store.SetKV(ctx, key, ""); err != nil {
		s.log.Warn().Err(err).Str("toolset", name).Msg("Could not expire a toolset")
		return
	}
	if len(room.Ghosts) == 0 {
		return
	}
	s.notice(ctx, roomID, room.Ghosts[0], thread,
		fmt.Sprintf("`%s` switched off after %s idle.", name, humanDuration(idle)))
}

// touchToolsets restarts the idle countdown of every toolset that is on. Idle
// means "no turns in this conversation", so every turn pushes the deadline out.
func (s *Service) touchToolsets(ctx context.Context, agentCfg config.Agent, room config.Room, roomKey string, roomID id.RoomID, thread id.EventID, sessionID string) {
	for _, name := range config.SortedKeys(agentCfg.Toolsets) {
		set := agentCfg.Toolsets[name]
		if time.Duration(set.Idle) <= 0 {
			continue
		}
		key := toolsetKey(roomKey, thread, name)
		raw, err := s.store.GetKV(ctx, key)
		if err != nil || raw == "" {
			continue
		}
		rowSession, on, _, ok := parseToolsetRow(raw)
		if !ok || !on {
			continue
		}
		session := sessionID
		if session == "" {
			session = rowSession
		}
		if err := s.store.SetKV(ctx, key, formatToolsetRow(session, true, time.Now())); err != nil {
			continue
		}
		s.armToolsetTimer(room, roomKey, roomID, thread, name, set, true)
	}
}

// adoptToolsets binds rows written before the conversation existed to it, so
// `/new +write` survives into the session it was meant for.
func (s *Service) adoptToolsets(ctx context.Context, agentCfg config.Agent, roomKey string, thread id.EventID, sessionID string) {
	if sessionID == "" {
		return
	}
	for _, name := range config.SortedKeys(agentCfg.Toolsets) {
		key := toolsetKey(roomKey, thread, name)
		raw, err := s.store.GetKV(ctx, key)
		if err != nil || raw == "" {
			continue
		}
		rowSession, on, since, ok := parseToolsetRow(raw)
		if !ok || rowSession != toolsetNextSession {
			continue
		}
		_ = s.store.SetKV(ctx, key, formatToolsetRow(sessionID, on, since))
	}
}

// humanDuration prints a timeout the way an operator would say it.
func humanDuration(d time.Duration) string {
	switch {
	case d >= time.Hour && d%time.Hour == 0:
		return fmt.Sprintf("%d h", int(d.Hours()))
	case d >= time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("%d min", int(d.Minutes()))
	default:
		return d.Round(time.Second).String()
	}
}
