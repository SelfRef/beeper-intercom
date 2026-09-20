package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
)

// How hard the model thinks, as a command.
//
// Reasoning effort is not a parameter in most self-hosted catalogues: it is a
// separate model id, one per level, by convention a suffix on a shared base
// (`some-model`, `some-model:t`, `some-model:m`). So /think is /model with the
// base held fixed — and because the convention is the deployment's, not the
// bridge's, the suffixes come from config, including the one that means "do
// not think" when that is not the bare id (reasoning.off_suffix).
//
// The change belongs to the conversation, not the room: a question that needs
// more thought is a question, not a new setting. It is written to the session's
// own model column, so the next conversation is back to the room's model.
// Asked before a conversation exists, it waits in one KV row for the one the
// next message opens.
const kvThinkPending = "think_pending:"

func thinkPendingKey(msg *bridge.Message) string {
	return kvThinkPending + msg.RoomKey + "\x00" + msg.ThreadRoot.String()
}

func (s *Service) commandThink(ctx context.Context, msg *bridge.Message, room config.Room, args string) string {
	reasoning := s.conf().Reasoning
	if !reasoning.Enabled {
		return "Reasoning levels are not configured for this installation — see `reasoning:` in the bridge config."
	}
	agentName, agentCfg, ok := s.agentConfigFor(ctx, room, msg.RoomKey)
	if !ok {
		return "This room has no agent, so it has nothing to think with."
	}

	live, err := s.store.LiveSession(ctx, msg.RoomKey, msg.ThreadRoot.String())
	if err != nil {
		return "Could not read the session: " + err.Error()
	}
	current := s.currentModel(ctx, msg, agentCfg)
	if live != nil {
		current = live.Model
	}
	base, level := reasoning.Split(current)

	// What this base model actually offers. The catalogue is the authority:
	// a level that is configured but not served would be a model id that 404s
	// on the next message.
	variants, known := s.reasoningVariants(ctx, agentName, base, reasoning)

	name := strings.ToLower(strings.TrimSpace(args))
	if name == "" {
		return reasoningList(base, level, reasoning, variants, known)
	}

	var target, label string
	switch {
	case name == config.ReasoningOff:
		off := reasoning.Off()
		label = off.Label()
		if known && !variants[config.ReasoningOff] {
			return fmt.Sprintf("`%s` cannot stop thinking — there is no %s variant of it.", base, code(base+off.Suffix))
		}
		target = base + off.Suffix
	default:
		wanted, found := reasoning.Level(name)
		if !found {
			return fmt.Sprintf("No reasoning level called `%s`.%s", name, reasoningHint(reasoning, variants, known))
		}
		if known && !variants[wanted.ID()] {
			return fmt.Sprintf("`%s` has no %s variant.%s", base, wanted.Label(), reasoningHint(reasoning, variants, known))
		}
		target, label = base+wanted.Suffix, wanted.Label()
	}

	if target == current {
		return fmt.Sprintf("Already %s (`%s`).", label, current)
	}
	if live == nil {
		if err := s.store.SetKV(ctx, thinkPendingKey(msg), name); err != nil {
			return "Could not set the reasoning level: " + err.Error()
		}
		return fmt.Sprintf("The conversation the next message starts will use %s (`%s`).", label, target)
	}
	if err := s.store.SetSessionModel(ctx, live.ID, target); err != nil {
		return "Could not set the reasoning level: " + err.Error()
	}
	_ = s.store.SetKV(ctx, thinkPendingKey(msg), "")
	return fmt.Sprintf("Now %s (`%s`) — this conversation only.", label, target)
}

// reasoningList is the menu: every level this base model actually has, with
// the current one marked. A table, not a list — see ui.go.
func reasoningList(base string, level *config.ReasoningLevel, reasoning config.Reasoning, variants map[string]bool, known bool) string {
	if len(reasoningOffered(reasoning, variants, known)) == 0 {
		return fmt.Sprintf("`%s` has no reasoning variants — it answers at one effort.", base)
	}
	currentID := config.ReasoningOff
	if level != nil {
		currentID = level.ID()
	} else if reasoning.OffSuffix != "" {
		// A bare id whose effort the catalogue chooses: it is not the off
		// variant, so nothing in the table is the current one.
		currentID = ""
	}

	rows := make([][]string, 0, len(reasoning.Levels)+1)
	add := func(id, label, suffix, description string, ids []string) {
		name, model := label, code(base+suffix)
		if id == currentID {
			name, model = yes(label), yes(base+suffix)
		}
		row := []string{name, strings.Join(codeAll(ids), " "), model}
		if description != "" {
			row[0] += " — " + description
		}
		rows = append(rows, row)
	}
	if !known || variants[config.ReasoningOff] {
		off := reasoning.Off()
		add(config.ReasoningOff, off.Label(), off.Suffix, "", off.IDs)
	}
	for _, lvl := range reasoning.Levels {
		if known && !variants[lvl.ID()] {
			continue
		}
		add(lvl.ID(), lvl.Label(), lvl.Suffix, lvl.Description, lvl.IDs)
	}

	out := table([]string{"Reasoning for " + code(base), "Type", "Model"}, rows)
	if !known {
		out += "\nThe backend publishes no model list, so these are the configured levels rather than the ones it is known to serve.\n"
	}
	return out + "\n`/think <level>` applies to this conversation only."
}

// reasoningVariants asks the backend which levels of a base model it serves.
// known is false when it cannot say, in which case the configured levels are
// offered unchecked — a command that refuses to work because a backend has no
// model list would be worse than one that occasionally names a missing id.
func (s *Service) reasoningVariants(ctx context.Context, agentName, base string, reasoning config.Reasoning) (map[string]bool, bool) {
	backend, built := s.agentFor(agentName)
	if !built {
		return nil, false
	}
	listCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	models, err := backend.Models(listCtx)
	if err != nil || len(models) == 0 {
		if err != nil && !errors.Is(err, agent.ErrUnsupported) {
			s.log.Debug().Err(err).Msg("Could not list models for /think")
		}
		return nil, false
	}
	served := make(map[string]bool, len(models))
	for _, m := range models {
		served[m] = true
	}
	variants := map[string]bool{config.ReasoningOff: served[base+reasoning.OffSuffix]}
	for _, level := range reasoning.Levels {
		variants[level.ID()] = served[base+level.Suffix]
	}
	return variants, true
}

func reasoningOffered(reasoning config.Reasoning, variants map[string]bool, known bool) []string {
	var out []string
	if !known || variants[config.ReasoningOff] {
		out = append(out, config.ReasoningOff)
	}
	for _, level := range reasoning.Levels {
		if !known || variants[level.ID()] {
			out = append(out, level.ID())
		}
	}
	return out
}

func reasoningHint(reasoning config.Reasoning, variants map[string]bool, known bool) string {
	offered := reasoningOffered(reasoning, variants, known)
	if len(offered) == 0 {
		return ""
	}
	return " Available: " + strings.Join(codeAll(offered), ", ") + "."
}

// currentModel is the model a new conversation in this room would use.
func (s *Service) currentModel(ctx context.Context, msg *bridge.Message, agentCfg config.Agent) string {
	model := agentCfg.Model
	if override, _ := s.store.GetKV(ctx, kvModelOverride+msg.RoomKey); override != "" {
		model = override
	}
	return model
}

// applyPendingReasoning rewrites the model of a conversation that is about to
// be created, when /think was used before there was one to change.
func (s *Service) applyPendingReasoning(ctx context.Context, roomKey, threadRoot, model string) string {
	reasoning := s.conf().Reasoning
	if !reasoning.Enabled {
		return model
	}
	key := kvThinkPending + roomKey + "\x00" + threadRoot
	name, err := s.store.GetKV(ctx, key)
	if err != nil || name == "" {
		return model
	}
	// One conversation is what it was asked for; the one after is not.
	_ = s.store.SetKV(ctx, key, "")
	base, _ := reasoning.Split(model)
	if name == config.ReasoningOff {
		return base + reasoning.OffSuffix
	}
	level, found := reasoning.Level(name)
	if !found {
		return model
	}
	return base + level.Suffix
}

// reasoningNote is the reasoning level as /status prints it beside the model,
// so that "why is this slow" and "which level am I on" are the same glance.
func reasoningNote(reasoning config.Reasoning, model string) string {
	if !reasoning.Enabled {
		return ""
	}
	if _, level := reasoning.Split(model); level != nil {
		if level.Suffix == reasoning.OffSuffix {
			return no(strings.ToLower(level.Label()))
		}
		return level.Label()
	}
	if reasoning.OffSuffix != "" {
		// The catalogue picks the effort for a bare id, and the bridge has no
		// way to ask which one it picked.
		return "default"
	}
	return no("off")
}
