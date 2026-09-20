package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"maunium.net/go/mautrix/id"

	"github.com/SelfRef/beeper-intercom/internal/agent"
	"github.com/SelfRef/beeper-intercom/internal/bridge"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// When the model asks ME something.
//
// Open WebUI's ask_user tool does not run and return: it stops the turn with
// the questions staged on the message, expecting a UI to draw a card and
// resolve it. In a chat there is already a way to ask a question with fixed
// answers — a poll — so each question becomes one, and the answers resolve the
// call, which resumes the same turn where it stopped.
//
// askLimit stops a model that answers every answer with another question.
const askLimit = 3

// resolveAsks runs the question-and-answer loop for a reply that stopped to
// ask something, and returns the reply that finally has an answer in it.
func (s *Service) resolveAsks(ctx context.Context, msg *bridge.Message, room config.Room, ghostKey string,
	backend agent.Agent, conv agent.Conversation, reply *agent.Reply, marker *progressMarker, sessionID int64) *agent.Reply {
	for asked := 0; reply != nil && reply.Ask != nil && len(reply.Ask.Questions) > 0; asked++ {
		if asked >= askLimit {
			s.postNotice(ctx, msg, room, "The model keeps asking questions; I stopped answering them. Say what you want in a message.")
			return reply
		}
		// Anything the model said before the question is context for it. The
		// resumed turn continues the SAME message, so its content comes back
		// with this text still at the front — remembered here, stripped below.
		said := strings.TrimSpace(reply.Text)
		if text := said; text != "" {
			plain, formatted := bridge.Markdown(text)
			if _, err := s.bridge.SendText(ctx, msg.RoomID, ghostKey, plain, formatted,
				bridge.SendOptions{ThreadRoot: msg.ThreadRoot}); err != nil {
				s.log.Warn().Err(err).Msg("Could not post what the model said before its question")
			}
		}

		marker.set(ctx, "asking")
		answers, carryOn := s.askQuestions(ctx, msg, room, ghostKey, reply.Ask, sessionID)
		marker.set(ctx, "queued")
		if !carryOn {
			// The question was dropped for a message. Tell the backend so the
			// paused turn is not left waiting forever, but do not wait for
			// what it says — the next turn is already the answer.
			s.dropQuestion(msg, backend, conv, reply.Ask)
			return nil
		}

		// Status only: the answer to a question is posted as one message, so
		// there is no stream to grow — but the phase still moves.
		next, err := backend.Answer(ctx, conv, reply.Ask, answers,
			agent.Sink{Status: func(state string) { marker.set(ctx, state) }})
		switch {
		case err != nil:
			s.postNotice(ctx, msg, room, "Could not deliver the answer: "+err.Error())
			return reply
		case next == nil:
			return reply
		}
		// Carry forward what the first part of the turn reported, so /status
		// still has the tokens of the run that produced the answer.
		if next.Usage == nil {
			next.Usage = reply.Usage
		}
		if next.UserMessage == "" {
			next.UserMessage = reply.UserMessage
		}
		next.Text = withoutPrefix(next.Text, said)
		reply = next
	}
	return reply
}

// Why a question produced no answer.
var (
	errQuestionTimedOut = errors.New("no answer in time")
	errQuestionDropped  = errors.New("dropped for a message")
)

// askQuestions posts one poll per question and waits for the answers. An
// unanswered question is simply absent from the map, which tells the backend
// the model has to carry on without it — and the room is told why, because a
// question that quietly stops mattering is worse than one that says so.
func (s *Service) askQuestions(ctx context.Context, msg *bridge.Message, room config.Room, ghostKey string,
	ask *agent.Ask, sessionID int64) (map[string]string, bool) {
	timeout := s.conf().Questions.Timeout.Or(10 * time.Minute)
	answers := make(map[string]string, len(ask.Questions))
	for _, question := range ask.Questions {
		answer, err := s.askOne(ctx, msg, room, ghostKey, question, ask, sessionID, timeout)
		switch {
		case errors.Is(err, errQuestionDropped):
			// A message arrived instead of an answer: that message is the
			// conversation now, and this turn is over.
			return answers, false
		case errors.Is(err, errQuestionTimedOut):
			s.postNotice(ctx, msg, room, fmt.Sprintf(
				"No answer in %s, so I carried on without it.", humanDuration(timeout)))
		case err != nil:
			s.log.Debug().Err(err).Str("question", question.ID).Msg("Question went unanswered")
		default:
			answers[question.ID] = answer
		}
	}
	return answers, true
}

// openQuestion is a question waiting to be answered, and what a message typed
// into the room means for it.
type openQuestion struct {
	// answer takes a typed answer, when the model said one is acceptable.
	answer chan string
	// abandoned closes when a message arrives that is NOT an answer: the
	// question is dropped and the message becomes the next turn instead.
	abandoned  chan struct{}
	allowOther bool
	// sessionID and messageID are what a dropped question leaves behind: the
	// abandoned turn still happened, so the conversation continues AFTER it
	// rather than branching off the point before it.
	sessionID int64
	messageID string
}

// How a typed message was treated while a question was open.
const (
	messageIsAMessage = iota // nothing was waiting
	messageIsTheAnswer
	messageDropsTheQuestion
)

func (s *Service) awaitingAnswer(key string, allowOther bool, sessionID int64, messageID string) *openQuestion {
	open := &openQuestion{
		answer:     make(chan string, 1),
		abandoned:  make(chan struct{}),
		allowOther: allowOther,
		sessionID:  sessionID,
		messageID:  messageID,
	}
	s.askMu.Lock()
	s.pendingAsks[key] = open
	s.askMu.Unlock()
	return open
}

func (s *Service) doneAwaiting(key string) {
	s.askMu.Lock()
	delete(s.pendingAsks, key)
	s.askMu.Unlock()
}

// offerMessage gives a typed message to whatever question is open. A question
// that accepts free text takes it as the answer; one that does not is dropped,
// because the alternative — leaving it open while the conversation moves on —
// is what makes an unanswerable question follow you around.
func (s *Service) offerMessage(ctx context.Context, key, text string) int {
	s.askMu.Lock()
	open, waiting := s.pendingAsks[key]
	if waiting {
		delete(s.pendingAsks, key)
	}
	s.askMu.Unlock()
	if !waiting {
		return messageIsAMessage
	}
	if open.allowOther {
		select {
		case open.answer <- text:
			return messageIsTheAnswer
		default:
		}
	}
	// The abandoned turn is still part of the conversation — the question was
	// asked, and it was ignored. Moving the parent on before the message runs
	// is what keeps the next turn in the same branch instead of starting a
	// second one from the point before the question.
	if open.sessionID != 0 && open.messageID != "" {
		if err := s.store.AdvanceSession(ctx, open.sessionID, open.messageID); err != nil {
			s.log.Warn().Err(err).Msg("Could not move past an abandoned question")
		}
	}
	close(open.abandoned)
	return messageDropsTheQuestion
}

// dropQuestion closes a question nobody answered, in the background. The
// backend resumes the turn when a call is resolved, so whatever it says next
// is thrown away: the message that replaced the question is what matters now.
func (s *Service) dropQuestion(msg *bridge.Message, backend agent.Agent, conv agent.Conversation, ask *agent.Ask) {
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 5*time.Minute)
		defer cancel()
		if _, err := backend.Answer(ctx, conv, ask, nil, agent.Sink{}); err != nil {
			s.log.Debug().Err(err).Msg("Could not close an abandoned question")
		}
	}()
}

// askOne posts a single question as a poll and waits for it.
func (s *Service) askOne(ctx context.Context, msg *bridge.Message, room config.Room, ghostKey string,
	question agent.Question, ask *agent.Ask, sessionID int64, timeout time.Duration) (string, error) {
	// A poll answer is one string, so an option's description rides on its
	// label — "Sunny — Clear skies" — which is where Open WebUI shows it too.
	// The model gets its own label back, not this rendering.
	labels := make([]string, 0, len(question.Options))
	byLabel := make(map[string]string, len(question.Options))
	for _, option := range question.Options {
		label := option.Label
		if option.Description != "" {
			label = truncate(option.Label+" — "+option.Description, optionLimit)
		}
		labels = append(labels, label)
		byLabel[label] = option.Label
	}
	if len(labels) == 0 {
		return "", fmt.Errorf("question %q has no options", question.ID)
	}

	eventID, err := s.bridge.SendPoll(ctx, msg.RoomID, ghostKey, questionText(question), labels,
		bridge.SendOptions{ThreadRoot: msg.ThreadRoot})
	if err != nil {
		return "", err
	}
	// Recorded without a webhook: this poll is a question from the model, not
	// a notification anybody automated.
	if err := s.store.PutPoll(ctx, &store.Poll{
		EventID:  eventID.String(),
		RoomKey:  msg.RoomKey,
		RoomID:   msg.RoomID.String(),
		Question: question.Text,
		Answers:  labels,
	}); err != nil {
		return "", err
	}

	ch := make(chan string, 1)
	s.waiters.Store(eventID.String(), ch)
	defer s.waiters.Delete(eventID.String())

	// A Matrix poll takes one of its options and nothing else, but the model
	// may have said a free-form answer is acceptable — so a message typed into
	// the room while the question is open counts as the answer, and the poll
	// says so.
	key := sessionKey(msg.RoomKey, msg.ThreadRoot)
	open := s.awaitingAnswer(key, question.AllowOther, sessionID, ask.MessageID)
	defer s.doneAwaiting(key)

	// A poll response carries the answer's ID; the model asked for the label
	// it offered, which is the one without the description glued on.
	byID := make(map[string]string, len(labels))
	for _, label := range labels {
		byID[bridge.AnswerID(label)] = byLabel[label]
	}

	var answer string
	select {
	case answer = <-ch:
		if label, ok := byID[answer]; ok {
			answer = label
		}
	case answer = <-open.answer:
	case <-open.abandoned:
		err = errQuestionDropped
	case <-time.After(timeout):
		err = errQuestionTimedOut
	case <-ctx.Done():
		err = ctx.Err()
	}
	s.settlePoll(ctx, msg.RoomID, ghostKey, eventID, err == nil)
	return answer, err
}

// settlePoll takes an answered question out of play: ended, so the client
// stops accepting answers that can no longer change anything, or removed
// entirely when the room would rather not keep it.
func (s *Service) settlePoll(ctx context.Context, roomID id.RoomID, ghostKey string, poll id.EventID, answered bool) {
	if answered && s.conf().Questions.DeleteAfterAnswer {
		if err := s.bridge.Redact(ctx, roomID, ghostKey, poll); err != nil {
			s.log.Debug().Err(err).Msg("Could not remove an answered question")
		}
		return
	}
	if err := s.bridge.ClosePoll(ctx, roomID, ghostKey, poll); err != nil {
		s.log.Debug().Err(err).Msg("Could not close a question")
	}
}

// questionText is what the poll asks: the header and the question, and
// nothing else. The options carry their own descriptions (see askOne), so
// repeating them here would put the whole card in the title.
func questionText(question agent.Question) string {
	text := question.Text
	if question.Header != "" && !strings.EqualFold(question.Header, question.Text) {
		text = question.Header + " — " + question.Text
	}
	if question.AllowOther {
		// A Matrix poll has no free-text option, so this is the substitute.
		// It goes on the same line: the question renders as one line whatever
		// is in it, so a newline only makes the seam look like a mistake.
		text += " (or type answer)"
	}
	return text
}

// optionLimit keeps an option readable: Open WebUI allows 80 characters of
// label and 240 of description, which together would be a paragraph in a
// radio button.
const optionLimit = 100

func truncate(text string, limit int) string {
	if len([]rune(text)) <= limit {
		return text
	}
	return string([]rune(text)[:limit-1]) + "…"
}

// withoutPrefix drops text that has already been posted from the front of the
// finished answer, so what the model said before its question is not repeated
// underneath it.
func withoutPrefix(text, posted string) string {
	if posted == "" {
		return text
	}
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, posted) {
		return text
	}
	return strings.TrimSpace(strings.TrimPrefix(trimmed, posted))
}
