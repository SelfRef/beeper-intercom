// Package store is every piece of state the bridge cannot recompute: the
// appservice registration, which config key became which room, what was
// already sent, what conversation a room is in, and what still has to be
// delivered to a webhook.
//
// One SQLite file, no external database, on purpose — this service has to be
// able to start and announce things when nothing else in the stack is up.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store is the database handle plus the small amount of typing that keeps the
// rest of the code from writing SQL.
type Store struct {
	db *sql.DB
}

const schema = `
CREATE TABLE IF NOT EXISTS kv (
	key         TEXT PRIMARY KEY,
	value       TEXT NOT NULL,
	updated_at  INTEGER NOT NULL
);

-- The config key -> room ID map. Renaming a room in config must patch the
-- existing room, not create a second one, so this is the identity of a room.
CREATE TABLE IF NOT EXISTS rooms (
	key         TEXT PRIMARY KEY,
	room_id     TEXT NOT NULL,
	state_hash  TEXT NOT NULL DEFAULT '',
	created_at  INTEGER NOT NULL,
	updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ghosts (
	key           TEXT PRIMARY KEY,
	mxid          TEXT NOT NULL,
	profile_hash  TEXT NOT NULL DEFAULT '',
	created_at    INTEGER NOT NULL,
	updated_at    INTEGER NOT NULL
);

-- Every notification that reached a room, with the source payload that made
-- it. This is what turns a later reaction into an action with context.
CREATE TABLE IF NOT EXISTS notifications (
	event_id       TEXT PRIMARY KEY,
	room_key       TEXT NOT NULL,
	room_id        TEXT NOT NULL,
	ghost          TEXT NOT NULL,
	title          TEXT NOT NULL DEFAULT '',
	body           TEXT NOT NULL DEFAULT '',
	priority       TEXT NOT NULL DEFAULT '',
	tags           TEXT NOT NULL DEFAULT '[]',
	source_kind    TEXT NOT NULL DEFAULT '',
	source_id      TEXT NOT NULL DEFAULT '',
	source_payload TEXT NOT NULL DEFAULT '',
	actions        TEXT NOT NULL DEFAULT '{}',
	thread_root    TEXT NOT NULL DEFAULT '',
	dedupe         TEXT NOT NULL DEFAULT '',
	created_at     INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS notifications_created ON notifications (created_at);
CREATE UNIQUE INDEX IF NOT EXISTS notifications_dedupe ON notifications (dedupe) WHERE dedupe != '';

-- One row per live conversation. thread_root '' is the main timeline; a
-- thread gets its own row, and therefore its own backend conversation.
CREATE TABLE IF NOT EXISTS sessions (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	room_key     TEXT NOT NULL,
	thread_root  TEXT NOT NULL DEFAULT '',
	agent        TEXT NOT NULL,
	conv_id      TEXT NOT NULL,
	parent_id    TEXT NOT NULL DEFAULT '',
	model        TEXT NOT NULL DEFAULT '',
	title        TEXT NOT NULL DEFAULT '',
	turns        INTEGER NOT NULL DEFAULT 0,
	created_at   INTEGER NOT NULL,
	last_active  INTEGER NOT NULL,
	closed_at    INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS sessions_live ON sessions (room_key, thread_root) WHERE closed_at IS NULL;

-- The transcript the bridge keeps for backends that keep none (the openai
-- adapter). Backends with server-side history never write here.
CREATE TABLE IF NOT EXISTS transcripts (
	conv_id     TEXT NOT NULL,
	idx         INTEGER NOT NULL,
	role        TEXT NOT NULL,
	content     TEXT NOT NULL,
	created_at  INTEGER NOT NULL,
	PRIMARY KEY (conv_id, idx)
);

-- Outbox: a webhook call becomes a row before any HTTP happens, so a match
-- survives the receiver being down.
CREATE TABLE IF NOT EXISTS outbox (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	kind         TEXT NOT NULL,
	url          TEXT NOT NULL,
	payload      TEXT NOT NULL,
	status       TEXT NOT NULL DEFAULT 'pending',
	attempts     INTEGER NOT NULL DEFAULT 0,
	next_attempt INTEGER NOT NULL,
	last_error   TEXT NOT NULL DEFAULT '',
	created_at   INTEGER NOT NULL,
	updated_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (status, next_attempt);

-- Media the bridge re-uploaded, keyed by a hash of the source URL: the same
-- snapshot linked twice is uploaded once.
CREATE TABLE IF NOT EXISTS media (
	hash        TEXT PRIMARY KEY,
	mxc         TEXT NOT NULL,
	mime        TEXT NOT NULL DEFAULT '',
	size        INTEGER NOT NULL DEFAULT 0,
	width       INTEGER NOT NULL DEFAULT 0,
	height      INTEGER NOT NULL DEFAULT 0,
	created_at  INTEGER NOT NULL
);

-- Transaction dedup. Hungryserv transaction ids are opaque strings; dedupe on
-- the string and nothing else.
CREATE TABLE IF NOT EXISTS txns (
	txn_id      TEXT PRIMARY KEY,
	created_at  INTEGER NOT NULL
);

-- Polls the bridge is waiting on, including the MCP call that is blocked on
-- the answer (resolved in-process; the row is what survives a restart).
CREATE TABLE IF NOT EXISTS polls (
	event_id    TEXT PRIMARY KEY,
	room_key    TEXT NOT NULL,
	room_id     TEXT NOT NULL,
	question    TEXT NOT NULL,
	answers     TEXT NOT NULL DEFAULT '[]',
	webhook     TEXT NOT NULL DEFAULT '',
	source      TEXT NOT NULL DEFAULT '',
	response    TEXT NOT NULL DEFAULT '',
	created_at  INTEGER NOT NULL,
	closed_at   INTEGER
);

-- The bridge's own messages: command replies and the notices it posts about
-- itself. Tracked only so they can be taken back again — /clear, and reacting
-- to a command to sweep it away with its answer.
CREATE TABLE IF NOT EXISTS notices (
	event_id      TEXT PRIMARY KEY,
	room_key      TEXT NOT NULL,
	thread_root   TEXT NOT NULL DEFAULT '',
	room_id       TEXT NOT NULL,
	ghost         TEXT NOT NULL DEFAULT '',
	command_event TEXT NOT NULL DEFAULT '',
	created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notices_slot ON notices (room_key, thread_root, created_at);
CREATE INDEX IF NOT EXISTS idx_notices_command ON notices (command_event);

-- The commands I have typed, so that editing one can be answered properly:
-- re-run when it is the newest one in a live conversation, refused when it is
-- an old one whose answer has already been read and acted on.
CREATE TABLE IF NOT EXISTS commands (
	event_id    TEXT PRIMARY KEY,
	room_key    TEXT NOT NULL,
	thread_root TEXT NOT NULL DEFAULT '',
	room_id     TEXT NOT NULL,
	body        TEXT NOT NULL,
	session_id  INTEGER NOT NULL DEFAULT 0,
	button_event TEXT NOT NULL DEFAULT '',
	created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_commands_slot ON commands (room_key, thread_root, created_at);

-- What the bridge sent in answer to which of my messages, so an edit can
-- regenerate and a redaction can cancel.
CREATE TABLE IF NOT EXISTS turns (
	event_id     TEXT PRIMARY KEY,
	session_id   INTEGER NOT NULL,
	user_msg_id  TEXT NOT NULL DEFAULT '',
	parent_id    TEXT NOT NULL DEFAULT '',
	reply_event  TEXT NOT NULL DEFAULT '',
	answer_events TEXT NOT NULL DEFAULT '',
	question     TEXT NOT NULL DEFAULT '',
	usage        TEXT NOT NULL DEFAULT '',
	created_at   INTEGER NOT NULL
);
`

// Open opens (and migrates) the database.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// modernc's driver is safe for concurrent use, but SQLite writers are
	// serialised anyway and a single connection removes a class of
	// "database is locked" surprises under the burst the ntfy mirror can
	// produce after downtime.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	// Columns added after the first release. CREATE TABLE IF NOT EXISTS does
	// nothing to a table that already exists, so each one is an ALTER that is
	// allowed to fail as "duplicate column".
	for _, alter := range []string{
		`ALTER TABLE turns ADD COLUMN question TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN usage TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE turns ADD COLUMN answer_events TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE commands ADD COLUMN button_event TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := db.Exec(alter); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("migrate: %s: %w", alter, err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for the rare query that does not deserve a method.
func (s *Store) DB() *sql.DB { return s.db }

func now() int64 { return time.Now().UnixMilli() }

// --- key/value -------------------------------------------------------------

func (s *Store) GetKV(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetKV(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kv (key, value, updated_at) VALUES (?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value, now())
	return err
}

// GetJSON and SetJSON are the same thing for values that are objects.
func (s *Store) GetJSON(ctx context.Context, key string, out any) (bool, error) {
	raw, err := s.GetKV(ctx, key)
	if err != nil || raw == "" {
		return false, err
	}
	return true, json.Unmarshal([]byte(raw), out)
}

func (s *Store) SetJSON(ctx context.Context, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.SetKV(ctx, key, string(raw))
}

// --- rooms and ghosts ------------------------------------------------------

func (s *Store) RoomID(ctx context.Context, key string) (string, string, error) {
	var roomID, hash string
	err := s.db.QueryRowContext(ctx, `SELECT room_id, state_hash FROM rooms WHERE key = ?`, key).Scan(&roomID, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	return roomID, hash, err
}

func (s *Store) PutRoom(ctx context.Context, key, roomID, stateHash string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO rooms (key, room_id, state_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET room_id = excluded.room_id, state_hash = excluded.state_hash, updated_at = excluded.updated_at`,
		key, roomID, stateHash, now(), now())
	return err
}

// Rooms returns the whole key -> room ID map, for the reverse lookup the
// event handler does on every incoming event.
func (s *Store) Rooms(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, room_id FROM rooms`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

func (s *Store) GhostHash(ctx context.Context, key string) (string, error) {
	var hash string
	err := s.db.QueryRowContext(ctx, `SELECT profile_hash FROM ghosts WHERE key = ?`, key).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return hash, err
}

func (s *Store) PutGhost(ctx context.Context, key, mxid, hash string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO ghosts (key, mxid, profile_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET mxid = excluded.mxid, profile_hash = excluded.profile_hash, updated_at = excluded.updated_at`,
		key, mxid, hash, now(), now())
	return err
}

// --- notifications ---------------------------------------------------------

// Notification is one delivered announcement, as stored.
type Notification struct {
	EventID       string            `json:"event_id"`
	RoomKey       string            `json:"room"`
	RoomID        string            `json:"room_id"`
	Ghost         string            `json:"ghost"`
	Title         string            `json:"title,omitempty"`
	Body          string            `json:"text,omitempty"`
	Priority      string            `json:"priority,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	SourceKind    string            `json:"source_kind,omitempty"`
	SourceID      string            `json:"source_id,omitempty"`
	SourcePayload json.RawMessage   `json:"source_payload,omitempty"`
	Actions       map[string]string `json:"actions,omitempty"`
	ThreadRoot    string            `json:"thread_root,omitempty"`
	Dedupe        string            `json:"dedupe,omitempty"`
	CreatedAt     int64             `json:"created_at"`
}

func (s *Store) PutNotification(ctx context.Context, n *Notification) error {
	tags, _ := json.Marshal(n.Tags)
	actions, _ := json.Marshal(n.Actions)
	payload := string(n.SourcePayload)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO notifications (event_id, room_key, room_id, ghost, title, body, priority, tags,
		    source_kind, source_id, source_payload, actions, thread_root, dedupe, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		n.EventID, n.RoomKey, n.RoomID, n.Ghost, n.Title, n.Body, n.Priority, string(tags),
		n.SourceKind, n.SourceID, payload, string(actions), n.ThreadRoot, n.Dedupe, now())
	return err
}

func scanNotification(scan func(dest ...any) error) (*Notification, error) {
	var n Notification
	var tags, actions, payload string
	if err := scan(&n.EventID, &n.RoomKey, &n.RoomID, &n.Ghost, &n.Title, &n.Body, &n.Priority,
		&tags, &n.SourceKind, &n.SourceID, &payload, &actions, &n.ThreadRoot, &n.Dedupe, &n.CreatedAt); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(tags), &n.Tags)
	_ = json.Unmarshal([]byte(actions), &n.Actions)
	if payload != "" {
		n.SourcePayload = json.RawMessage(payload)
	}
	return &n, nil
}

const notificationCols = `event_id, room_key, room_id, ghost, title, body, priority, tags,
	source_kind, source_id, source_payload, actions, thread_root, dedupe, created_at`

func (s *Store) NotificationByEvent(ctx context.Context, eventID string) (*Notification, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+notificationCols+` FROM notifications WHERE event_id = ?`, eventID)
	n, err := scanNotification(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return n, err
}

func (s *Store) NotificationByDedupe(ctx context.Context, dedupe string) (*Notification, error) {
	if dedupe == "" {
		return nil, nil
	}
	row := s.db.QueryRowContext(ctx, `SELECT `+notificationCols+` FROM notifications WHERE dedupe = ?`, dedupe)
	n, err := scanNotification(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return n, err
}

// NotificationBySource finds the most recent notification a source produced,
// so `thread: "frigate:123"` can name a source id instead of an event ID.
func (s *Store) NotificationBySource(ctx context.Context, kind, id string) (*Notification, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+notificationCols+` FROM notifications WHERE source_kind = ? AND source_id = ? ORDER BY created_at DESC LIMIT 1`, kind, id)
	n, err := scanNotification(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return n, err
}

func (s *Store) RecentNotifications(ctx context.Context, roomKey string, limit int) ([]*Notification, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := `SELECT ` + notificationCols + ` FROM notifications`
	args := []any{}
	if roomKey != "" {
		query += ` WHERE room_key = ?`
		args = append(args, roomKey)
	}
	query += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Notification
	for rows.Next() {
		n, err := scanNotification(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// --- sessions --------------------------------------------------------------

// Session is one live conversation with a backend.
type Session struct {
	ID         int64  `json:"id"`
	RoomKey    string `json:"room"`
	ThreadRoot string `json:"thread_root,omitempty"`
	Agent      string `json:"agent"`
	ConvID     string `json:"conv_id"`
	ParentID   string `json:"parent_id,omitempty"`
	Model      string `json:"model,omitempty"`
	Title      string `json:"title,omitempty"`
	Turns      int    `json:"turns"`
	CreatedAt  int64  `json:"created_at"`
	LastActive int64  `json:"last_active"`
	ClosedAt   *int64 `json:"closed_at,omitempty"`
	// Live is set by the listings that care whether a session is the current
	// one; it is derived from ClosedAt, not stored.
	Live bool `json:"live,omitempty"`
}

const sessionCols = `id, room_key, thread_root, agent, conv_id, parent_id, model, title, turns, created_at, last_active, closed_at`

func scanSession(scan func(dest ...any) error) (*Session, error) {
	var s Session
	var closed sql.NullInt64
	if err := scan(&s.ID, &s.RoomKey, &s.ThreadRoot, &s.Agent, &s.ConvID, &s.ParentID, &s.Model,
		&s.Title, &s.Turns, &s.CreatedAt, &s.LastActive, &closed); err != nil {
		return nil, err
	}
	if closed.Valid {
		v := closed.Int64
		s.ClosedAt = &v
	}
	return &s, nil
}

func (s *Store) LiveSession(ctx context.Context, roomKey, threadRoot string) (*Session, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+sessionCols+` FROM sessions WHERE room_key = ? AND thread_root = ? AND closed_at IS NULL`,
		roomKey, threadRoot)
	sess, err := scanSession(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return sess, err
}

func (s *Store) SessionByID(ctx context.Context, id int64) (*Session, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+sessionCols+` FROM sessions WHERE id = ?`, id)
	sess, err := scanSession(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return sess, err
}

func (s *Store) CreateSession(ctx context.Context, sess *Session) (*Session, error) {
	ts := now()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions (room_key, thread_root, agent, conv_id, parent_id, model, title, turns, created_at, last_active)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		sess.RoomKey, sess.ThreadRoot, sess.Agent, sess.ConvID, sess.ParentID, sess.Model, sess.Title, ts, ts)
	if err != nil {
		return nil, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	sess.ID, sess.CreatedAt, sess.LastActive = id, ts, ts
	return sess, nil
}

// AdvanceSession records one completed turn: the new parent for the next one,
// and the activity timestamp the idle rule reads.
func (s *Store) AdvanceSession(ctx context.Context, id int64, parentID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET parent_id = ?, turns = turns + 1, last_active = ? WHERE id = ?`,
		parentID, now(), id)
	return err
}

// RewindSession is AdvanceSession backwards: /undo puts the parent back to
// where it was before the undone turn and gives the turn count back, so a
// conversation cannot be rotated out by turns that were taken back.
func (s *Store) RewindSession(ctx context.Context, id int64, parentID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE sessions SET parent_id = ?, turns = MAX(turns - 1, 0), last_active = ? WHERE id = ?`,
		parentID, now(), id)
	return err
}

func (s *Store) TouchSession(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET last_active = ? WHERE id = ?`, now(), id)
	return err
}

// SetSessionModel changes the model of a running conversation. The backend
// keeps the branch either way — Open WebUI records a model per message — so a
// conversation can change its mind about how hard to think halfway through.
func (s *Store) SetSessionModel(ctx context.Context, id int64, model string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET model = ? WHERE id = ?`, model, id)
	return err
}

func (s *Store) SetSessionTitle(ctx context.Context, id int64, title string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET title = ? WHERE id = ?`, title, id)
	return err
}

func (s *Store) CloseSession(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET closed_at = ? WHERE id = ? AND closed_at IS NULL`, now(), id)
	return err
}

// ReopenSession makes a closed conversation the live one again — /resume.
// The activity stamp moves with it, because a conversation picked up on
// purpose is not idle, and the idle rule would otherwise rotate it away on the
// very next message. The caller closes whatever was live first: a room and
// thread have exactly one live conversation.
func (s *Store) ReopenSession(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET closed_at = NULL, last_active = ? WHERE id = ?`, now(), id)
	return err
}

func (s *Store) ListSessions(ctx context.Context, includeClosed bool, limit int) ([]*Session, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT ` + sessionCols + ` FROM sessions`
	if !includeClosed {
		query += ` WHERE closed_at IS NULL`
	}
	query += ` ORDER BY last_active DESC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Session
	for rows.Next() {
		sess, err := scanSession(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

// --- transcripts (bridge-held history) -------------------------------------

// TranscriptEntry is one stored turn for a backend without its own history.
type TranscriptEntry struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (s *Store) AppendTranscript(ctx context.Context, convID, role, content string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO transcripts (conv_id, idx, role, content, created_at)
		 VALUES (?, (SELECT COALESCE(MAX(idx), 0) + 1 FROM transcripts WHERE conv_id = ?), ?, ?, ?)`,
		convID, convID, role, content, now())
	return err
}

// TrimTranscript drops the last n entries of a bridge-held transcript, which
// is what /undo means for a backend that keeps no history of its own.
func (s *Store) TrimTranscript(ctx context.Context, convID string, n int) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM transcripts WHERE rowid IN (
			SELECT rowid FROM transcripts WHERE conv_id = ? ORDER BY idx DESC LIMIT ?
		 )`, convID, n)
	return err
}

func (s *Store) Transcript(ctx context.Context, convID string, limit int) ([]TranscriptEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT role, content FROM (
			SELECT idx, role, content FROM transcripts WHERE conv_id = ? ORDER BY idx DESC LIMIT ?
		 ) ORDER BY idx ASC`, convID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TranscriptEntry
	for rows.Next() {
		var e TranscriptEntry
		if err := rows.Scan(&e.Role, &e.Content); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// --- outbox ----------------------------------------------------------------

// Delivery is one queued webhook call.
type Delivery struct {
	ID          int64           `json:"id"`
	Kind        string          `json:"kind"`
	URL         string          `json:"url"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	NextAttempt int64           `json:"next_attempt"`
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   int64           `json:"created_at"`
	UpdatedAt   int64           `json:"updated_at"`
}

func (s *Store) EnqueueDelivery(ctx context.Context, kind, url string, payload any) (int64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	ts := now()
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO outbox (kind, url, payload, status, attempts, next_attempt, created_at, updated_at)
		 VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)`,
		kind, url, string(raw), ts, ts, ts)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) DueDeliveries(ctx context.Context, limit int) ([]*Delivery, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, kind, url, payload, status, attempts, next_attempt, last_error, created_at, updated_at
		 FROM outbox WHERE status = 'pending' AND next_attempt <= ? ORDER BY next_attempt LIMIT ?`, now(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Delivery
	for rows.Next() {
		var d Delivery
		var payload string
		if err := rows.Scan(&d.ID, &d.Kind, &d.URL, &payload, &d.Status, &d.Attempts, &d.NextAttempt,
			&d.LastError, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		d.Payload = json.RawMessage(payload)
		out = append(out, &d)
	}
	return out, rows.Err()
}

func (s *Store) MarkDelivered(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE outbox SET status = 'delivered', updated_at = ? WHERE id = ?`, now(), id)
	return err
}

func (s *Store) MarkDeliveryFailed(ctx context.Context, id int64, attempts int, nextAttempt int64, status, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET status = ?, attempts = ?, next_attempt = ?, last_error = ?, updated_at = ? WHERE id = ?`,
		status, attempts, nextAttempt, errMsg, now(), id)
	return err
}

func (s *Store) RetryDelivery(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE outbox SET status = 'pending', next_attempt = ?, updated_at = ? WHERE id = ?`, now(), now(), id)
	return err
}

func (s *Store) ListDeliveries(ctx context.Context, status string, limit int) ([]*Delivery, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id, kind, url, payload, status, attempts, next_attempt, last_error, created_at, updated_at FROM outbox`
	args := []any{}
	if status != "" {
		query += ` WHERE status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Delivery
	for rows.Next() {
		var d Delivery
		var payload string
		if err := rows.Scan(&d.ID, &d.Kind, &d.URL, &payload, &d.Status, &d.Attempts, &d.NextAttempt,
			&d.LastError, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		d.Payload = json.RawMessage(payload)
		out = append(out, &d)
	}
	return out, rows.Err()
}

// --- media cache -----------------------------------------------------------

// Media is a re-uploaded file.
type Media struct {
	MXC    string
	Mime   string
	Size   int64
	Width  int
	Height int
}

func (s *Store) Media(ctx context.Context, hash string) (*Media, error) {
	var m Media
	err := s.db.QueryRowContext(ctx, `SELECT mxc, mime, size, width, height FROM media WHERE hash = ?`, hash).
		Scan(&m.MXC, &m.Mime, &m.Size, &m.Width, &m.Height)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &m, err
}

func (s *Store) PutMedia(ctx context.Context, hash string, m *Media) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO media (hash, mxc, mime, size, width, height, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(hash) DO UPDATE SET mxc = excluded.mxc`,
		hash, m.MXC, m.Mime, m.Size, m.Width, m.Height, now())
	return err
}

// --- polls -----------------------------------------------------------------

// Poll is a question waiting for an answer.
type Poll struct {
	EventID   string   `json:"event_id"`
	RoomKey   string   `json:"room"`
	RoomID    string   `json:"room_id"`
	Question  string   `json:"question"`
	Answers   []string `json:"answers"`
	Webhook   string   `json:"webhook,omitempty"`
	Source    string   `json:"source,omitempty"`
	Response  string   `json:"response,omitempty"`
	CreatedAt int64    `json:"created_at"`
	ClosedAt  *int64   `json:"closed_at,omitempty"`
}

func (s *Store) PutPoll(ctx context.Context, p *Poll) error {
	answers, _ := json.Marshal(p.Answers)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO polls (event_id, room_key, room_id, question, answers, webhook, source, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		p.EventID, p.RoomKey, p.RoomID, p.Question, string(answers), p.Webhook, p.Source, now())
	return err
}

func (s *Store) Poll(ctx context.Context, eventID string) (*Poll, error) {
	var p Poll
	var answers string
	var closed sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT event_id, room_key, room_id, question, answers, webhook, source, response, created_at, closed_at
		 FROM polls WHERE event_id = ?`, eventID).
		Scan(&p.EventID, &p.RoomKey, &p.RoomID, &p.Question, &answers, &p.Webhook, &p.Source, &p.Response, &p.CreatedAt, &closed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(answers), &p.Answers)
	if closed.Valid {
		v := closed.Int64
		p.ClosedAt = &v
	}
	return &p, nil
}

func (s *Store) AnswerPoll(ctx context.Context, eventID, response string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE polls SET response = ?, closed_at = ? WHERE event_id = ? AND closed_at IS NULL`,
		response, now(), eventID)
	return err
}

// --- commands (what I typed) -----------------------------------------------

// Command is one command message of mine, with the conversation it belonged
// to. Kept so an edit of it can be judged: re-runnable, or too late.
type Command struct {
	EventID    string
	RoomKey    string
	ThreadRoot string
	RoomID     string
	Body       string
	SessionID  int64
	// ButtonEvent is the bot's own reaction on this command: the delete
	// button. Kept so it can be taken away with the command it belongs to,
	// instead of being left pointing at an event that no longer exists.
	ButtonEvent string
	CreatedAt   int64
}

const commandCols = `event_id, room_key, thread_root, room_id, body, session_id, button_event, created_at`

// PutCommand records a command, or updates the text of one that was edited.
// created_at is deliberately NOT refreshed: an edited command keeps its place
// in the order, so editing an old one cannot make it look like the newest.
func (s *Store) PutCommand(ctx context.Context, c *Command) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO commands (`+commandCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(event_id) DO UPDATE SET body = excluded.body, session_id = excluded.session_id`,
		c.EventID, c.RoomKey, c.ThreadRoot, c.RoomID, c.Body, c.SessionID, c.ButtonEvent, now())
	return err
}

// SetCommandButton records the delete button the bot put on a command.
func (s *Store) SetCommandButton(ctx context.Context, eventID, buttonEvent string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE commands SET button_event = ? WHERE event_id = ?`, buttonEvent, eventID)
	return err
}

func scanCommand(scan func(dest ...any) error) (*Command, error) {
	var c Command
	if err := scan(&c.EventID, &c.RoomKey, &c.ThreadRoot, &c.RoomID, &c.Body, &c.SessionID,
		&c.ButtonEvent, &c.CreatedAt); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) Command(ctx context.Context, eventID string) (*Command, error) {
	c, err := scanCommand(s.db.QueryRowContext(ctx,
		`SELECT `+commandCols+` FROM commands WHERE event_id = ?`, eventID).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return c, err
}

// LastCommand is the most recent command in a conversation slot.
func (s *Store) LastCommand(ctx context.Context, roomKey, threadRoot string) (*Command, error) {
	c, err := scanCommand(s.db.QueryRowContext(ctx,
		`SELECT `+commandCols+` FROM commands WHERE room_key = ? AND thread_root = ?
		  ORDER BY created_at DESC LIMIT 1`, roomKey, threadRoot).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return c, err
}

// Commands lists the commands in a conversation slot, newest first. since = 0
// means all of them.
func (s *Store) Commands(ctx context.Context, roomKey, threadRoot string, since int64) ([]*Command, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+commandCols+` FROM commands
		  WHERE room_key = ? AND thread_root = ? AND created_at >= ?
		  ORDER BY created_at DESC`, roomKey, threadRoot, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Command
	for rows.Next() {
		c, err := scanCommand(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) DeleteCommands(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	args := make([]any, len(eventIDs))
	holes := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		args[i], holes[i] = id, "?"
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM commands WHERE event_id IN (`+strings.Join(holes, ",")+`)`, args...)
	return err
}

// --- notices (the bridge's own messages) -----------------------------------

// Notice is one message the bridge posted about itself, with the command that
// caused it when there was one.
type Notice struct {
	EventID      string
	RoomKey      string
	ThreadRoot   string
	RoomID       string
	Ghost        string
	CommandEvent string
	CreatedAt    int64
}

func (s *Store) PutNotice(ctx context.Context, n *Notice) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO notices (event_id, room_key, thread_root, room_id, ghost, command_event, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(event_id) DO NOTHING`,
		n.EventID, n.RoomKey, n.ThreadRoot, n.RoomID, n.Ghost, n.CommandEvent, now())
	return err
}

const noticeCols = `event_id, room_key, thread_root, room_id, ghost, command_event, created_at`

func scanNotices(rows *sql.Rows) ([]*Notice, error) {
	defer rows.Close()
	var out []*Notice
	for rows.Next() {
		var n Notice
		if err := rows.Scan(&n.EventID, &n.RoomKey, &n.ThreadRoot, &n.RoomID, &n.Ghost,
			&n.CommandEvent, &n.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, &n)
	}
	return out, rows.Err()
}

// Notices returns the bridge's messages in one conversation slot, newest
// first. limit <= 0 means all of them.
func (s *Store) Notices(ctx context.Context, roomKey, threadRoot string, limit int) ([]*Notice, error) {
	query := `SELECT ` + noticeCols + ` FROM notices WHERE room_key = ? AND thread_root = ? ORDER BY created_at DESC`
	args := []any{roomKey, threadRoot}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return scanNotices(rows)
}

// NoticesSince returns the bridge's messages posted at or after a timestamp —
// what /clear all sweeps when a conversation is running.
func (s *Store) NoticesSince(ctx context.Context, roomKey, threadRoot string, since int64) ([]*Notice, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+noticeCols+` FROM notices
		  WHERE room_key = ? AND thread_root = ? AND created_at >= ?
		  ORDER BY created_at DESC`, roomKey, threadRoot, since)
	if err != nil {
		return nil, err
	}
	return scanNotices(rows)
}

// NoticesForCommand returns everything the bridge said in answer to one
// message of mine.
func (s *Store) NoticesForCommand(ctx context.Context, commandEvent string) ([]*Notice, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+noticeCols+` FROM notices WHERE command_event = ? ORDER BY created_at`, commandEvent)
	if err != nil {
		return nil, err
	}
	return scanNotices(rows)
}

func (s *Store) DeleteNotices(ctx context.Context, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	args := make([]any, len(eventIDs))
	holes := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		args[i], holes[i] = id, "?"
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM notices WHERE event_id IN (`+strings.Join(holes, ",")+`)`, args...)
	return err
}

// --- turns -----------------------------------------------------------------

// Turn links my message to the session and backend ids that answered it.
type Turn struct {
	EventID   string
	SessionID int64
	UserMsgID string
	ParentID  string
	// ReplyEvent is the first event of the answer: the one an edit replaces
	// and a reply points at.
	ReplyEvent string
	// AnswerEvents is EVERY room event the answer produced — the anchor, the
	// interim edits that grew it, the later parts of a split answer, the
	// attached file. /undo has to redact all of them: in Matrix an edit is its
	// own event, so redacting the anchor alone leaves the last edit behind and
	// the client happily goes on rendering it.
	AnswerEvents []string
	// Question is what was asked, kept so /retry can ask it again without
	// reading the room back from the homeserver.
	Question string
	// Usage is what the backend reported about the run (tokens, speed) as
	// compact JSON, for /status. Empty when the backend said nothing.
	Usage string
}

func (s *Store) PutTurn(ctx context.Context, t *Turn) error {
	events := ""
	if len(t.AnswerEvents) > 0 {
		raw, err := json.Marshal(t.AnswerEvents)
		if err != nil {
			return err
		}
		events = string(raw)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO turns (event_id, session_id, user_msg_id, parent_id, reply_event, answer_events, question, usage, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(event_id) DO UPDATE SET reply_event = excluded.reply_event,
		                                     answer_events = excluded.answer_events,
		                                     user_msg_id = excluded.user_msg_id,
		                                     question = excluded.question,
		                                     usage = excluded.usage`,
		t.EventID, t.SessionID, t.UserMsgID, t.ParentID, t.ReplyEvent, events, t.Question, t.Usage, now())
	return err
}

// turnCols and scanTurn keep the two readers in step.
const turnCols = `event_id, session_id, user_msg_id, parent_id, reply_event, answer_events, question, usage`

func scanTurn(scan func(dest ...any) error) (*Turn, error) {
	var t Turn
	var events string
	if err := scan(&t.EventID, &t.SessionID, &t.UserMsgID, &t.ParentID, &t.ReplyEvent, &events, &t.Question, &t.Usage); err != nil {
		return nil, err
	}
	if events != "" {
		// A turn whose event list cannot be read is not a broken turn; it just
		// falls back to the one event every turn has.
		_ = json.Unmarshal([]byte(events), &t.AnswerEvents)
	}
	if len(t.AnswerEvents) == 0 && t.ReplyEvent != "" {
		t.AnswerEvents = []string{t.ReplyEvent}
	}
	return &t, nil
}

func (s *Store) Turn(ctx context.Context, eventID string) (*Turn, error) {
	t, err := scanTurn(s.db.QueryRowContext(ctx,
		`SELECT `+turnCols+` FROM turns WHERE event_id = ?`, eventID).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// LastTurn is the most recent turn of the live conversation in a room (or
// thread), which is what /retry asks again and /undo takes back.
func (s *Store) LastTurn(ctx context.Context, roomKey, threadRoot string) (*Turn, error) {
	t, err := scanTurn(s.db.QueryRowContext(ctx,
		`SELECT `+prefixed(turnCols, "t.")+`
		   FROM turns t
		   JOIN sessions s ON s.id = t.session_id
		  WHERE s.room_key = ? AND s.thread_root = ? AND s.closed_at IS NULL
		  ORDER BY t.created_at DESC LIMIT 1`, roomKey, threadRoot).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// prefixed qualifies a column list for a join.
func prefixed(cols, prefix string) string {
	parts := strings.Split(cols, ", ")
	for i, col := range parts {
		parts[i] = prefix + col
	}
	return strings.Join(parts, ", ")
}

// DeleteTurn forgets one turn. /undo uses it after rewinding the session, so
// the turn it removed cannot be retried or counted again.
func (s *Store) DeleteTurn(ctx context.Context, eventID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM turns WHERE event_id = ?`, eventID)
	return err
}

// RoomSessions lists a room's conversations, newest first — what /history
// shows. The live one is included; it is usually the interesting one.
func (s *Store) RoomSessions(ctx context.Context, roomKey, threadRoot string, limit int) ([]*Session, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT s.id, s.room_key, s.thread_root, s.agent, s.conv_id, s.model, s.turns, s.last_active, s.created_at,
		        s.closed_at IS NULL
		   FROM sessions s
		  WHERE s.room_key = ? AND s.thread_root = ?
		  ORDER BY s.last_active DESC LIMIT ?`, roomKey, threadRoot, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Session
	for rows.Next() {
		var sess Session
		var live bool
		if err := rows.Scan(&sess.ID, &sess.RoomKey, &sess.ThreadRoot, &sess.Agent, &sess.ConvID,
			&sess.Model, &sess.Turns, &sess.LastActive, &sess.CreatedAt, &live); err != nil {
			return nil, err
		}
		sess.Live = live
		out = append(out, &sess)
	}
	return out, rows.Err()
}

// --- transactions ----------------------------------------------------------

// MarkTxn records a transaction id and reports whether it is new. Hungryserv
// retries transactions, and a retried announcement would be a second message.
func (s *Store) MarkTxn(ctx context.Context, txnID string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO txns (txn_id, created_at) VALUES (?, ?) ON CONFLICT(txn_id) DO NOTHING`, txnID, now())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// --- housekeeping ----------------------------------------------------------

// Cleanup drops history that is past its retention. Notifications are kept
// longer than deliveries because a reaction can arrive days after the message.
func (s *Store) Cleanup(ctx context.Context, notifyDays, deliveryDays int) error {
	cutoff := func(days int) int64 {
		return time.Now().Add(-time.Duration(days) * 24 * time.Hour).UnixMilli()
	}
	stmts := []struct {
		query string
		arg   int64
	}{
		{`DELETE FROM notifications WHERE created_at < ?`, cutoff(notifyDays)},
		{`DELETE FROM outbox WHERE status != 'pending' AND updated_at < ?`, cutoff(deliveryDays)},
		{`DELETE FROM txns WHERE created_at < ?`, cutoff(2)},
		{`DELETE FROM turns WHERE created_at < ?`, cutoff(notifyDays)},
		{`DELETE FROM polls WHERE closed_at IS NOT NULL AND closed_at < ?`, cutoff(notifyDays)},
	}
	for _, s2 := range stmts {
		if _, err := s.db.ExecContext(ctx, s2.query, s2.arg); err != nil {
			return err
		}
	}
	return nil
}
