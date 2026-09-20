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

-- What the bridge sent in answer to which of my messages, so an edit can
-- regenerate and a redaction can cancel.
CREATE TABLE IF NOT EXISTS turns (
	event_id     TEXT PRIMARY KEY,
	session_id   INTEGER NOT NULL,
	user_msg_id  TEXT NOT NULL DEFAULT '',
	parent_id    TEXT NOT NULL DEFAULT '',
	reply_event  TEXT NOT NULL DEFAULT '',
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

func (s *Store) TouchSession(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET last_active = ? WHERE id = ?`, now(), id)
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

// --- turns -----------------------------------------------------------------

// Turn links my message to the session and backend ids that answered it.
type Turn struct {
	EventID    string
	SessionID  int64
	UserMsgID  string
	ParentID   string
	ReplyEvent string
}

func (s *Store) PutTurn(ctx context.Context, t *Turn) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO turns (event_id, session_id, user_msg_id, parent_id, reply_event, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(event_id) DO UPDATE SET reply_event = excluded.reply_event`,
		t.EventID, t.SessionID, t.UserMsgID, t.ParentID, t.ReplyEvent, now())
	return err
}

func (s *Store) Turn(ctx context.Context, eventID string) (*Turn, error) {
	var t Turn
	err := s.db.QueryRowContext(ctx,
		`SELECT event_id, session_id, user_msg_id, parent_id, reply_event FROM turns WHERE event_id = ?`, eventID).
		Scan(&t.EventID, &t.SessionID, &t.UserMsgID, &t.ParentID, &t.ReplyEvent)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &t, err
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
