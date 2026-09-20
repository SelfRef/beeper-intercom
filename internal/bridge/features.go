package bridge

import (
	"maunium.net/go/mautrix/event"

	"github.com/SelfRef/beeper-intercom/internal/config"
)

// com.beeper.room_features — what the client is told this room can do.
//
// Every real portal carries one, and without it the client falls back to its
// own defaults. The one that matters here is the LAST field: deleting a
// message normally leaves a tombstone ("This message has been deleted"), which
// for the bridge bot renders as a left-aligned bubble with a raw MXID in it —
// louder than the dim centred notice it replaced. `delete_hide_placeholder`
// makes a redaction disappear instead, which is what a bridge clearing up
// after itself wants. Rooms can ask for the tombstones back
// (`delete_placeholder: true`), because in a room full of announcements a
// silent disappearance can be worse than a marker.
//
// Everything else is declared honestly: this bridge renders the Matrix HTML
// subset, takes files and voice, and answers in threads.
func (b *Bridge) roomFeatures(room config.Room) *event.RoomFeatures {
	const full = event.CapLevelFullySupported
	feat := &event.RoomFeatures{
		Formatting: event.FormattingFeatureMap{
			event.FmtBold: full, event.FmtItalic: full, event.FmtUnderline: full,
			event.FmtStrikethrough: full, event.FmtInlineCode: full,
			event.FmtCodeBlock: full, event.FmtSyntaxHighlighting: full,
			event.FmtBlockquote: full, event.FmtInlineLink: full,
			event.FmtUserLink: full, event.FmtRoomLink: full, event.FmtEventLink: full,
			event.FmtUnorderedList: full, event.FmtOrderedList: full,
			event.FmtHorizontalLine: full, event.FmtHeaders: full,
			event.FmtSuperscript: full, event.FmtSubscript: full,
			event.FmtTable: full, event.FmtDetailsSummary: full,
			event.FmtSpoiler: full, event.FmtSpoilerReason: full,
			event.FmtTextForegroundColor: full, event.FmtTextBackgroundColor: full,
			event.FmtCustomEmoji: full,
			// @room is accepted but hungryserv never evaluates the rule, so an
			// urgent notification mentions the owner by name instead (§10.6).
			event.FmtAtRoomMention: event.CapLevelPartialSupport,
		},
		MaxTextLength: b.conf().Limits.MessageSplitBytes,

		Thread: full,
		Reply:  full,
		// The bridge edits its own messages while an answer grows; the user
		// editing one re-runs the turn.
		Edit: full,
		// Deleting is what this file is really about.
		Delete:     full,
		DeleteHide: !room.DeletePlaceholder,

		Reaction:             full,
		ReactionCount:        1,
		CustomEmojiReactions: true,

		ReadReceipts:        true,
		TypingNotifications: true,
		MarkAsUnread:        true,
		Archive:             true,

		Poll:                full,
		PollEnd:             full,
		PollMaxOptions:      20,
		PollOptionMaxLength: 256,

		File: event.FileFeatureMap{
			event.MsgImage: fileFeature(b.conf().Limits.MaxMediaBytes),
			event.MsgVideo: fileFeature(b.conf().Limits.MaxMediaBytes),
			event.MsgAudio: fileFeature(b.conf().Limits.MaxMediaBytes),
			event.MsgFile:  fileFeature(b.conf().Limits.MaxMediaBytes),
		},
	}
	// A broadcast room is not a conversation: nothing is sent into it, so
	// there is nothing to edit or take back.
	if room.Kind == config.KindBroadcast {
		feat.Edit = event.CapLevelUnsupported
		feat.Delete = event.CapLevelUnsupported
	}
	return feat
}

func fileFeature(maxSize int64) *event.FileFeatures {
	return &event.FileFeatures{
		MimeTypes:        map[string]event.CapabilitySupportLevel{"*/*": event.CapLevelFullySupported},
		Caption:          event.CapLevelFullySupported,
		MaxCaptionLength: 8000,
		MaxSize:          maxSize,
	}
}
