package oracle

import (
	"fmt"
	"strings"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// The bucket-global chapter lifecycle: the accounting periods the audit chain is
// cut along, and the only part of the model the server advances without being
// asked.
//
// Two of the four transitions carry no request. The Sealer seals a CLOSING
// chapter once it has folded the audit up to the close boundary, and the Archiver
// proposes ConfirmArchiveChapter once the chapter's cold upload verifies. Every
// other transition needs an order, so no chapter is ever more than one
// unrequested step ahead of the model — which is what lets a prediction that
// depends on a pending step admit exactly two answers instead of a search.

// ChapterStatus is the lifecycle chain. The order is significant: every
// transition moves forward along it.
type ChapterStatus uint8

const (
	ChapterOpen ChapterStatus = iota
	ChapterClosing
	ChapterClosed
	ChapterArchiving
	ChapterArchived
)

func (s ChapterStatus) String() string {
	switch s {
	case ChapterOpen:
		return "OPEN"
	case ChapterClosing:
		return "CLOSING"
	case ChapterClosed:
		return "CLOSED"
	case ChapterArchiving:
		return "ARCHIVING"
	case ChapterArchived:
		return "ARCHIVED"
	default:
		return fmt.Sprintf("ChapterStatus(%d)", uint8(s))
	}
}

// ChapterStatusFromProto maps a status the server reported onto the model's
// chain. ok is false for one the model does not represent — the caller must
// surface that rather than approximate it.
func ChapterStatusFromProto(s commonpb.ChapterStatus) (ChapterStatus, bool) {
	switch s {
	case commonpb.ChapterStatus_CHAPTER_OPEN:
		return ChapterOpen, true
	case commonpb.ChapterStatus_CHAPTER_CLOSING:
		return ChapterClosing, true
	case commonpb.ChapterStatus_CHAPTER_CLOSED:
		return ChapterClosed, true
	case commonpb.ChapterStatus_CHAPTER_ARCHIVING:
		return ChapterArchiving, true
	case commonpb.ChapterStatus_CHAPTER_ARCHIVED:
		return ChapterArchived, true
	default:
		return 0, false
	}
}

// AutonomousNext returns the status a chapter reaches with no request behind it,
// and whether such a step exists at all: the Sealer seals a CLOSING chapter, the
// Archiver confirms an ARCHIVING one.
func AutonomousNext(s ChapterStatus) (ChapterStatus, bool) {
	switch s {
	case ChapterClosing:
		return ChapterClosed, true
	case ChapterArchiving:
		return ChapterArchived, true
	default:
		return s, false
	}
}

// Chapters is the chapter registry in normal form:
//
//	1 .. archivedThrough    ARCHIVED
//	archivedThrough+1       ARCHIVING          (only when archiving)
//	sealed[i]               CLOSED | CLOSING   (i counts up from firstUnsealedID)
//	the highest chapter     OPEN               (only when hasOpen)
//
// Ids are positional, so a hole in the registry, an archived chapter above an
// unarchived one, and an ARCHIVING chapter anywhere but the prefix successor have
// no encoding at all. sealed holds exactly the chapters that are CLOSED or
// CLOSING: one leaves the list the moment it starts archiving.
//
// It is bucket-global, not per-ledger — chapters cut across every ledger in the
// bucket.
type Chapters struct {
	archivedThrough uint64
	archiving       bool
	sealed          List[bool]
	hasOpen         bool
}

// NewChapters returns the registry of a bucket no proposal has reached yet: no
// chapter exists until the first one bootstraps chapter 1.
func NewChapters() Chapters {
	return Chapters{sealed: NewList(sealedTerm)}
}

func sealedTerm(i int, isSealed bool) Digest {
	t := newTerm("CHS")
	t.u64(uint64(i))
	t.boolean(isSealed)

	return t.sum()
}

// ChaptersFrom builds a registry from a status per chapter id, as ListChapters
// reports them. It fails when the observed set is not in normal form — a gap in
// the ids, an unarchived chapter below an archived one, more than one ARCHIVING,
// an ARCHIVING chapter off the prefix successor, or an OPEN chapter that is not
// the highest. Each of those is a lifecycle violation on the server's side, so
// the error is a finding for the caller to report, not a shape to accommodate.
func ChaptersFrom(observed map[uint64]ChapterStatus) (Chapters, error) {
	c := NewChapters()

	for id := uint64(1); id <= uint64(len(observed)); id++ {
		status, ok := observed[id]
		if !ok {
			return c, fmt.Errorf("chapter %d missing from a registry of %d chapters — ids must be gap-free", id, len(observed))
		}

		last := id == uint64(len(observed))

		switch status {
		case ChapterArchived:
			if c.archiving || c.sealed.Len() > 0 {
				return c, fmt.Errorf("chapter %d is ARCHIVED above an unarchived chapter", id)
			}
			c.archivedThrough = id
		case ChapterArchiving:
			if c.archiving || c.sealed.Len() > 0 {
				return c, fmt.Errorf("chapter %d is ARCHIVING but is not the archived prefix's successor", id)
			}
			c.archiving = true
		case ChapterClosed, ChapterClosing:
			c.sealed = c.sealed.Append(status == ChapterClosed)
		case ChapterOpen:
			if !last {
				return c, fmt.Errorf("chapter %d is OPEN below chapter %d", id, len(observed))
			}
			c.hasOpen = true
		default:
			return c, fmt.Errorf("chapter %d has unrepresentable status %s", id, status)
		}
	}

	return c, nil
}

// firstUnsealedID is the id of sealed[0].
func (c Chapters) firstUnsealedID() uint64 {
	id := c.archivedThrough + 1
	if c.archiving {
		id++
	}

	return id
}

// ArchivedThrough is the archived prefix: every chapter from 1 to it is
// ARCHIVED. 0 means none is. It mirrors the server's archivedThroughID, the gate
// ArchiveChapter and ConfirmArchiveChapter are both measured against.
func (c Chapters) ArchivedThrough() uint64 { return c.archivedThrough }

// Archiving reports whether the archived prefix's successor is ARCHIVING — i.e.
// whether a confirm is outstanding on the server side.
func (c Chapters) Archiving() bool { return c.archiving }

// OpenID is the chapter accepting transactions, 0 before chapter 1 bootstraps.
// The server's nextChapterID is OpenID+1 and is not stored here: only
// CloseChapter allocates, and it always assigns the successor.
func (c Chapters) OpenID() uint64 {
	if !c.hasOpen {
		return 0
	}

	return c.firstUnsealedID() + uint64(c.sealed.Len())
}

// LastID is the highest chapter that has ever existed, 0 when none has.
func (c Chapters) LastID() uint64 {
	last := c.firstUnsealedID() - 1 + uint64(c.sealed.Len())
	if c.hasOpen {
		last++
	}

	return last
}

// StatusOf resolves an id against the normal form. ok is false for an id that
// never existed. An archived chapter resolves the same whether or not its rows
// are still resident — the model tracks the durable registry ListChapters reads,
// which the purge never touches.
func (c Chapters) StatusOf(id uint64) (ChapterStatus, bool) {
	switch {
	case id == 0 || id > c.LastID():
		return 0, false
	case id <= c.archivedThrough:
		return ChapterArchived, true
	case c.archiving && id == c.archivedThrough+1:
		return ChapterArchiving, true
	case c.hasOpen && id == c.OpenID():
		return ChapterOpen, true
	case c.sealed.Get(int(id - c.firstUnsealedID())):
		return ChapterClosed, true
	default:
		return ChapterClosing, true
	}
}

// Fingerprint is the registry's identity, folded into the state's dedup key. The
// sealed list's terms are index-keyed, so {3 CLOSED, 4 CLOSING} and {3 CLOSING,
// 4 CLOSED} stay distinct — they predict different reasons for the same archive
// request and must not collapse.
func (c Chapters) Fingerprint() Digest {
	t := newTerm("CH")
	t.u64(c.archivedThrough)
	t.boolean(c.archiving)
	t.boolean(c.hasOpen)

	return t.sum().add(c.sealed.Fingerprint())
}

// WithSealed applies the Sealer's transition to a CLOSING chapter, reporting
// false when that chapter is not CLOSING. It is not an order: callers apply it to
// hypothesise that a pending seal has already landed.
func (c Chapters) WithSealed(id uint64) (Chapters, bool) {
	if status, ok := c.StatusOf(id); !ok || status != ChapterClosing {
		return c, false
	}

	c.sealed = c.sealed.Set(int(id-c.firstUnsealedID()), true)

	return c, true
}

// WithConfirmed applies the Archiver's transition: the archived prefix extends
// over the ARCHIVING chapter and its hot data is purged. False when nothing is
// archiving. Extending across a gap has no encoding, so it cannot be expressed.
func (c Chapters) WithConfirmed() (Chapters, bool) {
	if !c.archiving {
		return c, false
	}

	c.archivedThrough++
	c.archiving = false

	return c, true
}

// applyClose predicts CloseChapter: the open chapter becomes CLOSING and its
// successor opens. The successor needs no entry — it is the implied open one.
// WithClosed is the registry after a CloseChapter the server accepted, and
// whether acceptance was possible. A move for reachability folds: the same
// transition Apply performs, without an order's response semantics.
func (c Chapters) WithClosed() (Chapters, bool) {
	next, res := c.applyClose()

	return next, res.OK
}

// WithArchived is the registry after an ArchiveChapter(id) the server accepted,
// and whether acceptance was possible.
func (c Chapters) WithArchived(id uint64) (Chapters, bool) {
	next, res := c.applyArchive(id)

	return next, res.OK
}

func (c Chapters) applyClose() (Chapters, OrderResult) {
	if !c.hasOpen {
		return c, OrderResult{Reason: domain.ErrReasonNoChapterOpen}
	}

	c.sealed = c.sealed.Append(false)

	return c, OrderResult{OK: true}
}

// applyArchive predicts ArchiveChapter. The order of the checks is observable
// contract, not an implementation detail: the prefix gate runs before the
// residency lookup, so a re-archive of an archived chapter reports
// CHAPTER_ALREADY_ARCHIVED rather than the CHAPTER_NOT_FOUND a residency-first
// order would report on a node that has purged it.
func (c Chapters) applyArchive(id uint64) (Chapters, OrderResult) {
	// Inside the prefix the order is already carried out; past the successor it is
	// waiting on another chapter. Two situations, two reasons.
	if id <= c.archivedThrough {
		return c, OrderResult{Reason: domain.ErrReasonChapterAlreadyArchived}
	}

	status, ok := c.StatusOf(id)
	if !ok {
		return c, OrderResult{Reason: domain.ErrReasonChapterNotFound}
	}
	if status != ChapterClosed {
		return c, OrderResult{Reason: domain.ErrReasonChapterNotClosed}
	}
	if id != c.archivedThrough+1 {
		return c, OrderResult{Reason: domain.ErrReasonChapterArchiveOutOfOrder}
	}

	// id is the prefix successor and CLOSED, so it is sealed[0] and nothing is
	// archiving — an ARCHIVING successor would have failed the status check.
	c.archiving = true
	c.sealed = c.sealed.PopFront()

	return c, OrderResult{OK: true}
}

// String renders the registry for a finding's details: the archived prefix, then
// one letter per live chapter.
func (c Chapters) String() string {
	out := fmt.Sprintf("archived<=%d", c.archivedThrough)
	if c.archiving {
		out += fmt.Sprintf(" %d:ARCHIVING", c.archivedThrough+1)
	}

	var outSb312 strings.Builder
	for i, isSealed := range c.sealed.All() {
		status := ChapterClosing
		if isSealed {
			status = ChapterClosed
		}
		fmt.Fprintf(&outSb312, " %d:%s", c.firstUnsealedID()+uint64(i), status)
	}
	out += outSb312.String()

	if c.hasOpen {
		out += fmt.Sprintf(" %d:OPEN", c.OpenID())
	}

	return out
}
