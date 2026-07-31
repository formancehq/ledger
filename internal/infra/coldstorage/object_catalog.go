package coldstorage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ArchiveObject describes one chapter archive object discovered in cold
// storage. BucketID and ChapterID are the same logical coordinates accepted by
// ColdStorage; Size and LastModified support bounded capacity accounting.
type ArchiveObject struct {
	BucketID     string
	ChapterID    uint64
	Size         int64
	LastModified time.Time
}

// ArchiveObjectPage is one deterministic, bounded listing page. NextCursor is
// opaque to callers and empty when the scan is complete.
type ArchiveObjectPage struct {
	Objects    []ArchiveObject
	NextCursor string
}

// ObjectCatalog is the optional lifecycle companion to ColdStorage. Keeping it
// separate prevents read/write-only cold storage adapters and their mocks from
// gaining destructive capabilities implicitly.
//
// List returns archive data objects below bucketPrefix, including objects whose
// checksum commit marker is absent. Delete is idempotent and removes only the
// exact chapter object identified by bucketID and chapterID.
type ObjectCatalog interface {
	List(
		ctx context.Context,
		bucketPrefix string,
		cursor string,
		limit int,
	) (ArchiveObjectPage, error)
	Delete(ctx context.Context, bucketID string, chapterID uint64) error
}

func archiveObjectKey(bucketID string, chapterID uint64) string {
	return strings.Trim(bucketID, "/") + "/chapters/" + strconv.FormatUint(chapterID, 10) + "/" + archiveDataName
}

func parseArchiveObjectKey(key string) (ArchiveObject, bool) {
	key = strings.TrimPrefix(key, "/")
	suffix := "/" + archiveDataName
	if !strings.HasSuffix(key, suffix) {
		return ArchiveObject{}, false
	}

	withoutFile := strings.TrimSuffix(key, suffix)
	separator := strings.LastIndex(withoutFile, "/chapters/")
	if separator <= 0 {
		return ArchiveObject{}, false
	}
	bucketID := withoutFile[:separator]
	chapterText := withoutFile[separator+len("/chapters/"):]
	if chapterText == "" || strings.Contains(chapterText, "/") {
		return ArchiveObject{}, false
	}
	chapterID, err := strconv.ParseUint(chapterText, 10, 64)
	if err != nil || strconv.FormatUint(chapterID, 10) != chapterText {
		return ArchiveObject{}, false
	}
	if normalized, err := normalizeBucketPrefix(bucketID); err != nil || normalized != bucketID {
		return ArchiveObject{}, false
	}

	return ArchiveObject{BucketID: bucketID, ChapterID: chapterID}, true
}

func normalizeBucketPrefix(prefix string) (string, error) {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "", errors.New("cold storage bucket prefix is required")
	}
	for segment := range strings.SplitSeq(prefix, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.Contains(segment, `\`) {
			return "", fmt.Errorf("invalid cold storage bucket prefix %q", prefix)
		}
	}

	return prefix, nil
}

func bucketWithinPrefix(bucketID, prefix string) bool {
	return bucketID == prefix || strings.HasPrefix(bucketID, prefix+"/")
}
