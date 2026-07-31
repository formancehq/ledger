package balancehistoryarchive

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

const archiveDestinationIdentityVersion = "balance-history-archive-destination-v1"

const archiveNamespace = "balance-history/nodes"

// objectNamespace is the single formatter/parser for owned balance-history
// objects. OwnerID is a stable node identity, so a replica can never enumerate
// or delete another replica's archive namespace.
type objectNamespace struct {
	prefix string
}

// ObjectBucketID returns the canonical owned logical bucket for ref. It is
// useful to adapters and black-box tests that need the exact ColdStorage
// coordinates without duplicating the namespace layout.
func (c Config) ObjectBucketID(ref Ref) (string, error) {
	namespace, err := newObjectNamespace(c.BaseBucketID, c.OwnerID)
	if err != nil {
		return "", err
	}

	return namespace.objectBucket(ref.SHA256), nil
}

func newObjectNamespace(baseBucketID, ownerID string) (objectNamespace, error) {
	baseBucketID = strings.Trim(baseBucketID, "/")
	if baseBucketID == "" {
		return objectNamespace{}, errors.New("base cold storage bucket id is required")
	}
	for segment := range strings.SplitSeq(baseBucketID, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.Contains(segment, `\`) {
			return objectNamespace{}, fmt.Errorf("invalid base cold storage bucket id %q", baseBucketID)
		}
	}
	if ownerID == "" {
		return objectNamespace{}, errors.New("balance history archive owner id is required")
	}
	if ownerID != strings.TrimSpace(ownerID) || ownerID == "." || ownerID == ".." || strings.ContainsAny(ownerID, `/\`) {
		return objectNamespace{}, fmt.Errorf("invalid balance history archive owner id %q", ownerID)
	}

	return objectNamespace{
		prefix: baseBucketID + "/" + archiveNamespace + "/" + ownerID + "/runs",
	}, nil
}

func (n objectNamespace) objectBucket(digest [32]byte) string {
	return n.prefix + "/" + hex.EncodeToString(digest[:])
}

func (n objectNamespace) destinationIdentity(physicalIdentity string) string {
	digest := sha256.Sum256([]byte(archiveDestinationIdentityVersion + "\x00" + physicalIdentity + "\x00" + n.prefix))

	return archiveDestinationIdentityVersion + ":" + hex.EncodeToString(digest[:])
}

func (n objectNamespace) parse(object coldstorage.ArchiveObject) ([32]byte, bool) {
	if object.ChapterID != archiveChapterID {
		return [32]byte{}, false
	}
	hexed, ok := strings.CutPrefix(object.BucketID, n.prefix+"/")
	if !ok || len(hexed) != hex.EncodedLen(32) || strings.Contains(hexed, "/") {
		return [32]byte{}, false
	}
	decoded, err := hex.DecodeString(hexed)
	if err != nil || hex.EncodeToString(decoded) != hexed {
		return [32]byte{}, false
	}

	var digest [32]byte
	copy(digest[:], decoded)

	return digest, true
}
