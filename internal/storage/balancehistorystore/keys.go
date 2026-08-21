package balancehistorystore

import (
	"encoding/binary"
	"errors"
	"slices"
)

const (
	prefixStoreState     byte = 0x00
	prefixLatestManifest byte = 0x01
	prefixManifest       byte = 0x02
	prefixRunData        byte = 0x10
	prefixRunMeta        byte = 0x11
	prefixRunCatalog     byte = 0x12
)

func quarantineKey() []byte {
	return []byte{prefixStoreState, 0x01}
}

type recordIdentity struct {
	Temporality    Temporality
	LedgerName     string
	Account        string
	AssetBase      string
	AssetPrecision uint8
	Color          string
}

func latestManifestKey() []byte {
	return []byte{prefixLatestManifest}
}

func manifestKey(version uint64) []byte {
	key := make([]byte, 9)
	key[0] = prefixManifest
	binary.BigEndian.PutUint64(key[1:], version)

	return key
}

func runMetaKey(runID uint64) []byte {
	key := make([]byte, 9)
	key[0] = prefixRunMeta
	binary.BigEndian.PutUint64(key[1:], runID)

	return key
}

func runPrefix(kind byte, runID uint64) []byte {
	key := make([]byte, 9)
	key[0] = kind
	binary.BigEndian.PutUint64(key[1:], runID)

	return key
}

func appendString(dst []byte, value string) ([]byte, error) {
	if len(value) > int(^uint16(0)) {
		return nil, errors.New("history key component exceeds 65535 bytes")
	}

	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
	dst = append(dst, value...)

	return dst, nil
}

// appendAccount encodes an account so the encoded bytes preserve raw-prefix
// relationships. NUL is escaped as 0x00,0xff and 0x00,0x00 terminates an exact
// account. Ordinary account addresses therefore have no per-byte overhead.
func appendAccount(dst []byte, value string, terminate bool) ([]byte, error) {
	if len(value) > int(^uint16(0)) {
		return nil, errors.New("history account key component exceeds 65535 bytes")
	}
	for index := range len(value) {
		if value[index] == 0 {
			dst = append(dst, 0, 0xff)
		} else {
			dst = append(dst, value[index])
		}
	}
	if terminate {
		dst = append(dst, 0, 0)
	}

	return dst, nil
}

func appendIdentity(dst []byte, identity recordIdentity) ([]byte, error) {
	dst = append(dst, byte(identity.Temporality))

	var err error
	dst, err = appendString(dst, identity.LedgerName)
	if err != nil {
		return nil, err
	}
	dst, err = appendAccount(dst, identity.Account, true)
	if err != nil {
		return nil, err
	}

	dst, err = appendString(dst, identity.AssetBase)
	if err != nil {
		return nil, err
	}
	dst = append(dst, identity.AssetPrecision)
	dst, err = appendString(dst, identity.Color)
	if err != nil {
		return nil, err
	}

	return dst, nil
}

func dataIdentityPrefix(runID uint64, identity recordIdentity) ([]byte, error) {
	return appendIdentity(runPrefix(prefixRunData, runID), identity)
}

func dataKey(runID uint64, identity recordIdentity, timestamp uint64) ([]byte, error) {
	key, err := dataIdentityPrefix(runID, identity)
	if err != nil {
		return nil, err
	}

	return binary.BigEndian.AppendUint64(key, timestamp), nil
}

func catalogKey(runID uint64, identity recordIdentity) ([]byte, error) {
	return appendIdentity(runPrefix(prefixRunCatalog, runID), identity)
}

func catalogPrefix(runID uint64, temporality Temporality, ledgerName string, account *string) ([]byte, error) {
	prefix := runPrefix(prefixRunCatalog, runID)
	prefix = append(prefix, byte(temporality))
	var err error
	prefix, err = appendString(prefix, ledgerName)
	if err != nil {
		return nil, err
	}

	if account != nil {
		return appendAccount(prefix, *account, true)
	}

	return prefix, nil
}

func catalogAccountPrefix(runID uint64, temporality Temporality, ledgerName, accountPrefix string) ([]byte, error) {
	prefix := runPrefix(prefixRunCatalog, runID)
	prefix = append(prefix, byte(temporality))
	var err error
	prefix, err = appendString(prefix, ledgerName)
	if err != nil {
		return nil, err
	}

	return appendAccount(prefix, accountPrefix, false)
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := range slices.Backward(end) {
		if end[i] != 0xff {
			end[i]++

			return end[:i+1]
		}
	}

	return nil
}

func readString(src []byte, offset *int) (string, error) {
	if len(src)-*offset < 2 {
		return "", errors.New("truncated history key string length")
	}

	length := int(binary.BigEndian.Uint16(src[*offset : *offset+2]))
	*offset += 2
	if len(src)-*offset < length {
		return "", errors.New("truncated history key string value")
	}

	value := string(src[*offset : *offset+length])
	*offset += length

	return value, nil
}

func readAccount(src []byte, offset *int) (string, error) {
	value := make([]byte, 0)
	for {
		if len(src)-*offset < 1 {
			return "", errors.New("truncated history account key")
		}
		current := src[*offset]
		*offset++
		if current != 0 {
			value = append(value, current)

			continue
		}
		if len(src)-*offset < 1 {
			return "", errors.New("truncated history account key escape")
		}
		escaped := src[*offset]
		*offset++
		if escaped == 0 {
			return string(value), nil
		}
		if escaped != 0xff {
			return "", errors.New("invalid history account key escape")
		}
		value = append(value, 0)
	}
}

func decodeCatalogKey(key []byte) (recordIdentity, error) {
	if len(key) < 14 || key[0] != prefixRunCatalog {
		return recordIdentity{}, errors.New("invalid history catalog key")
	}

	offset := 9
	identity := recordIdentity{
		Temporality: Temporality(key[offset]),
	}
	offset++

	var err error
	identity.LedgerName, err = readString(key, &offset)
	if err != nil {
		return recordIdentity{}, err
	}
	identity.Account, err = readAccount(key, &offset)
	if err != nil {
		return recordIdentity{}, err
	}

	identity.AssetBase, err = readString(key, &offset)
	if err != nil {
		return recordIdentity{}, err
	}
	if offset >= len(key) {
		return recordIdentity{}, errors.New("truncated history asset precision")
	}
	identity.AssetPrecision = key[offset]
	offset++
	identity.Color, err = readString(key, &offset)
	if err != nil {
		return recordIdentity{}, err
	}
	if offset != len(key) || !identity.Temporality.valid() || identity.LedgerName == "" || identity.Account == "" {
		return recordIdentity{}, errors.New("invalid trailing history catalog key bytes")
	}

	return identity, nil
}

func decodeDataKey(key []byte) (uint64, recordIdentity, uint64, error) {
	if len(key) < 22 || key[0] != prefixRunData {
		return 0, recordIdentity{}, 0, errors.New("invalid history data key")
	}

	runID := binary.BigEndian.Uint64(key[1:9])
	timestamp := binary.BigEndian.Uint64(key[len(key)-8:])
	catalog := append([]byte(nil), key[:len(key)-8]...)
	catalog[0] = prefixRunCatalog
	identity, err := decodeCatalogKey(catalog)
	if err != nil {
		return 0, recordIdentity{}, 0, err
	}

	return runID, identity, timestamp, nil
}
