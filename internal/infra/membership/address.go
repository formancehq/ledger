package membership

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
)

// maxHostnameLen is the longest DNS name in presentation form (RFC 1035),
// excluding an optional trailing dot.
const maxHostnameLen = 253

// maxLabelLen is the longest DNS label (RFC 1035).
const maxLabelLen = 63

// ValidatePeerAddress checks that a peer's Raft or service address is a
// dialable host:port before a learner registration is proposed. field names
// the request field in the error. A registration committed with an
// unparseable address leaves a member that no peer can ever reach.
//
// The host must be empty (the local host), an IP literal, or a syntactically
// valid hostname. The check is purely syntactic: it does not resolve names.
//
// This is an admission check only: committed ConfChanges are applied as-is.
func ValidatePeerAddress(field, addr string) error {
	if addr == "" {
		return fmt.Errorf("%s is required", field)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s %q must be host:port: %w", field, addr, err)
	}

	if !validPeerHost(host) {
		return fmt.Errorf("%s %q must be host:port: invalid host", field, addr)
	}

	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return fmt.Errorf("%s %q must be host:port: port must be a number between 1 and 65535", field, addr)
	}

	return nil
}

// validPeerHost reports whether the TCP dialer can use host without a
// syntax error. An empty host (":7777") dials the local host and is used by
// single-machine clusters.
func validPeerHost(host string) bool {
	if host == "" {
		return true
	}

	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}

	return validHostname(host)
}

// validHostname applies the same rules as the Go resolver's domain-name
// check: dot-separated labels of letters, digits, '-' and '_', each 1 to 63
// bytes, not starting or ending with '-', with at least one non-digit
// character. A single trailing dot is allowed. Control characters such as
// NUL, whitespace and every other byte are rejected.
func validHostname(host string) bool {
	name := host
	if name[len(name)-1] == '.' {
		name = name[:len(name)-1]
	}

	if name == "" || len(name) > maxHostnameLen {
		return false
	}

	nonNumeric := false
	labelLen := 0
	previous := byte('.')

	for i := range len(name) {
		c := name[i]

		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', c == '_':
			nonNumeric = true
			labelLen++
		case '0' <= c && c <= '9':
			labelLen++
		case c == '-':
			if previous == '.' {
				return false
			}

			nonNumeric = true
			labelLen++
		case c == '.':
			if previous == '.' || previous == '-' || labelLen > maxLabelLen {
				return false
			}

			labelLen = 0
		default:
			return false
		}

		previous = c
	}

	// previous == '.' means the final label is empty ("node.." once the
	// optional trailing dot is removed).
	return previous != '-' && previous != '.' && labelLen <= maxLabelLen && nonNumeric
}
