package membership

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"unicode"
)

// ValidatePeerAddress checks that a peer's Raft or service address is a
// dialable host:port before a learner registration is proposed. field names
// the request field in the error. A registration committed with an
// unparseable address leaves a member that no peer can ever reach.
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

	if host == "" || strings.IndexFunc(host, unicode.IsSpace) >= 0 {
		return fmt.Errorf("%s %q must be host:port: invalid host", field, addr)
	}

	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return fmt.Errorf("%s %q must be host:port: port must be a number between 1 and 65535", field, addr)
	}

	return nil
}
