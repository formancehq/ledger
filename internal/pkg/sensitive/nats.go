package sensitive

import "strings"

func redactNATS(raw string) string {
	// NATS lists contain independent URLs, unlike ClickHouse multi-host DSNs.
	servers := strings.Split(raw, ",")
	for i, server := range servers {
		result, ok := redactConnectionURL(server, urlPolicy{
			schemes: []string{"nats", "tls", "ws", "wss"}, maskBareUserinfo: true,
		}, 0)
		if !ok {
			result = Marker
		}
		servers[i] = result
	}

	return strings.Join(servers, ",")
}
