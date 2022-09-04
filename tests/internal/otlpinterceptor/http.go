package otlpinterceptor

func HTTPStandardAttributes(method, target, route string) map[string]any {
	return map[string]any{
		"net.transport": "ip_tcp",
		//"net.peer.ip":      "127.0.0.1",
		"net.host.name": "localhost",
		//"net.host.port":    int64(3068),
		"http.method":      method,
		"http.target":      target,
		"http.server_name": "ledger",
		"http.route":       route,
		"http.user_agent":  "OpenAPI-Generator/latest/go",
		"http.scheme":      "http",
		//"http.host":        "localhost:3068",
		"http.flavor":      "1.1",
		"http.status_code": int64(200),
	}
}
