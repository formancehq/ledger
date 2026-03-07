package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type SnapshotResult struct {
	Report string
}

type BenchmarkReport struct {
	TestRun   string        `json:"testRun"`
	Namespace string        `json:"namespace"`
	TimeRange ReportTime    `json:"timeRange"`
	Entries   []ReportEntry `json:"entries"`
}

type ReportTime struct {
	From   string `json:"from"`
	To     string `json:"to"`
	FromMs int64  `json:"fromMs"`
	ToMs   int64  `json:"toMs"`
}

type ReportEntry struct {
	Dashboard   string `json:"dashboard"`
	Variant     string `json:"variant"`
	SnapshotURL string `json:"snapshotUrl"`
	DeleteURL   string `json:"deleteUrl"`
	DeleteKey   string `json:"deleteKey"`
	SnapshotKey string `json:"snapshotKey"`
	LiveURL     string `json:"liveUrl"`
}

type GrafanaClient struct {
	cfg    Config
	client *http.Client
}

func NewGrafanaClient(cfg Config) *GrafanaClient {
	return &GrafanaClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (g *GrafanaClient) ProcessTestRun(ctx context.Context, obj *unstructured.Unstructured) (SnapshotResult, error) {
	name := obj.GetName()
	namespace := obj.GetNamespace()

	startTime := obj.GetCreationTimestamp().Time
	endTime := getCompletionTime(obj)

	from := startTime.Add(-30 * time.Second)
	to := endTime.Add(30 * time.Second)

	dashboards, err := g.fetchDashboards(ctx)
	if err != nil {
		return SnapshotResult{}, err
	}

	if len(dashboards) == 0 {
		return SnapshotResult{}, errors.New("no dashboards found")
	}

	log.Printf("found %d dashboards for snapshot processing", len(dashboards))

	datasourceUID, err := g.resolveDatasourceUID(ctx)
	if err != nil {
		log.Printf("warning: failed to resolve datasource uid: %v", err)
	}

	snapshotTime := time.Now().UTC().Format("20060102-150405")
	prefix := g.cfg.SnapshotNamePrefix
	if prefix == "" {
		prefix = "k6-benchmark"
	}

	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	report := BenchmarkReport{
		TestRun:   name,
		Namespace: namespace,
		TimeRange: ReportTime{
			From:   startTime.Format(time.RFC3339),
			To:     endTime.Format(time.RFC3339),
			FromMs: fromMs,
			ToMs:   toMs,
		},
		Entries: make([]ReportEntry, 0),
	}

	for _, dash := range dashboards {
		dashboard, title, err := g.fetchDashboard(ctx, dash.UID)
		if err != nil {
			log.Printf("failed to fetch dashboard %s: %v", dash.UID, err)

			continue
		}

		setDashboardTime(dashboard, from, to)

		nodeValues := []string{}
		if datasourceUID != "" {
			values, err := g.fetchNodeValues(ctx, datasourceUID)
			if err != nil {
				log.Printf("warning: failed to fetch node values: %v", err)
			} else {
				nodeValues = values
			}
		}
		if len(nodeValues) > 0 {
			log.Printf("node values detected: %s", strings.Join(nodeValues, ","))
		}

		if g.cfg.SnapshotPerNode && len(nodeValues) > 0 {
			for _, node := range nodeValues {
				variantDashboard := cloneMap(dashboard)
				updateNodeVariable(variantDashboard, nodeValues, node)
				snapshotName := fmt.Sprintf("%s-%s-%s-node-%s-%s", prefix, name, sanitize(title), node, snapshotTime)
				snapshot, err := g.createSnapshot(ctx, snapshotName, variantDashboard)
				if err != nil {
					log.Printf("failed to create snapshot for node %s: %v", node, err)

					continue
				}
				liveURL := g.liveDashboardURL(dash.UID, fromMs, toMs, node)
				report.Entries = append(report.Entries, ReportEntry{
					Dashboard:   title,
					Variant:     "node " + node,
					SnapshotURL: snapshot.URL,
					DeleteURL:   snapshot.DeleteURL,
					DeleteKey:   snapshot.DeleteKey,
					SnapshotKey: snapshot.Key,
					LiveURL:     liveURL,
				})
			}

			continue
		}

		if len(nodeValues) > 0 {
			updateNodeVariable(dashboard, nodeValues, "")
		}
		snapshotName := fmt.Sprintf("%s-%s-%s-%s", prefix, name, sanitize(title), snapshotTime)
		snapshot, err := g.createSnapshot(ctx, snapshotName, dashboard)
		if err != nil {
			log.Printf("failed to create snapshot for %s: %v", dash.UID, err)

			continue
		}
		liveURL := g.liveDashboardURL(dash.UID, fromMs, toMs, "")
		report.Entries = append(report.Entries, ReportEntry{
			Dashboard:   title,
			Variant:     "all",
			SnapshotURL: snapshot.URL,
			DeleteURL:   snapshot.DeleteURL,
			DeleteKey:   snapshot.DeleteKey,
			SnapshotKey: snapshot.Key,
			LiveURL:     liveURL,
		})
	}

	sort.Slice(report.Entries, func(i, j int) bool {
		if report.Entries[i].Dashboard == report.Entries[j].Dashboard {
			return report.Entries[i].Variant < report.Entries[j].Variant
		}

		return report.Entries[i].Dashboard < report.Entries[j].Dashboard
	})

	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return SnapshotResult{}, err
	}

	return SnapshotResult{Report: string(payload) + "\n"}, nil
}

type dashboardInfo struct {
	UID   string `json:"uid"`
	Title string `json:"title"`
}

func (g *GrafanaClient) fetchDashboards(ctx context.Context) ([]dashboardInfo, error) {
	resp, err := g.doRequest(ctx, http.MethodGet, "/api/search?type=dash-db", nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("grafana search failed: %s", resp.Status)
	}

	var list []dashboardInfo
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return nil, err
	}

	return list, nil
}

func (g *GrafanaClient) fetchDashboard(ctx context.Context, uid string) (map[string]any, string, error) {
	resp, err := g.doRequest(ctx, http.MethodGet, "/api/dashboards/uid/"+uid, nil)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("grafana dashboard fetch failed: %s", resp.Status)
	}

	var payload struct {
		Dashboard map[string]any `json:"dashboard"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, "", err
	}

	if payload.Dashboard == nil {
		return nil, "", errors.New("dashboard payload missing")
	}

	title, _ := payload.Dashboard["title"].(string)
	if title == "" {
		title = uid
	}

	return payload.Dashboard, title, nil
}

func (g *GrafanaClient) resolveDatasourceUID(ctx context.Context) (string, error) {
	resp, err := g.doRequest(ctx, http.MethodGet, "/api/datasources", nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("grafana datasources fetch failed: %s", resp.Status)
	}

	var list []struct {
		UID  string `json:"uid"`
		Name string `json:"name"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		return "", err
	}

	for _, item := range list {
		if item.Name == g.cfg.DatasourceName {
			return item.UID, nil
		}
	}

	return "", fmt.Errorf("datasource %q not found", g.cfg.DatasourceName)
}

func (g *GrafanaClient) fetchNodeValues(ctx context.Context, datasourceUID string) ([]string, error) {
	match := url.QueryEscape(fmt.Sprintf("{__name__=\"%s\"}", g.cfg.NodeMetric))
	path := fmt.Sprintf("/api/datasources/proxy/uid/%s/api/v1/series?match[]=%s", datasourceUID, match)
	resp, err := g.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("series query failed: %s", resp.Status)
	}

	var payload struct {
		Data []map[string]string `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	values := make([]string, 0, len(payload.Data))
	for _, series := range payload.Data {
		if value := strings.TrimSpace(series[g.cfg.NodeLabel]); value != "" {
			values = append(values, value)
		}
	}

	if len(values) == 0 {
		return g.fetchNodeValuesViaQuery(ctx, datasourceUID)
	}

	sort.Strings(values)

	return unique(values), nil
}

func (g *GrafanaClient) fetchNodeValuesViaQuery(ctx context.Context, datasourceUID string) ([]string, error) {
	query := url.QueryEscape(g.cfg.NodeMetric)
	path := fmt.Sprintf("/api/datasources/proxy/uid/%s/api/v1/query?query=%s", datasourceUID, query)
	resp, err := g.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed: %s", resp.Status)
	}

	var payload struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	values := make([]string, 0, len(payload.Data.Result))
	for _, res := range payload.Data.Result {
		if value := strings.TrimSpace(res.Metric[g.cfg.NodeLabel]); value != "" {
			values = append(values, value)
		}
	}

	if len(values) == 0 {
		return nil, nil
	}

	sort.Strings(values)

	return unique(values), nil
}

type SnapshotInfo struct {
	URL       string
	DeleteURL string
	DeleteKey string
	Key       string
}

func (g *GrafanaClient) createSnapshot(ctx context.Context, name string, dashboard map[string]any) (SnapshotInfo, error) {
	payload := map[string]any{
		"dashboard": dashboard,
		"name":      name,
		"expires":   0,
		"external":  false,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return SnapshotInfo{}, err
	}

	resp, err := g.doRequest(ctx, http.MethodPost, "/api/snapshots", body)
	if err != nil {
		return SnapshotInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return SnapshotInfo{}, fmt.Errorf("snapshot create failed: %s", resp.Status)
	}

	var result struct {
		URL       string `json:"url"`
		DeleteURL string `json:"deleteUrl"`
		Key       string `json:"key"`
		DeleteKey string `json:"deleteKey"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return SnapshotInfo{}, err
	}

	if result.URL != "" {
		if strings.HasPrefix(result.URL, "http") {
			return SnapshotInfo{
				URL:       result.URL,
				DeleteURL: g.resolveURL(result.DeleteURL),
				DeleteKey: result.DeleteKey,
				Key:       result.Key,
			}, nil
		}

		return SnapshotInfo{
			URL:       strings.TrimRight(g.cfg.GrafanaURL, "/") + result.URL,
			DeleteURL: g.resolveURL(result.DeleteURL),
			DeleteKey: result.DeleteKey,
			Key:       result.Key,
		}, nil
	}

	if result.DeleteURL != "" {
		return SnapshotInfo{
			URL:       g.resolveURL(result.DeleteURL),
			DeleteURL: g.resolveURL(result.DeleteURL),
			DeleteKey: result.DeleteKey,
			Key:       result.Key,
		}, nil
	}

	if result.Key != "" {
		return SnapshotInfo{
			URL:       fmt.Sprintf("%s/dashboard/snapshot/%s", strings.TrimRight(g.cfg.GrafanaURL, "/"), result.Key),
			DeleteURL: g.resolveURL("/api/snapshots-delete/" + result.Key),
			DeleteKey: result.DeleteKey,
			Key:       result.Key,
		}, nil
	}

	return SnapshotInfo{}, errors.New("snapshot response missing URL")
}

func (g *GrafanaClient) DeleteSnapshot(ctx context.Context, deleteURL, deleteKey, snapshotKey string) error {
	candidates := []string{}
	if strings.TrimSpace(deleteURL) != "" {
		candidates = append(candidates, g.resolveURL(deleteURL))
	}
	if deleteKey != "" {
		candidates = append(candidates, g.resolveURL("/api/snapshots-delete/"+deleteKey))
	}
	if snapshotKey != "" {
		candidates = append(candidates, g.resolveURL("/api/snapshots/"+snapshotKey))
	}

	var lastErr error
	for _, target := range candidates {
		if target == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, target, nil)
		if err != nil {
			lastErr = err

			continue
		}
		if g.cfg.GrafanaUser != "" || g.cfg.GrafanaPassword != "" {
			req.SetBasicAuth(g.cfg.GrafanaUser, g.cfg.GrafanaPassword)
		}

		resp, err := g.client.Do(req)
		if err != nil {
			lastErr = err

			continue
		}
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent {
			return nil
		}

		lastErr = fmt.Errorf("snapshot delete failed: %s", resp.Status)
	}

	if lastErr == nil {
		return nil
	}

	return lastErr
}

func (g *GrafanaClient) liveDashboardURL(uid string, fromMs, toMs int64, nodeValue string) string {
	base := strings.TrimRight(g.cfg.GrafanaURL, "/")
	path := fmt.Sprintf("%s/d/%s", base, uid)

	params := url.Values{}
	if fromMs > 0 {
		params.Set("from", strconv.FormatInt(fromMs, 10))
	}
	if toMs > 0 {
		params.Set("to", strconv.FormatInt(toMs, 10))
	}
	if nodeValue != "" {
		params.Set("var-node", nodeValue)
	}

	if encoded := params.Encode(); encoded != "" {
		return path + "?" + encoded
	}

	return path
}

func (g *GrafanaClient) resolveURL(path string) string {
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "http") {
		return path
	}

	return strings.TrimRight(g.cfg.GrafanaURL, "/") + path
}

func (g *GrafanaClient) doRequest(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	base := strings.TrimRight(g.cfg.GrafanaURL, "/")
	url := base + path

	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, err
	}

	if g.cfg.GrafanaUser != "" || g.cfg.GrafanaPassword != "" {
		req.SetBasicAuth(g.cfg.GrafanaUser, g.cfg.GrafanaPassword)
	}

	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	return g.client.Do(req)
}

func getCompletionTime(obj *unstructured.Unstructured) time.Time {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err == nil && found {
		for _, raw := range conditions {
			condition, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if getString(condition, "type") != "TestRunRunning" {
				continue
			}
			if getString(condition, "status") != "False" {
				continue
			}
			if timestamp := getString(condition, "lastTransitionTime"); timestamp != "" {
				if parsed, err := time.Parse(time.RFC3339, timestamp); err == nil {
					return parsed
				}
			}
		}
	}

	return time.Now().UTC()
}

func setDashboardTime(dashboard map[string]any, from, to time.Time) {
	dashboard["time"] = map[string]any{
		"from": from.Format(time.RFC3339),
		"to":   to.Format(time.RFC3339),
	}
}

func updateNodeVariable(dashboard map[string]any, nodes []string, selected string) {
	if len(nodes) == 0 {
		return
	}

	templatingRaw, ok := dashboard["templating"].(map[string]any)
	if !ok {
		return
	}

	listRaw, ok := templatingRaw["list"].([]any)
	if !ok {
		return
	}

	for _, item := range listRaw {
		variable, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if variable["name"] != "node" {
			continue
		}

		includeAll, _ := variable["includeAll"].(bool)
		allValue, _ := variable["allValue"].(string)
		if allValue == "" {
			allValue = ".*"
		}

		options := make([]any, 0, len(nodes)+1)
		if includeAll {
			options = append(options, map[string]any{
				"text":     "All",
				"value":    allValue,
				"selected": selected == "",
			})
		}

		selectedValue := selected
		if selectedValue == "" && !includeAll && len(nodes) > 0 {
			selectedValue = nodes[0]
		}

		for _, node := range nodes {
			options = append(options, map[string]any{
				"text":     node,
				"value":    node,
				"selected": selectedValue == node,
			})
		}

		variable["options"] = options
		if selectedValue == "" {
			variable["current"] = map[string]any{"text": "All", "value": allValue}
		} else {
			variable["current"] = map[string]any{"text": selectedValue, "value": selectedValue}
		}
	}
}

func unique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}

	return result
}

func sanitize(value string) string {
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, " ", "-")
	value = strings.ReplaceAll(value, "_", "-")
	value = strings.ReplaceAll(value, "/", "-")
	value = strings.ReplaceAll(value, "--", "-")

	return strings.Trim(value, "-")
}

func cloneMap(source map[string]any) map[string]any {
	bytes, err := json.Marshal(source)
	if err != nil {
		return source
	}

	var cloned map[string]any
	if err := json.Unmarshal(bytes, &cloned); err != nil {
		return source
	}

	return cloned
}
