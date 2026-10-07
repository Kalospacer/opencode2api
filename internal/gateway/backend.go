package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"opencode2api/internal/config"
	"opencode2api/internal/identity"
	"opencode2api/internal/models"
)

var errBackendSelection = errors.New("upstream backend selection failed")
var errAttemptBudget = errors.New("upstream attempt budget exhausted")

type upstreamBudgetKey struct{}

// upstreamAttemptBudget 在同一次子模型请求中共享，避免节点轮换与后端重选叠加次数。
// 上游尝试顺序执行，不与后台健康检查共享。
type upstreamAttemptBudget struct {
	limit int
	used  int
}

func attemptBudget(ctx context.Context) *upstreamAttemptBudget {
	budget, _ := ctx.Value(upstreamBudgetKey{}).(*upstreamAttemptBudget)
	return budget
}

func (b *upstreamAttemptBudget) take() bool {
	if b == nil {
		return true
	}
	if b.used >= b.limit {
		return false
	}
	b.used++
	return true
}

func (g *Gateway) doUpstream(ctx context.Context, route models.Route, bodies map[config.Tier][]byte, ids identity.RequestIDs) (*http.Response, models.Route, error) {
	if route.BackendSelector == "" {
		return g.doUpstreamWithReasoningRetry(ctx, route, bodies, ids)
	}
	limit := g.cfg.Retry.MaxAttempts
	if _, selected := debugKeyOverrideFrom(ctx); selected {
		limit = 1
	}
	budget := &upstreamAttemptBudget{limit: limit}
	ctx = context.WithValue(ctx, upstreamBudgetKey{}, budget)
	var lastErr error
	effectiveRoute := route
	for budget.used < budget.limit {
		if err := ctx.Err(); err != nil {
			return nil, effectiveRoute, err
		}
		if budget.used > 0 {
			// 换会话尝试重新选择后端，但保留本地请求标识用于监控关联。
			ids.Session = identity.CanonicalSessionID(identity.RandomID("seed", 16))
		}
		resp, selectedRoute, err := g.doUpstreamWithReasoningRetry(ctx, route, bodies, ids)
		effectiveRoute = selectedRoute
		if err != nil || resp == nil {
			return resp, effectiveRoute, err
		}
		if resp.StatusCode/100 != 2 {
			return resp, effectiveRoute, nil
		}
		if !strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
			lastErr = errors.New("upstream did not return the required SSE stream")
		} else {
			body, id, peekErr := peekFirstSSEDataID(resp)
			resp.Body = body
			if peekErr == nil && strings.HasPrefix(id, route.BackendSelector) {
				return resp, effectiveRoute, nil
			}
			lastErr = peekErr
			if lastErr == nil {
				lastErr = fmt.Errorf("upstream response id %q does not match %q", id, route.BackendSelector)
			}
		}
		// 不排空仍在生成的响应；直接关闭才能及时中止错误后端。
		_ = resp.Body.Close()
		if err := ctx.Err(); err != nil {
			return nil, effectiveRoute, err
		}
		g.logger.Info("upstream backend was not accepted", "component", "upstream", "event", "backend_rejected",
			"request_id", ids.Request, "model", route.ID, "attempt", budget.used,
			"max_attempts", budget.limit, "expected_prefix", route.BackendSelector, "error", lastErr)
	}
	return nil, effectiveRoute, fmt.Errorf("%w after %d upstream attempts: %v", errBackendSelection, budget.used, lastErr)
}

type peekedBody struct {
	io.Reader
	closer io.Closer
}

func (p *peekedBody) Close() error { return p.closer.Close() }

// peekFirstSSEDataID 等待第一个带顶层 id 的完整数据块，并保留全部原始字节。
// 支持注释、无 id 数据块、多行 data 字段及 LF/CRLF 换行。
func peekFirstSSEDataID(resp *http.Response) (io.ReadCloser, string, error) {
	const maxPrefixBytes = 1 << 20
	reader := bufio.NewReader(resp.Body)
	var consumed, line, data bytes.Buffer
	replay := &peekedBody{Reader: io.MultiReader(&consumed, reader), closer: resp.Body}
	for {
		part, err := reader.ReadSlice('\n')
		consumed.Write(part)
		if consumed.Len() > maxPrefixBytes {
			return replay, "", errors.New("SSE prefix exceeded 1 MiB without a backend id")
		}
		line.Write(part)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return replay, "", fmt.Errorf("SSE ended before a complete backend id event: %w", err)
		}
		text := strings.TrimSuffix(strings.TrimSuffix(line.String(), "\n"), "\r")
		line.Reset()
		if text == "" {
			if data.Len() == 0 {
				continue
			}
			payload := bytes.TrimSpace(data.Bytes())
			if bytes.Equal(payload, []byte("[DONE]")) {
				return replay, "", errors.New("SSE terminated before a backend id")
			}
			var event struct {
				ID    string          `json:"id"`
				Error json.RawMessage `json:"error"`
			}
			if err := json.Unmarshal(payload, &event); err != nil {
				return replay, "", fmt.Errorf("invalid SSE data before backend selection: %w", err)
			}
			if len(event.Error) > 0 && !bytes.Equal(event.Error, []byte("null")) {
				return replay, "", errors.New("upstream SSE error before backend selection")
			}
			if event.ID != "" {
				return replay, event.ID, nil
			}
			data.Reset()
			continue
		}
		if strings.HasPrefix(text, "data:") {
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(text[5:], " "))
		}
	}
}
