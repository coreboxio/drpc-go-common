package http

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/coreboxio/drpc-go-common/config"
	"github.com/coreboxio/drpc-go-common/logging"
	"github.com/gin-gonic/gin"
)

func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := config.GetConfig()
		status := c.Writer.Status()
		if status != http.StatusOK {
			logging.Error("[HTTP Response Error] %s %s %d", c.Request.Method, c.Request.URL.Path, status)
			c.Abort()
			return
		}

		// 读取 query 参数
		query := c.Request.URL.Query().Encode()

		// 读取 body（仅对 POST/PUT/PATCH 等方法）
		var body string
		if c.Request.Method == http.MethodPost || c.Request.Method == http.MethodPut || c.Request.Method == http.MethodPatch {
			if c.Request.Body != nil {
				bodyBytes, err := io.ReadAll(c.Request.Body)
				if err == nil {
					body = string(bodyBytes)
					// 重新设置 body，因为已经被读取
					c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				}
			}
		}

		t := time.Now()

		c.Next()

		latency := time.Since(t)
		maxLatency := time.Duration(cfg.GetInt("http_max_latency_log_ms", 2000))
		if maxLatency > 0 && latency.Milliseconds() > maxLatency.Milliseconds() {
			logging.Warn("[HTTP Response Time] %s %s %s", c.Request.Method, c.Request.URL.Path, latency.String())
		}

		if cfg.GetBool("http_log_enabled") {
			finalStatus := c.Writer.Status()
			logMsg := "[HTTP Response] %s %s %d %s"
			logArgs := []interface{}{c.Request.Method, c.Request.URL.Path, finalStatus, latency.String()}

			if query != "" {
				logMsg += " query=%s"
				logArgs = append(logArgs, query)
			}

			if body != "" {
				logMsg += " body=%s"
				logArgs = append(logArgs, body)
			}

			logging.Info(logMsg, logArgs...)
		}
	}
}
