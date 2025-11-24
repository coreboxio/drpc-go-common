package http

import (
	"net/http"
	"time"

	"github.com/drpc/drpc-go-common/config"
	"github.com/drpc/drpc-go-common/logging"
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

		t := time.Now()

		c.Next()

		latency := time.Since(t)
		maxLatency := time.Duration(cfg.GetInt("http_max_latency_log_ms", 2000))
		if maxLatency > 0 && latency.Milliseconds() > maxLatency.Milliseconds() {
			logging.Warn("[HTTP Response Time] %s %s %s", c.Request.Method, c.Request.URL.Path, latency.String())
		}
	}
}
