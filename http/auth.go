package http

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/coreboxio/drpc-go-common/config"
	"github.com/coreboxio/drpc-go-common/logging"
	"github.com/coreboxio/drpc-go-common/utils"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

var (
	initOnce sync.Once
	rdb      *redis.Client
	ctx      = context.Background()
)

type UserAgentEntity struct {
	Platform string `json:"platform"`
	Uid      uint64 `json:"uid"`
	Token    string `json:"token"`
}

func UserAgent() gin.HandlerFunc {
	return func(c *gin.Context) {
		var ua *UserAgentEntity

		authorization := c.Request.Header.Get("Authorization")
		if authorization != "" {
			token := authorization

			if len(authorization) > 7 && authorization[:7] == "Bearer " {
				token = authorization[7:]
			}

			ua = queryUserAgent(token)
		}

		c.Set("ua", ua)

		c.Next()
	}
}

func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {

		baseController := BaseController{}

		ua, ok := c.Get("ua")
		if !ok {
			logging.Warn("auth fail: ua not found")
			baseController.Error(c, Code_AuthFail, nil)
			c.Abort()
			return
		}

		if ua == nil {
			logging.Warn("auth fail: ua is nil")
			baseController.Error(c, Code_AuthFail, nil)
			c.Abort()
			return
		}

		userAgentEntity := ua.(*UserAgentEntity)

		if userAgentEntity == nil {
			logging.Warn("auth fail: userAgentEntity is nil")
			baseController.Error(c, Code_AuthFail, nil)
			c.Abort()
			return
		}

		if userAgentEntity.Uid == 0 {
			logging.Warn("auth fail: userAgentEntity.Uid is 0")
			baseController.Error(c, Code_AuthFail, nil)
			c.Abort()
			return
		}

		c.Next()
	}
}

func queryUserAgent(token string) *UserAgentEntity {
	initOnce.Do(func() {
		cfg := config.GetConfig()
		rdb = redis.NewClient(&redis.Options{
			Addr: cfg.GetString("http_server_auth_redis", "127.0.0.1:6379"),
		})
	})

	cfg := config.GetConfig()
	key := fmt.Sprintf("%s%s", cfg.GetString("auth_key_prefix", "cba_"), token)

	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}

	var ua UserAgentEntity
	err = json.Unmarshal([]byte(val), &ua)
	if err != nil {
		return nil
	}

	return &ua
}

func CreateToken(uid uint64, platform string) (string, error) {
	initOnce.Do(func() {
		cfg := config.GetConfig()
		rdb = redis.NewClient(&redis.Options{
			Addr: cfg.GetString("http_server_auth_redis", "127.0.0.1:6379"),
		})
	})

	cfg := config.GetConfig()

	token := utils.GenUUID()

	ua := UserAgentEntity{
		Uid:      uid,
		Platform: platform,
		Token:    token,
	}

	uaJson, err := json.Marshal(ua)
	if err != nil {
		return "", err
	}

	key := fmt.Sprintf("%s%s", cfg.GetString("auth_key_prefix", "cba_"), token)

	ttlDays := cfg.GetInt(fmt.Sprintf("auth_token_ttl_days_%s", platform), 30)

	err = rdb.Set(ctx, key, uaJson, time.Duration(ttlDays)*24*60*60*time.Second).Err()
	if err != nil {
		return "", err
	}

	tokenListKey := fmt.Sprintf("%slist_%s_%d", cfg.GetString("auth_key_prefix", "cba_"), platform, uid)
	err = rdb.ZAdd(ctx, tokenListKey, &redis.Z{Score: float64(time.Now().Unix()), Member: token}).Err()
	if err != nil {
		return "", err
	}

	maxTokenCount := cfg.GetInt(fmt.Sprintf("auth_token_max_count_%s", platform), 2)

	script := fmt.Sprintf(`
		local count = redis.call("ZCARD", KEYS[1])
		if count > %d then
			local start = 0
			local stop = count - %d - 1
			local tokenKeyList = redis.call("ZRANGE", KEYS[1], start, stop)
			for _, tokenKey in ipairs(tokenKeyList) do
				redis.call("DEL", "%s" .. tokenKey)	
				redis.call("ZREM", KEYS[1], tokenKey)
			end
		end
	`, maxTokenCount, maxTokenCount, cfg.GetString("auth_key_prefix", "cba_"))

	err = rdb.Eval(ctx, script, []string{tokenListKey}).Err()

	if err != nil && err != redis.Nil {
		return "", err
	}

	return token, nil
}
