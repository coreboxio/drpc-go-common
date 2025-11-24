package frequency

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coreboxio/drpc-go-common/config"
	"github.com/coreboxio/drpc-go-common/logging"

	"github.com/go-redis/redis/v8"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

type RateLimitData struct {
	Timestamp int64 `json:"t"`
	Count     int   `json:"c"`
}

func Limit(key string, rangeMin int, maxCount int) bool {
	cfg := config.GetConfig()

	if cfg.GetBool("frequency_disabled") {
		return false
	}

	if rdb == nil {
		rdb = redis.NewClient(&redis.Options{
			Addr: cfg.GetString("http_server_redis", "127.0.0.1:6379"), // TODO change to args
		})
	}

	now := time.Now().Unix()
	now = now / int64(rangeMin*60) * int64(rangeMin*60)

	var data RateLimitData
	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		data = RateLimitData{Timestamp: now, Count: 1}
	} else if err != nil {
		logging.Error("Error getting key: %v", err)
		return false
	} else {
		err = json.Unmarshal([]byte(val), &data)
		if err != nil {
			logging.Error("Error unmarshalling data: %v", err)
			return false
		}

		if data.Timestamp != now {
			data = RateLimitData{Timestamp: now, Count: 1}
		} else if data.Count >= maxCount {
			return true
		} else {
			data.Count++
		}
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		logging.Error("Error marshalling data: %v", err)
		return false
	}

	_, err = rdb.SetEX(ctx, key, dataBytes, time.Duration(rangeMin)*time.Minute).Result()
	if err != nil {
		logging.Error("Error setting key: %v", err)
		return false
	}

	return false
}
