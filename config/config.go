package config

import (
	"log"
	"sync"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

type GlobalConfig struct {
	cfgViper *viper.Viper
	cache    map[string]interface{}
}

func (cfg *GlobalConfig) GetViper() *viper.Viper {
	return cfg.cfgViper
}

func (cfg *GlobalConfig) GetBool(key string, dft ...interface{}) bool {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return false
	}
	return cast.ToBool(value)
}

func (cfg *GlobalConfig) GetInt(key string, dft ...interface{}) int {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToInt(value)
}

func (cfg *GlobalConfig) GetFloat32(key string, dft ...interface{}) float32 {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToFloat32(value)
}

func (cfg *GlobalConfig) GetInt32(key string, dft ...interface{}) int32 {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToInt32(value)
}

func (cfg *GlobalConfig) GetUint32(key string, dft ...interface{}) uint32 {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToUint32(value)
}

func (cfg *GlobalConfig) GetFloat64(key string, dft ...interface{}) float64 {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToFloat64(value)
}

func (cfg *GlobalConfig) GetInt64(key string, dft ...interface{}) int64 {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToInt64(value)
}

func (cfg *GlobalConfig) GetUint64(key string, dft ...interface{}) uint64 {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return 0
	}
	return cast.ToUint64(value)
}

func (cfg *GlobalConfig) GetString(key string, dft ...interface{}) string {
	value := cfg.getValue(key, dft...)
	if value == nil {
		return ""
	}
	return cast.ToString(value)
}

func (cfg *GlobalConfig) getValue(key string, dft ...interface{}) interface{} {
	value := cfg.cfgViper.Get(key)
	if value == nil {
		if len(dft) == 0 {
			return nil
		}
		return dft[0]
	}
	return value
}

var (
	cfg  GlobalConfig
	once sync.Once
)

func Init(configPath string) {
	once.Do(func() {
		cfg = GlobalConfig{
			cfgViper: viper.New(),
			cache:    make(map[string]interface{}),
		}
		cfg.cfgViper.SetConfigType("yaml")
		cfg.cfgViper.SetConfigFile(configPath)
		if err := cfg.cfgViper.ReadInConfig(); err != nil {
			log.Fatalf("Error reading config file, %s", err)
		}
	})
}

func GetConfig() GlobalConfig {
	return cfg
}
