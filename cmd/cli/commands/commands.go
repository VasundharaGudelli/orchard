package commands

import (
	"fmt"

	"github.com/loupe-co/orchard/internal/config"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/handlers"
)

const DefaultENV = "dev"

var envConfigs = map[string]config.Config{
	"dev": {
		ProjectID:        "loupe-dev",
		DBHost:           "35.245.37.78",
		DBPort:           5432,
		DBUser:           "postgres",
		DBPassword:       "jLariybb1oe5FbDz",
		DBName:           "tenant",
		DBMaxConnections: 10,
		DBDebug:          false,
		RedisHost:        "localhost:6379",
		RedisPassword:    "",
	},
	"prod": {
		ProjectID:        "loupe-prod",
		DBHost:           "10.117.32.2",
		DBPort:           5432,
		DBUser:           "postgres",
		DBPassword:       "aM73nc7L6POJ3FIA",
		DBName:           "tenant",
		DBMaxConnections: 10,
		DBDebug:          false,
		RedisHost:        "localhost:6379",
		RedisPassword:    "",
	},
}

func GetOrchardHandlers(env string) (*handlers.Handlers, error) {
	cfg := envConfigs[DefaultENV]
	if _cfg, ok := envConfigs[env]; env != "" && !ok {
		return nil, fmt.Errorf("invalid env value")
	} else if ok {
		cfg = _cfg
	}
	dbClient, err := db.New(cfg)
	if err != nil {
		return nil, err
	}
	return handlers.New(cfg, dbClient, nil, nil, nil, nil), nil
}
