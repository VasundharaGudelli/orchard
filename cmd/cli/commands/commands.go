package commands

import (
	"fmt"

	"github.com/loupe-co/orchard/internal/clients"
	"github.com/loupe-co/orchard/internal/config"
	"github.com/loupe-co/orchard/internal/db"
)

const DefaultENV = "dev"

var envConfigs = map[string]config.Config{
	"dev": {
		ProjectID:         "loupe-dev",
		DBHost:            "35.245.37.78",
		DBPort:            5432,
		DBUser:            "postgres",
		DBPassword:        "jLariybb1oe5FbDz",
		DBName:            "tenant",
		DBMaxConnections:  10,
		DBDebug:           false,
		Auth0Issuer:       "loupe-dev.auth0.com",
		Auth0Audience:     "Fb8FuT6ezfLFG2tabZeFh2r8NsTD4AAm",
		Auth0Domain:       "https://loupe-dev.auth0.com",
		Auth0ClientID:     "DNGDG7ypZ1aCm98y2SGImpHIEexPTwDP",
		Auth0ClientSecret: "p6v0GpkOpDJHC3TatCHBHXbUj9QmaKZlP2wIW8ljWNlFyI32ex_dT7YwkYzNwpik",
	},
	"prod": {
		ProjectID:         "loupe-prod",
		DBHost:            "35.230.174.219",
		DBPort:            5432,
		DBUser:            "postgres",
		DBPassword:        "aM73nc7L6POJ3FIA",
		DBName:            "tenant",
		DBMaxConnections:  10,
		DBDebug:           false,
		Auth0Issuer:       "auth.canopy.io",
		Auth0Audience:     "Ub9IKZnGYUh7oM42iPBumI32cLWmVNWC",
		Auth0Domain:       "https://loupe.auth0.com",
		Auth0ClientID:     "srHbwyAP4Qk8REKLAFtgCsKS5YpJVY07",
		Auth0ClientSecret: "Z4UO0ageIzBGHEQvkmH6trPoyUzgl6YUhM_gerYuC7nrw-UwHaMxXi-Ee__oayqb",
	},
}

func GetOrchardDB(env string) (*db.DB, error) {
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
	return dbClient, nil
}

func GetAuth0Client(env string) (*clients.Auth0Client, error) {
	cfg := envConfigs[DefaultENV]
	if _cfg, ok := envConfigs[env]; env != "" && !ok {
		return nil, fmt.Errorf("invalid env value")
	} else if ok {
		cfg = _cfg
	}
	auth0Client := clients.NewAuth0Client(cfg)
	return auth0Client, nil
}
