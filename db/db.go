package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq" //pull in postgres
	ekg "github.com/loupe-co/go-common/ekg"
	"github.com/loupe-co/orchard/config"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

const DefaultDBTimeout = 30 * time.Second
const DefaultTenantID = "00000000-0000-0000-0000-000000000000"

var (
	Global *sql.DB
)

var (
	DefaultHealthCheckPolicy = ekg.HealthCheckPolicy{
		Interval:               "0/5 * * * *",
		FailureCondition:       ekg.FailureConditionCount,
		CountThreshold:         1,
		RetryPolicy:            ekg.RetryPolicyExponential,
		MaxRetries:             5,
		FailureStatusThreshold: ekg.StatusFailure,
		StatusRetention:        12,
	}
)

func Init(cfg config.Config) error {
	password := ""
	if cfg.DBPassword != "" {
		password = fmt.Sprintf("password=%s", cfg.DBPassword)
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s %s dbname=%s sslmode=disable", cfg.DBHost, cfg.DBPort, cfg.DBUser, password, cfg.DBName)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return err
	}
	Global = db
	maxConns := minInt(maxInt(cfg.DBMaxConnections, 10), 80)
	maxIdleConns := maxConns / 2
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	boil.SetDB(db)
	boil.DebugMode = cfg.DBDebug

	return nil
}

func HealthCheck(ctx context.Context) ekg.HealthStatus {
	if err := Global.PingContext(ctx); err != nil {
		return ekg.NewStatus(ekg.StatusFailure, err.Error())
	}
	return ekg.NewStatus(ekg.StatusOK, "")
}

func MakeID() string {
	uuid, err := uuid.NewUUID()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return uuid.String()
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}
