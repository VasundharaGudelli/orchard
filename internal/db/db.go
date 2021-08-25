package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq" //pull in postgres
	ekg "github.com/loupe-co/go-common/ekg"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-common/retry"
	"github.com/loupe-co/orchard/internal/config"
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

// DB is a wrapper around the database functionality required for the auth service, such as getting system_roles and looking up person records
type DB struct {
	db *sql.DB
}

func New(cfg config.Config) (*DB, error) {
	password := ""
	if cfg.DBPassword != "" {
		password = fmt.Sprintf("password=%s", cfg.DBPassword)
	}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s %s dbname=%s sslmode=disable", cfg.DBHost, cfg.DBPort, cfg.DBUser, password, cfg.DBName)
	var db *sql.DB
	err := retry.RunWithRetryExponential(5, func() error {
		var err error
		db, err = sql.Open("postgres", psqlInfo)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to postgres")
	}
	maxConns := minInt(maxInt(cfg.DBMaxConnections, 10), 80)
	maxIdleConns := maxConns / 2
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	boil.SetDB(db)
	boil.DebugMode = cfg.DBDebug
	Global = db

	return &DB{
		db: db,
	}, nil
}

func (db *DB) NewTransaction(ctx context.Context) (*sql.Tx, error) {
	return db.db.BeginTx(ctx, nil)
}

type ModelExecuter interface {
	GetTransaction() *sql.Tx
}

func (db *DB) GetContextExecutor(svc ModelExecuter) boil.ContextExecutor {
	if tx := svc.GetTransaction(); tx != nil {
		return tx
	}
	return db.db
}

func (db *DB) HealthCheck(ctx context.Context) ekg.HealthStatus {
	if err := db.db.PingContext(ctx); err != nil {
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

type DBService struct {
	db *sql.DB
	tx *sql.Tx
}

func (db *DB) NewDBService() *DBService {
	return &DBService{
		db: db.db,
	}
}

func (svc *DBService) GetTransaction() *sql.Tx {
	return svc.tx
}

func (svc *DBService) SetTransaction(tx *sql.Tx) {
	svc.tx = tx
}

func (svc *DBService) Rollback() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Rollback()
}

func (svc *DBService) Commit() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Commit()
}

func (svc *DBService) GetContextExecutor() boil.ContextExecutor {
	if svc.tx != nil {
		return svc.tx
	}
	return svc.db
}
