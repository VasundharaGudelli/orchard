package helpers

import (
	"context"
	"fmt"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	"google.golang.org/grpc/codes"
)

var (
	ErrBadRequest = errors.New("bad request").WithCode(codes.InvalidArgument)
)

// Helper function to create transaction
func CreateTransaction(db *db.DB, logger *log.LogChain, spanCtx context.Context, svc *db.GroupService, txName string) error {
	tx, err := db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, fmt.Sprintf("error starting transaction for %s", txName))
		logger.Error(err)
		return err.AsGRPC()
	}
	svc.SetTransaction(tx)
	return nil
}

// Helper function to log error and rollback the transaction based on rollback flag
func ErrorHandler(logger *log.LogChain, svc *db.GroupService, err error, methodName string, rollback bool) error {
	if rollback {
		svc.Rollback()
	}
	nerr := errors.Wrap(err, methodName)
	logger.Error(nerr)
	return nerr.AsGRPC()
}

// Helper function to commit transaction
func CommitTransaction(logger *log.LogChain, svc *db.GroupService, methodName string) error {
	if err := svc.Commit(); err != nil {
		svc.Rollback()
		nerr := errors.Wrap(err, fmt.Sprintf("error commiting %s", methodName))
		logger.Error(nerr)
		return nerr.AsGRPC()
	}
	return nil
}

// Helper function to log Bad Requests
func BadRequest(logger *log.LogChain, errorString string) error {
	err := ErrBadRequest.New(errorString)
	logger.Warn(err.String())
	return err.AsGRPC()
}
