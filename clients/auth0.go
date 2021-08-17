package clients

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/config"
	"github.com/loupe-co/orchard/models"
	"gopkg.in/auth0.v4"
	"gopkg.in/auth0.v4/management"
)

type Auth0Client struct {
	cfg config.Config
}

func NewAuth0Client(cfg config.Config) *Auth0Client {
	return &Auth0Client{cfg: cfg}
}

func (ac Auth0Client) getClient(ctx context.Context) (*management.Management, error) {
	return management.New(ac.cfg.Auth0Domain, ac.cfg.Auth0ClientID, ac.cfg.Auth0ClientSecret, management.WithContext(ctx))
}

type Auth0License struct {
	IsActive bool `json:"is_active"`
}

func (ac Auth0Client) Provision(ctx context.Context, tenantID string, user *models.Person) error {
	spanCtx, span := log.StartSpan(ctx, "Provision")
	defer span.End()

	logger := log.WithTenantID(tenantID).WithCustom("userId", user.ID)

	provisionedUser := &management.User{
		ID:            auth0.String(getAuth0UserID(tenantID, user.ID)),
		Email:         auth0.String(user.Email.String),
		EmailVerified: auth0.Bool(true),
		Connection:    auth0.String("email"),
		AppMetadata: map[string]interface{}{
			"license":   &Auth0License{IsActive: true},
			"person_id": user.ID,
			"tenant_id": tenantID,
		},
	}

	client, err := ac.getClient(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting auth0 management client")
		logger.Error(err)
		return err
	}

	if err := client.User.Create(provisionedUser); err != nil {
		err := errors.Wrap(err, "error creating provisioned user in auth0")
		logger.Error(err)
		return err
	}

	return nil
}

func (ac Auth0Client) Unprovision(ctx context.Context, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Unprovision")
	defer span.End()

	logger := log.WithTenantID(tenantID).WithCustom("userId", userID)

	client, err := ac.getClient(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting auth0 management client")
		logger.Error(err)
		return err
	}

	if err := client.User.Delete(getAuth0UserID(tenantID, userID)); err != nil {
		err := errors.Wrap(err, "error deleting provisioned user in auth0")
		logger.Error(err)
		return err
	}

	return nil
}

func getAuth0UserID(tenantID, userID string) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%s", tenantID, userID)))
}
