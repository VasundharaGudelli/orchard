package clients

import (
	"context"
	"fmt"

	commonSet "github.com/loupe-co/go-common/data-structures/set/string"
	strUtil "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/config"
	"github.com/loupe-co/orchard/models"
	"google.golang.org/grpc/codes"
	"gopkg.in/auth0.v4"
	"gopkg.in/auth0.v4/management"
)

var LegacyUserRoleMappings = strUtil.Strings{
	"64cf1b39-d863-4601-bc21-f45dcf449e14",
	"6f71019d-25cf-4c6a-a31e-bdd25472ba26",
	"aafad8da-9dfa-417a-972b-89afadcb3302",
}

var LegacyManagerRoleMappings = strUtil.Strings{
	"a2e39cf5-e016-44a4-b037-057a16fe14fc",
	"9cb26135-9c37-4bc5-b620-c89177ad3ca3",
	"e3a44322-0559-4f0f-bf61-a3a0dcca0c54",
}

var LegacyAdminRoleMappings = strUtil.Strings{
	"8d94bd88-78a5-467c-a0d8-079f26b412d9",
}

var LegacySuperAdminRoleMappings = strUtil.Strings{
	"aaff61e7-d5e1-4cf6-9682-00f4f38bf1f5",
	"95f00236-3b8c-4806-bec1-fbf532b7ad10",
}

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

	legacyRoles := commonSet.New()
	for _, roleID := range user.RoleIds {
		if LegacyUserRoleMappings.Has(roleID) {
			legacyRoles.Set("rol_JbKBz2HaApjrd7yW")
		}
		if LegacyManagerRoleMappings.Has(roleID) {
			legacyRoles.Set("rol_510TUetL44xR7zmm")
		}
		if LegacyAdminRoleMappings.Has(roleID) {
			legacyRoles.Set("rol_6tBbx6gNRYgb47wM")
		}
		if LegacySuperAdminRoleMappings.Has(roleID) {
			legacyRoles.Set("rol_42KN8JcK3EgysI0Q")
		}
	}

	provisionedUser := &management.User{
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

	userRoles := make([]*management.Role, len(legacyRoles))
	for i, rID := range legacyRoles.Members() {
		userRoles[i] = &management.Role{
			ID: &rID,
		}
	}
	if err := client.User.AssignRoles(*provisionedUser.ID, userRoles...); err != nil {
		err := errors.Wrap(err, "error assigning user roles in auth0")
		logger.Error(err)
		return err
	}

	return nil
}

func (ac Auth0Client) Unprovision(ctx context.Context, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Unprovision")
	defer span.End()

	logger := log.WithTenantID(tenantID).WithCustom("userId", userID)

	// Get auth0 client instance
	client, err := ac.getClient(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting auth0 management client")
		logger.Error(err)
		return err
	}

	// Get user from auth0 so we can delete by the auth0 user ID
	user, err := ac.getByUserID(spanCtx, client, tenantID, userID)
	if err != nil {
		err := errors.Wrap(err, "error getting user from auth0")
		logger.Error(err)
		return err
	}
	if user == nil {
		return errors.New("user not found in auth0").WithCode(codes.NotFound)
	}

	// Delete user in auth0 by the auth0 user id
	if err := client.User.Delete(*user.ID); err != nil {
		err := errors.Wrap(err, "error deleting provisioned user in auth0")
		logger.Error(err)
		return err
	}

	return nil
}

func (ac Auth0Client) getByUserID(ctx context.Context, client *management.Management, tenantID, userID string) (*management.User, error) {
	q := fmt.Sprintf(`app_metadata.tenant_id:"%s" AND app_metadata.person_id:"%s"`, tenantID, userID)
	mQ := management.Query(q)
	users, err := client.User.List(mQ, management.PerPage(1), management.Parameter("search_engine", "v3"))
	if err != nil {
		return nil, errors.Wrap(err, "error getting list of users from auth0")
	}
	if users == nil || len(users.Users) == 0 {
		return nil, nil
	}
	return users.Users[0], nil
}
