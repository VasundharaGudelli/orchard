package clients

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	strUtil "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/config"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
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
	x, err := management.New(ac.cfg.Auth0Domain, ac.cfg.Auth0ClientID, ac.cfg.Auth0ClientSecret, management.WithContext(ctx), management.WithDebug(false))
	count := 0
	for err != nil && strings.Contains(strings.ToLower(err.Error()), "unexpected eof") && count < 3 {
		time.Sleep(100 * time.Millisecond)
		x, err = management.New(ac.cfg.Auth0Domain, ac.cfg.Auth0ClientID, ac.cfg.Auth0ClientSecret, management.WithContext(ctx), management.WithDebug(false))
		count++
	}
	return x, err
}

type Auth0License struct {
	IsActive bool `json:"is_active"`
}

type TenantContext struct {
	TenantID  string `json:"tenant_id"`
	UserID    string `json:"user_id"`
	IsPrimary bool   `json:"is_primary"`
}

func (ac Auth0Client) Provision(ctx context.Context, personRecords []*models.Person) error {
	spanCtx, span := log.StartSpan(ctx, "Provision")
	defer span.End()

	if len(personRecords) == 0 {
		err := errors.New("personRecords cannot be empty for provisioning").WithCode(codes.InvalidArgument)
		return err
	}

	var (
		user           *models.Person
		isPrimaryIndex int
	)
	tenantContexts := make([]*TenantContext, len(personRecords))
	for i, person := range personRecords {
		if user == nil || person.CreatedAt.Before(user.CreatedAt) {
			user = person
			isPrimaryIndex = i
		}

		tenantContexts[i] = &TenantContext{
			TenantID:  person.TenantID,
			UserID:    person.ID,
			IsPrimary: false,
		}
	}

	// set primary
	tenantContexts[isPrimaryIndex].IsPrimary = true

	logger := log.WithTenantID(user.TenantID).WithCustom("userId", user.ID)

	provisionedUser := &management.User{
		Email:         auth0.String(user.Email.String),
		EmailVerified: auth0.Bool(true),
		Connection:    auth0.String("email"),
		AppMetadata: map[string]interface{}{
			"license":         &Auth0License{IsActive: true},
			"person_id":       user.ID,
			"tenant_id":       user.TenantID,
			"tenant_contexts": tenantContexts,
		},
	}

	client, err := ac.getClient(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting auth0 management client")
		logger.Error(err)
		return err
	}

	existingUsers, err := ac.searchUserByEmail(spanCtx, client, user.TenantID, user.Email.String)
	if err != nil {
		err := errors.Wrap(err, "error checking for existing user in auth0 by email")
		logger.Error(err)
		return err
	}

	if len(existingUsers) > 0 {
		existingUser := existingUsers[0]
		provisionedUser.ID = existingUser.ID
		if err := client.User.Update(*existingUser.ID, &management.User{AppMetadata: provisionedUser.AppMetadata}); err != nil {
			err := errors.Wrap(err, "error re-provisioning existing user in auth0")
			logger.Error(err)
			return err
		}
	} else {
		if err := client.User.Create(provisionedUser); err != nil {
			err := errors.Wrap(err, "error creating provisioned user in auth0")
			logger.Error(err)
			return err
		}
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

func (ac Auth0Client) ImportUsers(ctx context.Context, tenantID string) ([]*orchardPb.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "ImportUsers")
	defer span.End()

	logger := log.WithTenantID(tenantID)

	client, err := ac.getClient(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting auth0 management client")
		logger.Error(err)
		return nil, err
	}

	people := []*orchardPb.Person{}
	for i := 0; ; i++ {
		users, total, err := ac.getUsersByTenantID(spanCtx, client, tenantID, i, 50)
		if err != nil {
			err := errors.Wrap(err, "error getting users by tenantId for import")
			logger.Error(err)
			return nil, err
		}
		people = append(people, convertUsers(users)...)
		if len(people) >= total {
			break
		}
	}

	return people, nil
}

func (ac Auth0Client) GetRoleUsers(ctx context.Context, roleID string) ([]*orchardPb.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "GetRoleUsers")
	defer span.End()

	logger := log.WithCustom("roleId", roleID)

	client, err := ac.getClient(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting auth0 management client")
		logger.Error(err)
		return nil, err
	}

	people := []*orchardPb.Person{}
	for i := 0; ; i++ {
		users, total, err := ac.getRoleUsers(spanCtx, client, roleID, i, 50)
		if err != nil {
			err := errors.Wrap(err, "error getting users by roleId")
			logger.Error(err)
			return nil, err
		}
		people = append(people, convertUsers(users)...)
		if len(people) >= total {
			break
		}
	}

	return people, nil
}

func (ac Auth0Client) getRoleUsers(ctx context.Context, client *management.Management, roleID string, page, take int) ([]*management.User, int, error) {
	usersRes, err := client.Role.Users(roleID, management.IncludeTotals(true), management.Page(page), management.PerPage(take))
	if err != nil {
		return nil, 0, errors.Wrap(err, "error getting list of users by role from auth0")
	}
	if usersRes == nil || len(usersRes.Users) == 0 {
		return nil, 0, nil
	}
	return usersRes.Users, usersRes.Total, nil
}

func (ac Auth0Client) getByUserID(ctx context.Context, client *management.Management, tenantID, userID string) (*management.User, error) {
	q := fmt.Sprintf(`(app_metadata.tenant_id:"%s" AND app_metadata.person_id:"%s") OR (app_metadata.tenant_contexts.tenant_id:"%s" AND app_metadata.tenant_contexts.user_id:"%s")`, tenantID, userID, tenantID, userID)
	mQ := management.Query(q)
	users, err := client.User.List(mQ, management.PerPage(1), management.Parameter("search_engine", "v3"))
	if err != nil {
		return nil, errors.Wrap(err, "error getting list of users from auth0")
	}
	if users == nil || len(users.Users) == 0 {
		return nil, nil
	}

	ac.sortUsersByUsage(users)

	return users.Users[0], nil
}

// sort users by login count/last login
func (ac Auth0Client) sortUsersByUsage(users *management.UserList) {
	if len(users.Users) > 1 {
		now := time.Now().Unix()
		sort.SliceStable(users.Users, func(i, j int) bool {
			var (
				iSort int64
				jSort int64
			)

			if users.Users[i].LoginsCount != nil {
				iSort = now + *users.Users[i].LoginsCount
			} else if users.Users[i].LastLogin != nil {
				iSort = users.Users[i].LastLogin.Unix()
			}

			if users.Users[j].LoginsCount != nil {
				jSort = now + *users.Users[j].LoginsCount
			} else if users.Users[j].LastLogin != nil {
				iSort = users.Users[j].LastLogin.Unix()
			}

			return iSort > jSort
		})
	}
}

func (ac Auth0Client) searchUserByEmail(ctx context.Context, client *management.Management, tenantID, email string) ([]*management.User, error) {
	q := fmt.Sprintf(`(app_metadata.tenant_id:"%s" OR app_metadata.tenant_contexts.tenant_id:"%s") AND email:"%s"`, tenantID, tenantID, email)
	mQ := management.Query(q)
	users, err := client.User.List(mQ, management.PerPage(50), management.Parameter("search_engine", "v3"))
	if err != nil {
		return nil, errors.Wrap(err, "error getting list of users from auth0")
	}
	if users == nil || len(users.Users) == 0 {
		return nil, nil
	}

	ac.sortUsersByUsage(users)

	return users.Users, nil
}

func (ac Auth0Client) getUsersByTenantID(ctx context.Context, client *management.Management, tenantID string, page, take int) ([]*management.User, int, error) {
	q := fmt.Sprintf(`(app_metadata.tenant_id:"%s" OR app_metadata.tenant_contexts.tenant_id:"%s") AND app_metadata.license.is_active:true`, tenantID, tenantID)
	mQ := management.Query(q)
	users, err := client.User.List(mQ, management.IncludeTotals(true), management.Page(page), management.PerPage(take), management.Parameter("search_engine", "v3"))
	if err != nil {
		return nil, 0, errors.Wrap(err, "error getting list of users from auth0")
	}
	if users == nil || len(users.Users) == 0 {
		return nil, 0, nil
	}
	return users.Users, users.Total, nil
}

func convertUsers(users []*management.User) []*orchardPb.Person {
	people := make([]*orchardPb.Person, len(users))
	for i, user := range users {
		people[i] = convertUser(user)
	}
	return people
}

func convertUser(user *management.User) *orchardPb.Person {
	person := &orchardPb.Person{}

	if personIDRaw, ok := user.AppMetadata["person_id"]; ok {
		if personID, ok := personIDRaw.(string); ok {
			person.Id = personID
		}
	} else {
		if user.ID != nil && strings.HasPrefix(*user.ID, "salesforce|") {
			person.Id = strings.TrimPrefix(*user.ID, "salesforce|")
		}
	}
	if tenantIDRaw, ok := user.AppMetadata["tenant_id"]; ok {
		if tenantID, ok := tenantIDRaw.(string); ok {
			person.TenantId = tenantID
		}
	}
	if user.Email != nil {
		person.Email = *user.Email
	}
	if user.Picture != nil {
		person.PhotoUrl = *user.Picture
	}
	if user.Name != nil {
		person.Name = *user.Name
	}
	if user.GivenName != nil {
		person.FirstName = *user.GivenName
	}
	if user.FamilyName != nil {
		person.LastName = *user.FamilyName
	}

	person.CreatedAt = ptypes.TimestampNow()
	if user.CreatedAt != nil {
		person.CreatedAt, _ = ptypes.TimestampProto(*user.CreatedAt)
	}

	person.UpdatedAt = ptypes.TimestampNow()
	if user.UpdatedAt != nil {
		person.UpdatedAt, _ = ptypes.TimestampProto(*user.UpdatedAt)
	}

	return person
}
