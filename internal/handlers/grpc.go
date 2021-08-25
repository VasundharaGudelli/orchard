package handlers

import (
	bouncer "github.com/loupe-co/bouncer/pkg/client"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/orchard/internal/clients"
	"github.com/loupe-co/orchard/internal/config"
	"github.com/loupe-co/orchard/internal/db"
	"google.golang.org/grpc/codes"
)

var (
	ErrBadRequest = errors.New("bad request").WithCode(codes.InvalidArgument)
)

type Handlers struct {
	cfg           config.Config
	db            *db.DB
	tenantClient  *clients.TenantClient
	crmClient     *clients.CRMClient
	auth0Client   *clients.Auth0Client
	bouncerClient *bouncer.BouncerClient
}

func New(
	cfg config.Config,
	dbClient *db.DB,
	tenantClient *clients.TenantClient,
	crmClient *clients.CRMClient,
	auth0Client *clients.Auth0Client,
	bouncerClient *bouncer.BouncerClient,
) *Handlers {
	return &Handlers{
		cfg:           cfg,
		db:            dbClient,
		tenantClient:  tenantClient,
		crmClient:     crmClient,
		auth0Client:   auth0Client,
		bouncerClient: nil,
	}
}
