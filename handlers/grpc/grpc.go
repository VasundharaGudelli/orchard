package grpchandlers

import (
	bouncer "github.com/loupe-co/bouncer/pkg/client"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/orchard/clients"
	"github.com/loupe-co/orchard/config"
	"google.golang.org/grpc/codes"
)

var (
	ErrBadRequest = errors.New("bad request").WithCode(codes.InvalidArgument)
)

type OrchardGRPCServer struct {
	cfg           config.Config
	tenantClient  *clients.TenantClient
	crmClient     *clients.CRMClient
	auth0Client   *clients.Auth0Client
	bouncerClient *bouncer.BouncerClient
}

func NewOrchardGRPCServer(
	cfg config.Config,
	tenantClient *clients.TenantClient,
	crmClient *clients.CRMClient,
	auth0Client *clients.Auth0Client,
	bouncerClient *bouncer.BouncerClient,
) *OrchardGRPCServer {
	return &OrchardGRPCServer{
		cfg:           cfg,
		tenantClient:  tenantClient,
		crmClient:     crmClient,
		auth0Client:   auth0Client,
		bouncerClient: nil,
	}
}
