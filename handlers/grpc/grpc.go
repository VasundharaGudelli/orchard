package grpchandlers

import (
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/orchard/clients"
	"github.com/loupe-co/orchard/config"
	"google.golang.org/grpc/codes"
)

var (
	ErrBadRequest = errors.New("bad request").WithCode(codes.InvalidArgument)
)

type OrchardGRPCServer struct {
	cfg          config.Config
	tenantClient *clients.TenantClient
	crmClient    *clients.CRMClient
	auth0Client  *clients.Auth0Client
}

func NewOrchardGRPCServer(cfg config.Config, tenantClient *clients.TenantClient, crmClient *clients.CRMClient, auth0Client *clients.Auth0Client) *OrchardGRPCServer {
	return &OrchardGRPCServer{cfg: cfg, tenantClient: tenantClient, crmClient: crmClient, auth0Client: auth0Client}
}
