package clients

import (
	"context"

	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/config"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/tenant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TenantClient struct {
	conn   *grpc.ClientConn
	client servicePb.TenantClient
}

func NewTenantClient(cfg config.Config) (*TenantClient, error) {
	conn, err := grpc.Dial(cfg.TenantServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := &TenantClient{
		conn:   conn,
		client: servicePb.NewTenantClient(conn),
	}
	return c, nil
}

func (client *TenantClient) Close() {
	if err := client.conn.Close(); err != nil {
		log.Error(err)
	}
}

func (client *TenantClient) GetProvisionedUsers(ctx context.Context, tenantID string) ([]*orchardPb.Person, error) {
	res, err := client.client.GetTenantProvisionedUsers(ctx, &servicePb.GetTenantProvisionedUsersRequest{TenantId: tenantID})
	if err != nil {
		return nil, err
	}
	return res.ProvisionedUsers, nil
}
