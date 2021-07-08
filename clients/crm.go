package clients

import (
	"context"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/config"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/crm-data-access"
	"google.golang.org/grpc"
)

type CRMClient struct {
	conn   *grpc.ClientConn
	client servicePb.CrmDataAccessClient
}

func NewCRMClient(cfg config.Config) (*CRMClient, error) {
	conn, err := grpc.Dial(cfg.CRMServiceAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c := &CRMClient{
		conn:   conn,
		client: servicePb.NewCrmDataAccessClient(conn),
	}
	return c, nil
}

func (x *CRMClient) Close() {
	if err := x.conn.Close(); err != nil {
		log.Error(err)
	}
}

func (client *CRMClient) GetLatestChangedPeople(ctx context.Context, tenantID string, changeSince *timestamp.Timestamp) ([]*orchardPb.Person, error) {
	res, err := client.client.GetLatestPeople(ctx, &servicePb.GetLatestPeopleRequest{TenantId: tenantID, ChangeSince: changeSince})
	if err != nil {
		return nil, err
	}
	return res.LatestPeople, nil
}
