package clients

import (
	"context"
	"math"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/config"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/crm-data-access"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CRMClient struct {
	conn   *grpc.ClientConn
	client servicePb.CrmDataAccessClient
}

func NewCRMClient(cfg config.Config) (*CRMClient, error) {
	conn, err := grpc.Dial(cfg.CRMServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := &CRMClient{
		conn:   conn,
		client: servicePb.NewCrmDataAccessClient(conn),
	}
	return c, nil
}

func (client *CRMClient) Close() {
	if err := client.conn.Close(); err != nil {
		log.Error(err)
	}
}

func (client *CRMClient) GetLatestChangedPeople(ctx context.Context, tenantID string, changeSince *timestamp.Timestamp, limit int, token string) ([]*orchardPb.Person, int, string, error) {
	res, err := client.client.GetLatestPeople(
		ctx,
		&servicePb.GetLatestPeopleRequest{
			TenantId:    tenantID,
			ChangeSince: changeSince,
			Limit:       int32(limit),
			Token:       token,
		},
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
	)
	if err != nil {
		return nil, 0, "", err
	}
	return res.LatestPeople, int(res.Total), res.NextToken, nil
}

func (client *CRMClient) GetLatestCRMRoles(ctx context.Context, tenantID string, changeSince *timestamp.Timestamp, limit int, token string) ([]*orchardPb.CRMRole, int, string, error) {
	res, err := client.client.GetLatestCRMRoles(
		ctx,
		&servicePb.GetLatestCRMRolesRequest{
			TenantId:    tenantID,
			ChangeSince: changeSince,
			Limit:       int32(limit),
			Token:       token,
		},
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
	)
	if err != nil {
		return nil, 0, "", err
	}
	return res.LatestRoles, int(res.Total), res.NextToken, nil
}
