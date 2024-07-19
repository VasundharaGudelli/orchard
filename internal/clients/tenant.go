package clients

import (
	"context"
	"encoding/json"

	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/config"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	"github.com/loupe-co/protos/src/common/tenant"
	servicePb "github.com/loupe-co/protos/src/services/tenant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	ctx, span := log.StartSpan(ctx, "Tenant.GetProvisionedUsers")
	defer span.End()

	res, err := client.client.GetTenantProvisionedUsers(ctx, &servicePb.GetTenantProvisionedUsersRequest{TenantId: tenantID})
	if err != nil {
		return nil, err
	}
	return res.ProvisionedUsers, nil
}

func (client *TenantClient) GetTenantLastFullDataSync(ctx context.Context, tenantID string) (*timestamppb.Timestamp, error) {
	res, err := client.client.GetTenantLastFullDataSync(ctx, &servicePb.GetTenantLastFullDataSyncRequest{TenantId: tenantID})
	if err != nil {
		return nil, err
	}
	return res.LastSync, nil
}

func (x *TenantClient) GetTenantByID(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	res, err := x.client.GetTenantById(ctx, &servicePb.GetTenantByIdRequest{TenantId: tenantID})
	if err != nil {
		return nil, nil
	}
	return res.Tenant, nil
}

func (x *TenantClient) IsOutreachSyncEnabled(ctx context.Context, tenantData *tenant.Tenant) (bool, error) {
	if tenantData != nil {
		dataSyncSettingsBytes := tenantData.DataSyncSettings
		var dataSyncSettings map[string]interface{}
		err := json.Unmarshal(dataSyncSettingsBytes, &dataSyncSettings)
		if err != nil {
			return false, err
		}
		if dataSyncSettings != nil {
			if isOutreachSyncEnabled, ok := dataSyncSettings["IsOutreachSyncEnabled"].(bool); ok {
				log.WithTenantID(tenantData.Id).Infof("IsOutreachSyncEnabled from tenant-service: %v, id: %v", isOutreachSyncEnabled, tenantData.Id)
				return isOutreachSyncEnabled, nil
			}
			log.WithTenantID(tenantData.Id).Warnf("IsOutreachSyncEnabled from tenant-service not present id: %v", tenantData.Id)
		}
	}
	return false, nil
}
