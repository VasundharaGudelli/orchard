package db

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	"github.com/volatiletech/sqlboiler/v4/boil"
)

// TODO: Add tracing

type GroupViewerService struct{}

func NewGroupViewerService() *GroupViewerService {
	return &GroupViewerService{}
}

func (svc *GroupViewerService) FromProto(gv *orchardPb.GroupViewer) *models.GroupViewer {
	createdAt := gv.CreatedAt.AsTime()
	updatedAt := gv.UpdatedAt.AsTime()

	return &models.GroupViewer{
		TenantID:    gv.TenantId,
		GroupID:     gv.GroupId,
		PersonID:    gv.PersonId,
		Permissions: gv.Permissions,
		CreatedAt:   createdAt,
		CreatedBy:   gv.CreatedBy,
		UpdatedAt:   updatedAt,
		UpdatedBy:   gv.UpdatedBy,
	}
}

func (svc *GroupViewerService) ToProto(gv *models.GroupViewer) (*orchardPb.GroupViewer, error) {
	createdAt, err := ptypes.TimestampProto(gv.CreatedAt)
	if err != nil {
		return nil, err
	}

	updatedAt, err := ptypes.TimestampProto(gv.UpdatedAt)
	if err != nil {
		return nil, err
	}

	return &orchardPb.GroupViewer{
		TenantId:    gv.TenantID,
		GroupId:     gv.GroupID,
		PersonId:    gv.PersonID,
		Permissions: gv.Permissions,
		CreatedAt:   createdAt,
		CreatedBy:   gv.CreatedBy,
		UpdatedAt:   updatedAt,
		UpdatedBy:   gv.UpdatedBy,
	}, nil
}

var (
	groupViewerInsertWhitelist = []string{
		"tenant_id", "group_id", "person_id", "permissions",
		"created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *GroupViewerService) Insert(ctx context.Context, gv *models.GroupViewer) error {
	return gv.Insert(ctx, Global, boil.Whitelist(groupViewerInsertWhitelist...))
}

var (
	defaultGroupViewerUpdateWhitelist = []string{
		"permissions", "updated_at", "updated_by",
	}
)

func (svc *GroupViewerService) Update(ctx context.Context, gv *models.GroupViewer) error {
	whitelist := defaultGroupViewerUpdateWhitelist

	numAffected, err := gv.Update(ctx, Global, boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating groupViewer: update affected 0 rows")
	}

	return nil
}

func (svc *GroupViewerService) DeleteByID(ctx context.Context, tenantID, groupID, personID string) error {
	gv := &models.GroupViewer{GroupID: groupID, PersonID: personID, TenantID: tenantID}
	numAffected, err := gv.Delete(ctx, Global)
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting groupViewer: delete affected 0 rows")
	}
	return nil
}
