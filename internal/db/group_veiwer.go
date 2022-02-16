package db

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
)

type GroupViewerService struct {
	*DBService
}

func (db *DB) NewGroupViewerService() *GroupViewerService {
	return &GroupViewerService{
		DBService: db.NewDBService(),
	}
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
	spanCtx, span := log.StartSpan(ctx, "GroupViewer.Insert")
	defer span.End()
	return gv.Insert(spanCtx, svc.GetContextExecutor(), boil.Whitelist(groupViewerInsertWhitelist...))
}

const (
	getGroupViewersQuery = `SELECT p.*
	FROM group_viewer gv INNER JOIN person p ON p.id = gv.person_id AND p.tenant_id = gv.tenant_id
	WHERE gv.group_id = $1 AND gv.tenant_id = $2;`
)

func (svc *GroupViewerService) GetGroupViewers(ctx context.Context, tenantID, groupID string) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "GroupViewer.GetGroupViewers")
	defer span.End()
	results := []*models.Person{}
	err := queries.Raw(getGroupViewersQuery, groupID, tenantID).Bind(spanCtx, svc.GetContextExecutor(), &results)
	if err != nil {
		log.WithTenantID(tenantID).WithCustom("groupId", groupID).WithCustom("query", getGroupViewersQuery)
		return nil, err
	}
	return results, nil
}

const (
	getPersonViewableGroupsQuery = `SELECT g.*
	FROM group_viewer gv INNER JOIN "group" g ON g.id = gv.group_id AND g.tenant_id = gv.tenant_id
	WHERE gv.person_id = $1 AND gv.tenant_id = $2;`
)

func (svc *GroupViewerService) GetPersonViewableGroups(ctx context.Context, tenantID, personID string) ([]*models.Group, error) {
	spanCtx, span := log.StartSpan(ctx, "GroupViewer.GetPersonViewableGroups")
	defer span.End()
	results := []*models.Group{}
	err := queries.Raw(getPersonViewableGroupsQuery, personID, tenantID).Bind(spanCtx, svc.GetContextExecutor(), &results)
	if err != nil {
		log.WithTenantID(tenantID).WithCustom("personId", personID).WithCustom("query", getPersonViewableGroupsQuery)
		return nil, err
	}
	return results, nil
}

var (
	defaultGroupViewerUpdateWhitelist = []string{
		"permissions", "updated_at", "updated_by",
	}
)

func (svc *GroupViewerService) Update(ctx context.Context, gv *models.GroupViewer) error {
	spanCtx, span := log.StartSpan(ctx, "GroupViewer.Update")
	defer span.End()
	whitelist := defaultGroupViewerUpdateWhitelist
	numAffected, err := gv.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating groupViewer: update affected 0 rows")
	}

	return nil
}

func (svc *GroupViewerService) DeleteByID(ctx context.Context, tenantID, groupID, personID string) error {
	spanCtx, span := log.StartSpan(ctx, "GroupViewer.DeleteByID")
	defer span.End()
	gv := &models.GroupViewer{GroupID: groupID, PersonID: personID, TenantID: tenantID}
	numAffected, err := gv.Delete(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting groupViewer: delete affected 0 rows")
	}
	return nil
}
