package db

import (
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/types"
)

// TODO: Add tracing

type GroupService struct{}

func NewGroupService() *GroupService {
	return &GroupService{}
}

func (svc *GroupService) FromProto(g *orchardPb.Group) *models.Group {
	createdAt := g.CreatedAt.AsTime()
	updatedAt := g.UpdatedAt.AsTime()

	return &models.Group{
		ID:         g.Id,
		TenantID:   g.TenantId,
		Name:       g.Name,
		Type:       strings.ToLower(g.Type.String()),
		Status:     strings.ToLower(g.Status.String()),
		RoleIds:    types.StringArray(g.RoleIds),
		CRMRoleIds: types.StringArray(g.CrmRoleIds),
		ParentID:   null.NewString(g.ParentId, g.ParentId != ""),
		GroupPath:  g.GroupPath,
		Order:      int(g.Order),
		CreatedAt:  createdAt,
		CreatedBy:  g.CreatedBy,
		UpdatedAt:  updatedAt,
		UpdatedBy:  g.UpdatedBy,
	}
}

func (svc *GroupService) ToProto(g *models.Group) (*orchardPb.Group, error) {
	createdAt, err := ptypes.TimestampProto(g.CreatedAt)
	if err != nil {
		return nil, err
	}

	updatedAt, err := ptypes.TimestampProto(g.UpdatedAt)
	if err != nil {
		return nil, err
	}

	status := orchardPb.BasicStatus_Inactive
	switch g.Status {
	case strings.ToLower(orchardPb.BasicStatus_Active.String()):
		status = orchardPb.BasicStatus_Active
	}

	typ := orchardPb.SystemRoleType_Unknown
	switch g.Type {
	case strings.ToLower(orchardPb.SystemRoleType_IC.String()):
		typ = orchardPb.SystemRoleType_IC
	case strings.ToLower(orchardPb.SystemRoleType_Internal.String()):
		typ = orchardPb.SystemRoleType_Internal
	case strings.ToLower(orchardPb.SystemRoleType_Manager.String()):
		typ = orchardPb.SystemRoleType_Manager
	}

	return &orchardPb.Group{
		Id:         g.ID,
		TenantId:   g.TenantID,
		Name:       g.Name,
		Type:       typ,
		Status:     status,
		RoleIds:    []string(g.RoleIds),
		CrmRoleIds: []string(g.CRMRoleIds),
		ParentId:   g.ParentID.String,
		GroupPath:  g.GroupPath,
		Order:      int32(g.Order),
		CreatedAt:  createdAt,
		CreatedBy:  g.CreatedBy,
		UpdatedAt:  updatedAt,
		UpdatedBy:  g.UpdatedBy,
	}, nil
}
