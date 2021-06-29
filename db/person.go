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

type PersonService struct{}

func NewPersonService() *PersonService {
	return &PersonService{}
}

func (svc *PersonService) FromProto(p *orchardPb.Person) *models.Person {
	createdAt := p.CreatedAt.AsTime()
	updatedAt := p.UpdatedAt.AsTime()

	return &models.Person{
		ID:            p.Id,
		TenantID:      p.TenantId,
		Name:          null.NewString(p.Name, p.Name != ""),
		FirstName:     null.NewString(p.FirstName, p.FirstName != ""),
		LastName:      null.NewString(p.LastName, p.LastName != ""),
		Email:         null.NewString(p.Email, p.Email != ""),
		ManagerID:     null.NewString(p.ManagerId, p.ManagerId != ""),
		GroupID:       null.NewString(p.GroupId, p.GroupId != ""),
		RoleIds:       types.StringArray(p.RoleIds),
		CRMRoleIds:    types.StringArray(p.CrmRoleIds),
		IsProvisioned: p.IsProvisioned,
		IsSynced:      p.IsSynced,
		Status:        strings.ToLower(p.Status.String()),
		CreatedAt:     createdAt,
		CreatedBy:     p.CreatedBy,
		UpdatedAt:     updatedAt,
		UpdatedBy:     p.UpdatedBy,
	}
}

func (svc *PersonService) ToProto(p *models.Person) (*orchardPb.Person, error) {
	createdAt, err := ptypes.TimestampProto(p.CreatedAt)
	if err != nil {
		return nil, err
	}

	updatedAt, err := ptypes.TimestampProto(p.UpdatedAt)
	if err != nil {
		return nil, err
	}

	status := orchardPb.BasicStatus_Inactive
	switch p.Status {
	case strings.ToLower(orchardPb.BasicStatus_Active.String()):
		status = orchardPb.BasicStatus_Active
	}

	return &orchardPb.Person{
		Id:            p.ID,
		TenantId:      p.TenantID,
		Name:          p.Name.String,
		FirstName:     p.FirstName.String,
		LastName:      p.LastName.String,
		Email:         p.Email.String,
		ManagerId:     p.ManagerID.String,
		GroupId:       p.GroupID.String,
		RoleIds:       []string(p.RoleIds),
		CrmRoleIds:    []string(p.CRMRoleIds),
		IsProvisioned: p.IsProvisioned,
		IsSynced:      p.IsSynced,
		Status:        status,
		CreatedAt:     createdAt,
		CreatedBy:     p.CreatedBy,
		UpdatedAt:     updatedAt,
		UpdatedBy:     p.UpdatedBy,
	}, nil
}
