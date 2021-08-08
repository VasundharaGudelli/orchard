package grpchandlers

import (
	"context"
	"time"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) CreatePerson(ctx context.Context, in *servicePb.CreatePersonRequest) (*servicePb.CreatePersonResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreatePerson")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Person == nil {
		return nil, ErrBadRequest.New("person can't be nil")
	}

	if in.Person.Id != "" {
		return nil, ErrBadRequest.New("person id must be empty to create a new person")
	}

	in.Person.Id = db.MakeID()

	svc := db.NewPersonService()

	insertablePerson := svc.FromProto(in.Person)
	insertablePerson.CreatedAt = time.Now().UTC()
	insertablePerson.UpdatedAt = time.Now().UTC()

	if err := svc.Insert(spanCtx, insertablePerson); err != nil {
		err := errors.Wrap(err, "error inserting person in sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	createdRes, err := svc.ToProto(insertablePerson)
	if err != nil {
		err := errors.Wrap(err, "error converting created person to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.CreatePersonResponse{Person: createdRes}, nil
}

func (server *OrchardGRPCServer) UpsertPeople(ctx context.Context, in *servicePb.UpsertPeopleRequest) (*servicePb.UpsertPeopleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpsertPeople")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if len(in.People) == 0 {
		return &servicePb.UpsertPeopleResponse{}, nil
	}

	svc := db.NewPersonService()

	upsertablePeople := make([]*models.Person, len(in.People))
	for i, p := range in.People {
		upsertablePeople[i] = svc.FromProto(p)
	}

	if err := svc.UpsertAll(spanCtx, upsertablePeople); err != nil {
		err := errors.Wrap(err, "error upserting one or more person records")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpsertPeopleResponse{}, nil
}

func (server *OrchardGRPCServer) GetPersonById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "GetPersonById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewPersonService()

	p, err := svc.GetByID(spanCtx, in.PersonId, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting person by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	person, err := svc.ToProto(p)
	if err != nil {
		err := errors.Wrap(err, "error converting person db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return person, nil
}

func (server *OrchardGRPCServer) SearchPeople(ctx context.Context, in *servicePb.SearchPeopleRequest) (*servicePb.SearchPeopleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "SearchPeople")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("search", in.Search).WithCustom("page", in.Page).WithCustom("pageSize", in.PageSize)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	limit := 20
	if in.PageSize > 0 {
		limit = int(in.PageSize)
	}
	offset := 0
	if in.Page > 0 {
		offset = (int(in.Page) - 1) * limit
	}

	svc := db.NewPersonService()

	peeps, total, err := svc.Search(spanCtx, in.TenantId, in.Search, limit, offset)
	if err != nil {
		err := errors.Wrap(err, "error searching people")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	crmRoleMap := map[string]int{}
	systemRoleMap := map[string]int{}
	people := make([]*orchardPb.Person, len(peeps))
	for i, peep := range peeps {
		p, err := svc.ToProto(peep)
		if err != nil {
			err := errors.Wrap(err, "error converting person db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		people[i] = p
		for _, crmRoleID := range p.CrmRoleIds {
			crmRoleMap[crmRoleID] = i
		}
		for _, roleID := range p.RoleIds {
			systemRoleMap[roleID] = i
		}
	}

	if in.HydrateCrmRoles {
		crmSvc := db.NewCRMRoleService()
		ids := make([]string, len(crmRoleMap))
		i := 0
		for id := range crmRoleMap {
			ids[i] = id
			i++
		}
		crmRoles, err := crmSvc.GetByIDs(spanCtx, in.TenantId, ids...)
		if err != nil {
			err := errors.Wrap(err, "error getting person crm roles")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		for _, crmRole := range crmRoles {
			crmRoleProto, err := crmSvc.ToProto(crmRole)
			if err != nil {
				err := errors.Wrap(err, "error converting person crm role to proto")
				logger.Error(err)
				return nil, err.AsGRPC()
			}
			personIdx := crmRoleMap[crmRole.ID]
			people[personIdx].CrmRoles = append(people[personIdx].CrmRoles, crmRoleProto)
		}
	}

	if in.HydrateRoles {
		// TODO: Hydrate crm_roles
	}

	return &servicePb.SearchPeopleResponse{
		People: people,
		Total:  int32(total),
	}, nil
}

func (server *OrchardGRPCServer) GetGroupMembers(ctx context.Context, in *servicePb.GetGroupMembersRequest) (*servicePb.GetGroupMembersResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupMembers")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewPersonService()

	peeps, err := svc.GetPeopleByGroupId(spanCtx, in.TenantId, in.GroupId)
	if err != nil {
		err := errors.Wrap(err, "error getting person records by group id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	people := make([]*orchardPb.Person, len(peeps))
	for i, peep := range peeps {
		p, err := svc.ToProto(peep)
		if err != nil {
			err := errors.Wrap(err, "error converting person db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		people[i] = p
	}

	return &servicePb.GetGroupMembersResponse{
		GroupId: in.GroupId,
		Members: people,
	}, nil
}

func (server *OrchardGRPCServer) GetUngroupedPeople(ctx context.Context, in *servicePb.GetUngroupedPeopleRequest) (*servicePb.GetUngroupedPeopleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupMembers")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewPersonService()

	peeps, err := svc.GetPeopleByGroupId(spanCtx, in.TenantId, "")
	if err != nil {
		err := errors.Wrap(err, "error getting ungrouped person records")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	people := make([]*orchardPb.Person, len(peeps))
	for i, peep := range peeps {
		p, err := svc.ToProto(peep)
		if err != nil {
			err := errors.Wrap(err, "error converting person db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		people[i] = p
	}

	return &servicePb.GetUngroupedPeopleResponse{
		People: people,
	}, nil
}

func (server *OrchardGRPCServer) UpdatePerson(ctx context.Context, in *servicePb.UpdatePersonRequest) (*servicePb.UpdatePersonResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdatePerson")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Person == nil {
		return &servicePb.UpdatePersonResponse{}, nil
	}

	logger = logger.WithCustom("personId", in.Person.Id)

	svc := db.NewPersonService()

	updatePerson := svc.FromProto(in.Person)

	if err := svc.Update(spanCtx, updatePerson, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating person record")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	person, err := svc.ToProto(updatePerson)
	if err != nil {
		err := errors.Wrap(err, "error converting person db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdatePersonResponse{
		Person: person,
	}, nil
}

func (server *OrchardGRPCServer) DeletePersonById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeletePersonById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = "00000000-0000-0000-0000-000000000000"
	}

	svc := db.NewPersonService()

	if err := svc.SoftDeleteByID(spanCtx, in.PersonId, in.TenantId, in.UserId); err != nil {
		logger.Errorf("error deleting person by id: %s", err.Error())
		return nil, err
	}

	return &servicePb.Empty{}, nil
}
