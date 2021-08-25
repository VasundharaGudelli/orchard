package grpchandlers

import (
	"context"
	"encoding/json"
	"time"

	strUtil "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	bouncerPb "github.com/loupe-co/protos/src/services/bouncer"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc/codes"
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
	in.Person.IsSynced = false
	in.Person.IsProvisioned = true
	in.Person.Status = orchardPb.BasicStatus_Active

	svc := db.NewPersonService()

	insertablePerson := svc.FromProto(in.Person)
	insertablePerson.CreatedAt = time.Now().UTC()
	insertablePerson.UpdatedAt = time.Now().UTC()

	// Check if email already exists
	existingPerson, err := svc.GetByEmail(spanCtx, in.TenantId, in.Person.Email)
	if err != nil {
		err := errors.Wrap(err, "error checking for existing person by email")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	if existingPerson != nil {
		err := errors.New("can't insert person with given email").WithCode(codes.AlreadyExists)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	// Get transaction, so we can rollback if user provisioning fails
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error creating transaction for creating person")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Perform insert in db
	if err := svc.Insert(spanCtx, insertablePerson); err != nil {
		err := errors.Wrap(err, "error inserting person in sql")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Provision user in Auth0
	if err := server.auth0Client.Provision(spanCtx, in.TenantId, insertablePerson); err != nil {
		err := errors.Wrap(err, "error provisioning user in auth0")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Commit create person transaction
	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting create person transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Convert updated person model back to proto for response
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

	// Parse generic person filters for db
	dbFilters := make([]db.PersonFilter, len(in.Filters))
	for i, f := range in.Filters {
		vals := make([]interface{}, len(f.Values))
		for j, raw := range f.Values {
			if err := json.Unmarshal(raw, &vals[j]); err != nil {
				err := errors.Wrap(err, "error unmarshaling person filter value")
				logger.Error(err)
				return nil, err
			}
		}
		var field string
		switch f.Field {
		case orchardPb.PersonField_Id:
			field = "id"
		case orchardPb.PersonField_TenantId:
			field = "tenant_id"
		case orchardPb.PersonField_Name:
			field = "name"
		case orchardPb.PersonField_FirstName:
			field = "first_name"
		case orchardPb.PersonField_LastName:
			field = "last_name"
		case orchardPb.PersonField_Email:
			field = "email"
		case orchardPb.PersonField_ManagerId:
			field = "manager_id"
		case orchardPb.PersonField_GroupId:
			field = "group_id"
		case orchardPb.PersonField_RoleIds:
			field = "role_ids"
		case orchardPb.PersonField_CrmRoleIds:
			field = "crm_role_ids"
		case orchardPb.PersonField_IsProvisioned:
			field = "is_provisioned"
		case orchardPb.PersonField_IsSynced:
			field = "is_synced"
		case orchardPb.PersonField_Status:
			field = "status"
		case orchardPb.PersonField_CreatedAt:
			field = "created_at"
		case orchardPb.PersonField_CreatedBy:
			field = "created_by"
		case orchardPb.PersonField_UpdatedAt:
			field = "updated_at"
		case orchardPb.PersonField_UpdatedBy:
			field = "updated_by"
		}
		dbFilters[i] = db.PersonFilter{
			Field:  field,
			Op:     f.Op.String(),
			Values: vals,
		}
	}

	svc := db.NewPersonService()

	peeps, total, err := svc.Search(spanCtx, in.TenantId, in.Search, limit, offset, dbFilters...)
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

	if in.Person.UpdatedBy == "" {
		in.Person.UpdatedBy = db.DefaultTenantID
	}

	logger = logger.WithCustom("personId", in.Person.Id)

	// Check if we are updating a person's provisioning
	changeProvisioning := strUtil.Strings(in.OnlyFields).Has("is_provisioned")

	// Check if we're updating a person's system_roles
	changeRoles := strUtil.Strings(in.OnlyFields).Has("role_ids") || len(in.OnlyFields) == 0

	// Check if groupId changed
	changeGroup := strUtil.Strings(in.OnlyFields).Has("group_id")
	if changeGroup {
		in.Person.IsSynced = false
	}

	svc := db.NewPersonService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error creating update person transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	updatePerson := svc.FromProto(in.Person)

	// Update person in sql
	if err := svc.Update(spanCtx, updatePerson, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating person record")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// If we changed the provisioning of the person, update in Auth0
	if changeProvisioning {
		if updatePerson.IsProvisioned {
			if err := server.auth0Client.Provision(spanCtx, in.TenantId, updatePerson); err != nil {
				err := errors.Wrap(err, "error provisioning user in auth0")
				logger.Error(err)
				if err := svc.Rollback(); err != nil {
					logger.Error(errors.Wrap(err, "error rolling back transaction"))
				}
				return nil, err.AsGRPC()
			}
		} else {
			if err := server.auth0Client.Unprovision(spanCtx, in.TenantId, updatePerson.ID); err != nil {
				err := errors.Wrap(err, "error unprovisioning user in auth0")
				logger.Error(err)
				if err := svc.Rollback(); err != nil {
					logger.Error(errors.Wrap(err, "error rolling back transaction"))
				}
				return nil, err.AsGRPC()
			}
		}
	}

	// If we updated the user's system roles, then bust their auth cache in bouncer
	if changeRoles {
		if _, err := server.bouncerClient.BustAuthCache(spanCtx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.PersonId}); err != nil {
			err := errors.Wrap(err, "error busting auth data cache for user")
			logger.Error(err)
			if err := svc.Rollback(); err != nil {
				logger.Error(errors.Wrap(err, "error rolling back transaction"))
			}
			return nil, err.AsGRPC()
		}
	}

	// Commit the update person transaction in sql
	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting update person transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Convert the updated person db model to proto for response
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
		in.UserId = db.DefaultTenantID
	}

	svc := db.NewPersonService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error creating delete person transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.SoftDeleteByID(spanCtx, in.PersonId, in.TenantId, in.UserId); err != nil {
		logger.Errorf("error deleting person by id: %s", err.Error())
		return nil, err
	}

	if _, err := server.bouncerClient.BustAuthCache(spanCtx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.UserId}); err != nil {
		err := errors.Wrap(err, "error busting auth data cache for user")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Commit the update person transaction in sql
	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting delete person transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
