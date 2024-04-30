package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"strings"
	"time"

	strUtil "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/clients"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	bouncerPb "github.com/loupe-co/protos/src/services/bouncer"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"google.golang.org/grpc/codes"
)

const (
	playerCoachValue = "outreach_playercoach"
)

func (h *Handlers) CreatePerson(ctx context.Context, in *servicePb.CreatePersonRequest) (*servicePb.CreatePersonResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

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

	svc := h.db.NewPersonService()

	insertablePerson := svc.FromProto(in.Person)
	insertablePerson.CreatedAt = time.Now().UTC()
	insertablePerson.UpdatedAt = time.Now().UTC()

	srSVC := h.db.NewSystemRoleService()
	srM, err := srSVC.GetInternalRoleIDs(ctx)
	if err != nil {
		err := errors.Wrap(err, "error getting internal roles")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	rIDs := []string{}
	for _, id := range in.Person.RoleIds {
		if _, ok := srM[id]; !ok {
			rIDs = append(rIDs, id)
		}
	}
	in.Person.RoleIds = rIDs

	// Check if email already exists
	existingPerson, err := svc.GetByEmail(ctx, in.TenantId, in.Person.Email)
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
	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction for creating person")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc.SetTransaction(tx)

	// Perform insert in db
	if err := svc.Insert(ctx, insertablePerson); err != nil {
		err := errors.Wrap(err, "error inserting person in sql")
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

	// clear transaction
	svc.SetTransaction(nil)

	if _, err := updateUserProvisioning(ctx, in.GetTenantId(), "", in.GetPerson().GetEmail(), svc, h.auth0Client); err != nil {
		err := errors.Wrap(err, "error provisioning")
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

func (h *Handlers) UpsertPeople(ctx context.Context, in *servicePb.UpsertPeopleRequest) (*servicePb.UpsertPeopleResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if len(in.People) == 0 {
		return &servicePb.UpsertPeopleResponse{}, nil
	}

	svc := h.db.NewPersonService()

	srSVC := h.db.NewSystemRoleService()
	srM, err := srSVC.GetInternalRoleIDs(ctx)
	if err != nil {
		err := errors.Wrap(err, "error getting internal roles")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	for _, p := range in.People {
		rIDs := []string{}
		for _, id := range p.RoleIds {
			if _, ok := srM[id]; !ok {
				rIDs = append(rIDs, id)
			}
		}
		p.RoleIds = rIDs
	}

	upsertablePeople := make([]*models.Person, len(in.People))
	for i, p := range in.People {
		upsertablePeople[i] = svc.FromProto(p)
	}

	if err := svc.UpsertAll(ctx, upsertablePeople); err != nil {
		err := errors.Wrap(err, "error upserting one or more person records")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpsertPeopleResponse{}, nil
}

func (h *Handlers) GetPersonById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Person, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	p, err := svc.GetByID(ctx, in.PersonId, in.TenantId)
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

func (h *Handlers) SearchPeople(ctx context.Context, in *servicePb.SearchPeopleRequest) (*servicePb.SearchPeopleResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("search", in.Search).WithCustom("page", in.Page).WithCustom("pageSize", in.PageSize)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	limit := 20
	if in.PageSize > 0 {
		limit = int(in.PageSize)
	}
	if in.PageSize == -1 {
		limit = math.MaxInt32
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

	svc := h.db.NewPersonService()

	peeps, total, err := svc.Search(ctx, in.TenantId, in.Search, limit, offset, dbFilters...)
	if err != nil {
		err := errors.Wrap(err, "error searching people")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	gvSvc := h.db.NewGroupViewerService()

	peepIds := make([]string, len(peeps))
	for i, peep := range peeps {
		peepIds[i] = peep.ID
	}

	logger.WithCustom("peepIds", peepIds).Debug("search people peep ids")

	peepsViewableGroups, err := gvSvc.GetPersonsViewableGroups(ctx, in.TenantId, peepIds...)
	if err != nil {
		err := errors.Wrap(err, "error querrying group viewer db in people search")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	peepGroupIds := map[string][]string{}
	for _, peepViewableGroup := range peepsViewableGroups {
		if _, ok := peepGroupIds[peepViewableGroup.PersonID]; !ok {
			peepGroupIds[peepViewableGroup.PersonID] = []string{}
		}
		peepGroupIds[peepViewableGroup.PersonID] = append(peepGroupIds[peepViewableGroup.PersonID], peepViewableGroup.GroupID)
	}

	crmRoleMap := map[string][]int{}
	systemRoleMap := map[string][]int{}
	people := make([]*orchardPb.Person, len(peeps))
	for i, peep := range peeps {
		p, err := svc.ToProto(peep)
		if err != nil {
			err := errors.Wrap(err, "error converting person db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		peepIds[i] = peep.ID
		if groupIds, ok := peepGroupIds[peep.ID]; ok {
			p.GroupViewerIds = groupIds
		}
		people[i] = p
		for _, crmRoleID := range p.CrmRoleIds {
			if _, ok := crmRoleMap[crmRoleID]; !ok {
				crmRoleMap[crmRoleID] = []int{}
			}
			crmRoleMap[crmRoleID] = append(crmRoleMap[crmRoleID], i)
		}
		for _, roleID := range p.RoleIds {
			if _, ok := systemRoleMap[roleID]; !ok {
				systemRoleMap[roleID] = []int{}
			}
			systemRoleMap[roleID] = append(systemRoleMap[roleID], i)
		}
	}

	if in.HydrateCrmRoles {
		crmSvc := h.db.NewCRMRoleService()
		ids := make([]string, len(crmRoleMap))
		i := 0
		for id := range crmRoleMap {
			ids[i] = id
			i++
		}
		crmRoles, err := crmSvc.GetByIDs(ctx, in.TenantId, in.IsOutreach, ids...)
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
			if personIdxs, ok := crmRoleMap[crmRole.ID]; ok {
				for _, personIdx := range personIdxs {
					people[personIdx].CrmRoles = append(people[personIdx].CrmRoles, crmRoleProto)
				}
			}
		}
	}

	if in.HydrateRoles {
		sysRoleSvc := h.db.NewSystemRoleService()
		ids := make([]string, len(systemRoleMap))
		i := 0
		for id := range systemRoleMap {
			ids[i] = id
			i++
		}
		systemRoles, err := sysRoleSvc.GetByIDs(ctx, ids...)
		if err != nil {
			err := errors.Wrap(err, "error getting person system roles")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		for _, sysRole := range systemRoles {
			sysRoleProto, err := sysRoleSvc.ToProto(sysRole)
			if err != nil {
				err := errors.Wrap(err, "error converting person system role to proto")
				logger.Error(err)
				return nil, err.AsGRPC()
			}
			if personIdxs, ok := systemRoleMap[sysRole.ID]; ok {
				for _, personIdx := range personIdxs {
					people[personIdx].Roles = append(people[personIdx].Roles, sysRoleProto)
				}
			}
		}
	}

	return &servicePb.SearchPeopleResponse{
		People: people,
		Total:  int32(total),
	}, nil
}

func (h *Handlers) GetGroupMembers(ctx context.Context, in *servicePb.GetGroupMembersRequest) (*servicePb.GetGroupMembersResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	peeps, err := svc.GetPeopleByGroupId(ctx, in.TenantId, in.GroupId)
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

func (h *Handlers) GetUngroupedPeople(ctx context.Context, in *servicePb.GetUngroupedPeopleRequest) (*servicePb.GetUngroupedPeopleResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	peeps, err := svc.GetPeopleByGroupId(ctx, in.TenantId, "")
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

func (h *Handlers) GetVirtualUsers(ctx context.Context, in *servicePb.GetVirtualUsersRequest) (*servicePb.GetVirtualUsersResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	lastFullDataSync, err := h.tenantClient.GetTenantLastFullDataSync(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting tenant last full data sync")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	peeps, err := svc.GetVirtualUsers(ctx, in.TenantId, lastFullDataSync)
	if err != nil {
		err := errors.Wrap(err, "error getting virtual user records")
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

	return &servicePb.GetVirtualUsersResponse{
		People: people,
	}, nil
}

func (h *Handlers) UpdatePerson(ctx context.Context, in *servicePb.UpdatePersonRequest) (*servicePb.UpdatePersonResponse, error) {
	ctx, span := log.StartSpan(ctx, "UpdatePerson")
	defer span.End()

	logger := log.WithContext(ctx).
		WithTenantID(in.TenantId).
		WithCustom("onlyFields", in.OnlyFields).
		WithCustom("person", in.Person).
		WithCustom("personId", in.PersonId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Person == nil {
		err := ErrBadRequest.New("person can't be null")
		logger.Warn(err.Error())
		return nil, err
	}

	if in.Person.Id == "" && in.PersonId == "" {
		err := ErrBadRequest.New("person.ID can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}
	// If a top-level personId was given but no id on the person record, then add it to person record id.
	// Otherwise then we're good as we don't actually use the top level id.
	if in.Person.Id == "" && in.PersonId != "" {
		in.Person.Id = in.PersonId
	}
	if in.Person.UpdatedBy == "" {
		in.Person.UpdatedBy = db.DefaultTenantID
	}

	svc := h.db.NewPersonService()
	p, err := svc.GetByID(ctx, in.GetPerson().GetId(), in.GetTenantId())
	if err != nil {
		err := errors.Wrap(err, "error getting person record by Id in update person")
		logger.Error(err)

		return nil, err.AsGRPC()
	}

	existingPerson, err := svc.ToProto(p)
	if err != nil {
		err := errors.Wrap(err, "error converting person db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Check if we are updating a person's provisioning
	changeProvisioning := strUtil.Strings(in.OnlyFields).Has("is_provisioned")

	// Check if we're updating a person's system_roles
	changeRoles := strUtil.Strings(in.OnlyFields).Has("role_ids") || len(in.OnlyFields) == 0

	srSVC := h.db.NewSystemRoleService()
	srM, err := srSVC.GetInternalRoleIDs(ctx)
	if err != nil {
		err := errors.Wrap(err, "error getting internal roles")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if changeRoles {
		rIDs := []string{}
		for _, id := range in.Person.RoleIds {
			if _, ok := srM[id]; !ok {
				rIDs = append(rIDs, id)
			}
		}
		in.Person.RoleIds = rIDs
	}

	// Check if groupId changed
	changeGroup := strUtil.Strings(in.OnlyFields).Has("group_id")
	if changeGroup {
		in.Person.IsSynced = false
		if len(in.OnlyFields) > 0 {
			in.OnlyFields = append(in.OnlyFields, "is_synced")
		}

		if in.GetPerson().GetGroupId() != existingPerson.GetGroupId() &&
			existingPerson.GetManagerId() == playerCoachValue {
			groupService := h.db.NewGroupService()
			g, err := groupService.GetByID(ctx, in.GetPerson().GetGroupId(), in.GetTenantId())
			if err != nil {
				err := errors.Wrap(err, "error getting group record by Id in update person")
				logger.Error(err)

				return nil, err.AsGRPC()
			}

			if g != nil {
				group, err := groupService.ToProto(g)
				if err != nil {
					err := errors.Wrap(err, "error converting group db model to proto")
					logger.Error(err)
					return nil, err.AsGRPC()
				}

				if group.GetType() != orchardPb.SystemRoleType_IC ||
					strings.ToUpper(existingPerson.GetType()) != orchardPb.SystemRoleType_IC.String() {
					if !strUtil.Strings(in.OnlyFields).Has("manager_id") {
						in.OnlyFields = append(in.OnlyFields, "manager_id")
					}

					in.Person.ManagerId = ""
				}
			}
		}
	}

	if in.Person.CreatedBy != "" {
		// Check if this is virtual user that is no longer provisioned: -> status=inactive
		if !in.Person.IsProvisioned && in.Person.CreatedBy != db.DefaultTenantID && in.Person.CreatedBy != db.DefaultOutreachSyncID {
			in.Person.Status = orchardPb.BasicStatus_Inactive
			if len(in.OnlyFields) > 0 {
				in.OnlyFields = append(in.OnlyFields, "status")
			}
		}

		// If virtual user and provisioning, set to active
		if in.Person.IsProvisioned && in.Person.CreatedBy != db.DefaultTenantID && in.Person.CreatedBy != db.DefaultOutreachSyncID {
			in.Person.Status = orchardPb.BasicStatus_Active
			if len(in.OnlyFields) > 0 {
				in.OnlyFields = append(in.OnlyFields, "status")
			}
		}
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating update person transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc.SetTransaction(tx)

	updatePerson := svc.FromProto(in.Person)

	// Update person in sql
	if err := svc.Update(ctx, updatePerson, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating person record")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
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

	// clear transaction
	svc.SetTransaction(nil)

	// If we changed the provisioning of the person, update in Auth0
	// Due to now being able to update inactive users, need to make sure empty email don't get through here and cause issues
	if changeProvisioning && in.Person.Email != "" {
		if _, err := updateUserProvisioning(ctx, updatePerson.TenantID, updatePerson.ID, in.Person.Email, svc, h.auth0Client); err != nil {
			err := errors.Wrap(err, "error provisioning")
			logger.Error(err)
			if err := svc.Rollback(); err != nil {
				logger.Error(errors.Wrap(err, "error rolling back transaction"))
			}
			return nil, err.AsGRPC()
		}
	}

	// If we updated the user's system roles or group, then bust their auth cache in bouncer
	if changeRoles || changeGroup {
		if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: updatePerson.ID}); err != nil {
			err := errors.Wrap(err, "error busting auth data cache for user")
			logger.Error(err)
			if err := svc.Rollback(); err != nil {
				logger.Error(errors.Wrap(err, "error rolling back transaction"))
			}
			return nil, err.AsGRPC()
		}
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

func (h *Handlers) UpdatePersonGroups(ctx context.Context, in *servicePb.UpdatePersonGroupsRequest) (*servicePb.UpdatePersonGroupsResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if err := h.updatePersonGroups(ctx, in.TenantId, nil); err != nil {
		err := errors.Wrap(err, "error updating person groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdatePersonGroupsResponse{}, nil
}

func (h *Handlers) updatePersonGroups(ctx context.Context, tenantID string, tx *sql.Tx) error {
	spanCtx, span := log.StartSpan(ctx, "updatePersonGroups")
	defer span.End()

	personSvc := h.db.NewPersonService()
	if tx != nil {
		personSvc.SetTransaction(tx)
	}

	personGroups, err := personSvc.GetPersonGroupIDs(spanCtx, tenantID)
	if err != nil {
		return errors.Wrap(err, "error getting current person groups")
	}

	if err := personSvc.UpdatePersonGroups(spanCtx, tenantID); err != nil {
		return errors.Wrap(err, "error updating person groups")
	}

	updatedPersonGroups, err := personSvc.GetPersonGroupIDs(spanCtx, tenantID)
	if err != nil {
		return errors.Wrap(err, "error getting updated person groups")
	}

	bustReqs := []*bouncerPb.BustAuthCacheRequest{}
	for personID, groupID := range personGroups {
		if newGroupID, ok := updatedPersonGroups[personID]; newGroupID != groupID || !ok {
			bustReqs = append(bustReqs, &bouncerPb.BustAuthCacheRequest{TenantId: tenantID, UserId: personID})
		}
	}

	if len(bustReqs) > 0 {
		if err := h.bouncerClient.MultiBustAuthCache(spanCtx, bustReqs...); err != nil {
			return errors.Wrap(err, "error busting auth cache for one or more users in bouncer")
		}
	}

	return nil
}

func (h *Handlers) DeletePersonById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating delete person transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()
	svc.SetTransaction(tx)

	if err := svc.SoftDeleteByID(ctx, in.PersonId, in.TenantId, in.UserId); err != nil {
		logger.Errorf("error deleting person by id: %s", err.Error())
		return nil, err
	}

	if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.PersonId}); err != nil {
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

func (h *Handlers) ClonePerson(ctx context.Context, in *servicePb.ClonePersonRequest) (*servicePb.ClonePersonResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.GetCurrentTenantId()).WithCustom("personId", in.GetPersonId())

	if in.GetCurrentTenantId() == "" || in.GetPersonId() == "" || in.GetNewTenantId() == "" {
		err := ErrBadRequest.New("currentTenantId, personId, newTenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	p, err := svc.GetByID(ctx, in.GetPersonId(), in.GetCurrentTenantId())
	if err != nil {
		err := errors.Wrap(err, "error getting person by id")
		if err == sql.ErrNoRows {
			err = err.WithCode(codes.NotFound)
		}

		logger.Error(err)
		return nil, err.AsGRPC()
	}

	p.TenantID = in.GetNewTenantId()
	p.IsProvisioned = true
	p.CreatedAt = time.Now().UTC()
	p.UpdatedAt = time.Now().UTC()

	// Check if email already exists
	existingPerson, err := svc.GetByEmail(ctx, in.GetNewTenantId(), p.Email.String)
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
	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction for cloning person")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc.SetTransaction(tx)

	// Perform insert in db
	if err := svc.Insert(ctx, p); err != nil {
		err := errors.Wrap(err, "error inserting person in sql")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Commit clone person transaction
	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting clone person transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// clear transaction
	svc.SetTransaction(nil)

	if _, err := updateUserProvisioning(ctx, in.GetNewTenantId(), p.ID, p.Email.String, svc, h.auth0Client); err != nil {
		err := errors.Wrap(err, "error provisioning")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// Convert updated person model back to proto for response
	createdRes, err := svc.ToProto(p)
	if err != nil {
		err := errors.Wrap(err, "error converting cloned person to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.ClonePersonResponse{Person: createdRes}, nil
}

func (h *Handlers) HardDeletePersonById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating delete person transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()
	svc.SetTransaction(tx)

	var personEmail string
	person, err := svc.GetByID(ctx, in.PersonId, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting person")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	personEmail = person.Email.String

	if err := svc.DeleteByID(ctx, in.PersonId, in.TenantId); err != nil {
		err := errors.Wrap(err, "error deleting person by id")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.PersonId}); err != nil {
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

	// clear transaction
	svc.SetTransaction(nil)

	// update auth0 (remove user if no additional records, otherwise update existing user)
	if person.IsProvisioned {
		if _, err := updateUserProvisioning(ctx, in.GetTenantId(), in.GetPersonId(), personEmail, svc, h.auth0Client); err != nil {
			err := errors.Wrap(err, "error provisioning")
			logger.Error(err)
			if err := svc.Rollback(); err != nil {
				logger.Error(errors.Wrap(err, "error rolling back transaction"))
			}
			return nil, err.AsGRPC()
		}
	}

	return &servicePb.Empty{}, nil
}

func (h *Handlers) ConvertVirtualUsers(ctx context.Context, in *servicePb.ConvertVirtualUsersRequest) (*servicePb.ConvertVirtualUsersResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.GetTenantId())

	if in.GetTenantId() == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	// get virtual users for tenant
	peeps, err := svc.GetNonOutreachSyncedVirtualUsers(ctx, in.GetTenantId(), nil)
	if err != nil {
		err := errors.Wrap(err, "error getting virtual user records")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// grab the email addresses
	emails := make([]interface{}, 0, len(peeps))
	for _, person := range peeps {
		if person.Email.Valid && !person.Email.IsZero() && person.Status == "active" {
			emails = append(emails, strings.ToLower(person.Email.String))
		}
	}

	// no emails, so return
	if len(emails) == 0 {
		return &servicePb.ConvertVirtualUsersResponse{}, nil
	}

	// get non-virtual users using above emails to see if there are any matches
	nonVirtualPeeps, err := svc.GetAllActiveNonVirtualByEmails(ctx, in.GetTenantId(), emails...)
	if err != nil {
		err := errors.Wrap(err, "error getting non virtual user records")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// no matches get out
	if len(nonVirtualPeeps) == 0 {
		return &servicePb.ConvertVirtualUsersResponse{}, nil
	}

	// start a transaction
	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating convert person transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc.SetTransaction(tx)

	updatedPeeps := []*orchardPb.Person{}
	oldPeepsIDMap := make(map[string]string)
	for _, oldPerson := range peeps {
		for _, newPerson := range nonVirtualPeeps {
			if !strings.EqualFold(oldPerson.Email.String, newPerson.Email.String) {
				continue
			}
			// store old id for cache changes later
			oldPeepsIDMap[oldPerson.Email.String] = oldPerson.ID

			// update the new person with roles & groupids
			newPerson.RoleIds = oldPerson.RoleIds
			newPerson.IsProvisioned = true
			_, err := newPerson.Update(ctx, svc.GetContextExecutor(), boil.Whitelist("role_ids", "group_id", "is_provisioned"))
			if err != nil {
				err := errors.Wrap(err, "error updating new person")
				logger.Error(err)
				if err := svc.Rollback(); err != nil {
					logger.Error(errors.Wrap(err, "error rolling back transaction"))
				}
				return nil, err.AsGRPC()
			}

			// deactivate old person
			oldPerson.Status = "inactive"
			oldPerson.IsProvisioned = false
			_, err = oldPerson.Update(ctx, svc.GetContextExecutor(), boil.Whitelist("status", "is_provisioned"))
			if err != nil {
				err := errors.Wrap(err, "error updating old person")
				logger.Error(err)
				if err := svc.Rollback(); err != nil {
					logger.Error(errors.Wrap(err, "error rolling back transaction"))
				}
				return nil, err.AsGRPC()
			}

			newP, err := svc.ToProto(newPerson)
			if err == nil {
				updatedPeeps = append(updatedPeeps, newP)
			}
			break
		}
	}

	// commit the transaction
	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting convert person transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	// clear transaction
	svc.SetTransaction(nil)

	for _, newPerson := range updatedPeeps {
		// provision new user
		if _, err := updateUserProvisioning(ctx, newPerson.TenantId, newPerson.Id, newPerson.Email, svc, h.auth0Client); err != nil {
			err := errors.Wrap(err, "error provisioning")
			logger.Error(err)
			return nil, err.AsGRPC()
		}

		// go to next if old id not found
		if _, ok := oldPeepsIDMap[newPerson.Email]; !ok {
			continue
		}

		// bust auth cache for old id
		if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: oldPeepsIDMap[newPerson.Email]}); err != nil {
			err := errors.Wrap(err, "error busting auth data cache for user")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	return &servicePb.ConvertVirtualUsersResponse{
		People: updatedPeeps,
	}, nil
}

func (h *Handlers) ReprovisionPeople(ctx context.Context, in *servicePb.IdRequest) (*servicePb.ReprovisionPeopleResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.GetTenantId())

	if in.GetTenantId() == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewPersonService()

	numProvisioned := int64(0)
	numUnprovisioned := int64(0)

	if in.GetPersonId() != "" {
		provisioned, err := updateUserProvisioning(ctx, in.GetTenantId(), in.GetPersonId(), "", svc, h.auth0Client)
		if err != nil {
			err = errors.Wrap(err, "error reprovisioning person")
			return nil, err
		}

		if provisioned {
			numProvisioned++
		} else {
			numUnprovisioned++
		}

		return &servicePb.ReprovisionPeopleResponse{
			ProvisionedCount:   numProvisioned,
			UnprovisionedCount: numUnprovisioned,
		}, nil
	}

	people, _, err := svc.Search(ctx, in.GetTenantId(), "", 10000, 0,
		db.PersonFilter{Field: "is_provisioned", Op: "EQ", Values: []interface{}{true}},
		db.PersonFilter{Field: "status", Op: "EQ", Values: []interface{}{"active"}},
	)
	if err != nil {
		err = errors.Wrap(err, "error getting people to reprovision")
		return nil, err
	}

	for _, person := range people {
		provisioned, err := updateUserProvisioning(ctx, in.GetTenantId(), person.ID, person.Email.String, svc, h.auth0Client)
		if err != nil {
			err := errors.Wrap(err, "error provisioning person")
			logger.Error(err)
			return nil, err.AsGRPC()
		}

		if provisioned {
			numProvisioned++
		} else {
			numUnprovisioned++
		}
	}

	return &servicePb.ReprovisionPeopleResponse{
		ProvisionedCount:   numProvisioned,
		UnprovisionedCount: numUnprovisioned,
	}, nil
}

func (h *Handlers) GetPeopleByEmail(ctx context.Context, in *servicePb.GetPeopleByEmailRequest) (*servicePb.GetPeopleByEmailResponse, error) {
	logger := log.WithContext(ctx).WithCustom("email", in.EmailAddress)

	svc := h.db.NewPersonService()

	people, err := svc.GetAllByEmail(ctx, in.EmailAddress)
	if err != nil {
		err := errors.Wrap(err, "error getting person by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if people == nil {
		return &servicePb.GetPeopleByEmailResponse{}, nil
	}

	finalRes := make([]*orchardPb.Person, len(people))
	for i, person := range people {
		pp, err := svc.ToProto(person)
		if err != nil {
			err := errors.Wrap(err, "error converting person db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}

		finalRes[i] = pp
	}

	return &servicePb.GetPeopleByEmailResponse{People: finalRes}, nil
}

func updateUserProvisioning(ctx context.Context, tenantID string, personID string, personEmail string, personSvc *db.PersonService, auth0Client *clients.Auth0Client) (bool, error) {
	if len(tenantID) == 0 {
		return false, errors.New("tenantId is required to provision")
	}

	personEmail = strings.TrimSpace(personEmail)

	if len(personEmail) == 0 && len(personID) == 0 {
		return false, errors.New("id or email is required to provision")
	}

	if len(personEmail) == 0 && len(personID) > 0 {
		person, err := personSvc.GetByID(ctx, personID, tenantID)
		if err != nil {
			err := errors.Wrap(err, "error getting person record for provisioning")
			return false, err
		}

		// if no email we cant provision
		if person.Email.IsZero() {
			return false, nil
		}

		personEmail = person.Email.String
	}

	personRecords, err := personSvc.GetAllByEmailForProvisioning(ctx, personEmail)
	if err != nil {
		err := errors.Wrap(err, "error getting person records for provisioning")
		return false, err
	}

	// found person records to provision, make it happen
	if len(personRecords) > 0 {
		if err := auth0Client.Provision(ctx, personRecords); err != nil {
			err := errors.Wrap(err, "error provisioning user in auth0")
			return false, err
		}
		return true, nil
	}

	// no person records, unprovision
	if len(personID) > 0 {
		if err := auth0Client.Unprovision(ctx, tenantID, personID); err != nil {
			var ignoreError bool
			// if not found in auth0, dont return an error
			if cErr, ok := err.(errors.CommonError); ok && cErr.Code == codes.NotFound {
				ignoreError = true
			}

			if !ignoreError {
				err := errors.Wrap(err, "error unprovisioning user in auth0")
				return false, err
			}
		}
	}

	return false, nil
}

func (h *Handlers) GetOutreachUserCommitMappings(ctx context.Context, in *servicePb.GetOutreachUserCommitMappingsRequest) (*servicePb.GetOutreachUserCommitMappingsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetOutreachUserCommitMappings")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" || in.CommitId == "" {
		err := ErrBadRequest.New("tenantId and commitId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	personService := h.db.NewPersonService()

	outreachReturnIDS, err := personService.GetOutreachIdsFromCommitIds(spanCtx, in.TenantId, in.CommitId)
	if err != nil {
		err := errors.Wrap(err, "error mapping people ids")
		logger.Error(err)
		return nil, err
	}

	return &servicePb.GetOutreachUserCommitMappingsResponse{
		OutreachIds: outreachReturnIDS,
	}, nil
}
