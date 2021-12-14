package grpc

import (
	"context"

	"github.com/loupe-co/bouncer/pkg/client"
	"github.com/loupe-co/orchard/internal/clients"
	"github.com/loupe-co/orchard/internal/config"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/handlers"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

type OrchardGRPCServer struct {
	cfg      config.Config
	db       *db.DB
	handlers *handlers.Handlers
}

func New(cfg config.Config, dbClient *db.DB, tenantClient *clients.TenantClient, crmClient *clients.CRMClient, auth0Client *clients.Auth0Client, bouncerClient *client.BouncerClient) *OrchardGRPCServer {
	h := handlers.New(cfg, dbClient, tenantClient, crmClient, auth0Client, bouncerClient)
	return &OrchardGRPCServer{
		cfg:      cfg,
		db:       dbClient,
		handlers: h,
	}
}

func (server *OrchardGRPCServer) GetUserTeam(ctx context.Context, in *servicePb.GetUserTeamRequest) (*servicePb.GetUserTeamResponse, error) {
	return server.handlers.GetUserTeam(ctx, in)
}

func (server *OrchardGRPCServer) Sync(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return server.handlers.Sync(ctx, in)
}

func (server *OrchardGRPCServer) SyncUsers(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return server.handlers.SyncUsers(ctx, in)
}

func (server *OrchardGRPCServer) SyncGroups(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return server.handlers.SyncGroups(ctx, in)
}

func (server *OrchardGRPCServer) SyncCrmRoles(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return server.handlers.SyncCrmRoles(ctx, in)
}

func (server *OrchardGRPCServer) IsHierarchySynced(ctx context.Context, in *servicePb.IsHierarchySyncedRequest) (*servicePb.IsHierarchySyncedResponse, error) {
	return server.handlers.IsHierarchySynced(ctx, in)
}

func (server *OrchardGRPCServer) ResetHierarchy(ctx context.Context, in *servicePb.ResetHierarchyRequest) (*servicePb.ResetHierarchyResponse, error) {
	return server.handlers.ResetHierarchy(ctx, in)
}

func (server *OrchardGRPCServer) GetGroupSyncSettings(ctx context.Context, in *servicePb.GetGroupSyncSettingsRequest) (*servicePb.GetGroupSyncSettingsResponse, error) {
	return server.handlers.GetGroupSyncSettings(ctx, in)
}

func (server *OrchardGRPCServer) ReSyncCRM(ctx context.Context, in *servicePb.ReSyncCRMRequest) (*servicePb.ReSyncCRMResponse, error) {
	return server.handlers.ReSyncCRM(ctx, in)
}

func (server *OrchardGRPCServer) UpdateGroupSyncState(ctx context.Context, in *servicePb.UpdateGroupSyncStateRequest) (*servicePb.UpdateGroupSyncStateResponse, error) {
	return server.handlers.UpdateGroupSyncState(ctx, in)
}

func (server *OrchardGRPCServer) UpdateGroupSyncMetadata(ctx context.Context, in *servicePb.UpdateGroupSyncMetadataRequest) (*servicePb.UpdateGroupSyncMetadataResponse, error) {
	return server.handlers.UpdateGroupSyncMetadata(ctx, in)
}

func (server *OrchardGRPCServer) GetLegacyTeamStructure(ctx context.Context, in *servicePb.GetLegacyTeamStructureRequest) (*servicePb.GetLegacyTeamStructureResponse, error) {
	return server.handlers.GetLegacyTeamStructure(ctx, in)
}

// System Roles
func (server *OrchardGRPCServer) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	return server.handlers.CreateSystemRole(ctx, in)
}

func (server *OrchardGRPCServer) CloneSystemRole(ctx context.Context, in *servicePb.CloneSystemRoleRequest) (*servicePb.CloneSystemRoleResponse, error) {
	return server.handlers.CloneSystemRole(ctx, in)
}

func (server *OrchardGRPCServer) GetSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.SystemRole, error) {
	return server.handlers.GetSystemRoleById(ctx, in)
}

func (server *OrchardGRPCServer) GetSystemRoleWithBaseRole(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetSystemRoleWithBaseRoleResponse, error) {
	return server.handlers.GetSystemRoleWithBaseRole(ctx, in)
}

func (server *OrchardGRPCServer) GetSystemRoles(ctx context.Context, in *servicePb.GetSystemRolesRequest) (*servicePb.GetSystemRolesResponse, error) {
	return server.handlers.GetSystemRoles(ctx, in)
}

func (server *OrchardGRPCServer) UpdateSystemRole(ctx context.Context, in *servicePb.UpdateSystemRoleRequest) (*servicePb.UpdateSystemRoleResponse, error) {
	return server.handlers.UpdateSystemRole(ctx, in)
}

func (server *OrchardGRPCServer) DeleteSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return server.handlers.DeleteSystemRoleById(ctx, in)
}

func (server *OrchardGRPCServer) UpsertCRMRoles(ctx context.Context, in *servicePb.UpsertCRMRolesRequest) (*servicePb.UpsertCRMRolesResponse, error) {
	return server.handlers.UpsertCRMRoles(ctx, in)
}

func (server *OrchardGRPCServer) GetCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.CRMRole, error) {
	return server.handlers.GetCRMRoleById(ctx, in)
}

func (server *OrchardGRPCServer) GetCRMRoles(ctx context.Context, in *servicePb.GetCRMRolesRequest) (*servicePb.GetCRMRolesResponse, error) {
	return server.handlers.GetCRMRoles(ctx, in)
}

func (server *OrchardGRPCServer) GetUnsyncedCRMRoles(ctx context.Context, in *servicePb.GetUnsyncedCRMRolesRequest) (*servicePb.GetUnsyncedCRMRolesResponse, error) {
	return server.handlers.GetUnsyncedCRMRoles(ctx, in)
}

func (server *OrchardGRPCServer) DeleteCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return server.handlers.DeleteCRMRoleById(ctx, in)
}

// Group Viewers
func (server *OrchardGRPCServer) InsertGroupViewer(ctx context.Context, in *servicePb.InsertGroupViewerRequest) (*servicePb.InsertGroupViewerResponse, error) {
	return server.handlers.InsertGroupViewer(ctx, in)
}

func (server *OrchardGRPCServer) GetGroupViewers(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetGroupViewersResponse, error) {
	return server.handlers.GetGroupViewers(ctx, in)
}

func (server *OrchardGRPCServer) GetPersonViewableGroups(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetPersonViewableGroupsResponse, error) {
	return server.handlers.GetPersonViewableGroups(ctx, in)
}

func (server *OrchardGRPCServer) UpdateGroupViewer(ctx context.Context, in *servicePb.UpdateGroupViewerRequest) (*servicePb.UpdateGroupViewerResponse, error) {
	return server.handlers.UpdateGroupViewer(ctx, in)
}

func (server *OrchardGRPCServer) DeleteGroupViewerById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return server.handlers.DeleteGroupViewerById(ctx, in)
}

// Person
func (server *OrchardGRPCServer) CreatePerson(ctx context.Context, in *servicePb.CreatePersonRequest) (*servicePb.CreatePersonResponse, error) {
	return server.handlers.CreatePerson(ctx, in)
}

func (server *OrchardGRPCServer) UpsertPeople(ctx context.Context, in *servicePb.UpsertPeopleRequest) (*servicePb.UpsertPeopleResponse, error) {
	return server.handlers.UpsertPeople(ctx, in)
}

func (server *OrchardGRPCServer) GetPersonById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Person, error) {
	return server.handlers.GetPersonById(ctx, in)
}

func (server *OrchardGRPCServer) SearchPeople(ctx context.Context, in *servicePb.SearchPeopleRequest) (*servicePb.SearchPeopleResponse, error) {
	return server.handlers.SearchPeople(ctx, in)
}

func (server *OrchardGRPCServer) GetGroupMembers(ctx context.Context, in *servicePb.GetGroupMembersRequest) (*servicePb.GetGroupMembersResponse, error) {
	return server.handlers.GetGroupMembers(ctx, in)
}

func (server *OrchardGRPCServer) GetUngroupedPeople(ctx context.Context, in *servicePb.GetUngroupedPeopleRequest) (*servicePb.GetUngroupedPeopleResponse, error) {
	return server.handlers.GetUngroupedPeople(ctx, in)
}

func (server *OrchardGRPCServer) GetVirtualUsers(ctx context.Context, in *servicePb.GetVirtualUsersRequest) (*servicePb.GetVirtualUsersResponse, error) {
	return server.handlers.GetVirtualUsers(ctx, in)
}

func (server *OrchardGRPCServer) UpdatePerson(ctx context.Context, in *servicePb.UpdatePersonRequest) (*servicePb.UpdatePersonResponse, error) {
	return server.handlers.UpdatePerson(ctx, in)
}

func (server *OrchardGRPCServer) UpdatePersonGroups(ctx context.Context, in *servicePb.UpdatePersonGroupsRequest) (*servicePb.UpdatePersonGroupsResponse, error) {
	return server.handlers.UpdatePersonGroups(ctx, in)
}

func (server *OrchardGRPCServer) DeletePersonById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return server.handlers.DeletePersonById(ctx, in)
}

// Groups
func (server *OrchardGRPCServer) CreateGroup(ctx context.Context, in *servicePb.CreateGroupRequest) (*servicePb.CreateGroupResponse, error) {
	return server.handlers.CreateGroup(ctx, in)
}

func (server *OrchardGRPCServer) GetGroupById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {
	return server.handlers.GetGroupById(ctx, in)
}

func (server *OrchardGRPCServer) GetGroups(ctx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {
	return server.handlers.GetGroups(ctx, in)
}

func (server *OrchardGRPCServer) GetGroupSubTree(ctx context.Context, in *servicePb.GetGroupSubTreeRequest) (*servicePb.GetGroupSubTreeResponse, error) {
	return server.handlers.GetGroupSubTree(ctx, in)
}

func (server *OrchardGRPCServer) UpdateGroup(ctx context.Context, in *servicePb.UpdateGroupRequest) (*servicePb.UpdateGroupResponse, error) {
	return server.handlers.UpdateGroup(ctx, in)
}

func (server *OrchardGRPCServer) UpdateGroupTypes(ctx context.Context, in *servicePb.UpdateGroupTypesRequest) (*servicePb.UpdateGroupTypesResponse, error) {
	return server.handlers.UpdateGroupTypes(ctx, in)
}

func (server *OrchardGRPCServer) DeleteGroupById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return server.handlers.DeleteGroupById(ctx, in)
}

func (server *OrchardGRPCServer) GetTenantGroupsLastModifiedTS(ctx context.Context, in *servicePb.GetTenantGroupsLastModifiedTSRequest) (*servicePb.GetTenantGroupsLastModifiedTSResponse, error) {
	return server.handlers.GetTenantGroupsLastModifiedTS(ctx, in)
}

func (server *OrchardGRPCServer) ClonePerson(ctx context.Context, in *servicePb.ClonePersonRequest) (*servicePb.ClonePersonResponse, error) {
	return server.handlers.ClonePerson(ctx, in)
}
