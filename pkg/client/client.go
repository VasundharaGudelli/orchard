package client

import (
	"context"

	configUtil "github.com/loupe-co/go-common/config"
	"github.com/loupe-co/go-common/errors"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc"
)

type OrchardClientConfig struct {
	Addr string `env:"ORCHARD_ADDR" envDefault:"" json:"orchardAddr" yaml:"orchardAddr"`
}

type OrchardClient struct {
	conn   *grpc.ClientConn
	client servicePb.OrchardClient
}

func New(addr string) (*OrchardClient, error) {
	cfg := OrchardClientConfig{
		Addr: addr,
	}

	// If no address was passed then look for config values in the env automatically
	if addr == "" {
		if err := configUtil.Load(&cfg, configUtil.FromENV()); err != nil {
			return nil, errors.Wrap(err, "error loading orchard client config from env")
		}
	}

	// Establish grpc connection with bouncer service, used for refreshing auth data in cache
	conn, err := grpc.Dial(cfg.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "error getting bouncer service connection")
	}

	return &OrchardClient{
		conn:   conn,
		client: servicePb.NewOrchardClient(conn),
	}, nil
}

func (client *OrchardClient) GetUserTeam(ctx context.Context, in *servicePb.GetUserTeamRequest) (*servicePb.GetUserTeamResponse, error) {
	return client.client.GetUserTeam(ctx, in)
}

func (client *OrchardClient) Sync(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return client.client.Sync(ctx, in)
}

func (client *OrchardClient) SyncUsers(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return client.client.SyncUsers(ctx, in)
}

func (client *OrchardClient) SyncGroups(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return client.client.SyncGroups(ctx, in)
}

func (client *OrchardClient) SyncCrmRoles(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	return client.client.SyncCrmRoles(ctx, in)
}

func (client *OrchardClient) IsHierarchySynced(ctx context.Context, in *servicePb.IsHierarchySyncedRequest) (*servicePb.IsHierarchySyncedResponse, error) {
	return client.client.IsHierarchySynced(ctx, in)
}

func (client *OrchardClient) ResetHierarchy(ctx context.Context, in *servicePb.ResetHierarchyRequest) (*servicePb.ResetHierarchyResponse, error) {
	return client.client.ResetHierarchy(ctx, in)
}

func (client *OrchardClient) GetGroupSyncSettings(ctx context.Context, in *servicePb.GetGroupSyncSettingsRequest) (*servicePb.GetGroupSyncSettingsResponse, error) {
	return client.client.GetGroupSyncSettings(ctx, in)
}

func (client *OrchardClient) ReSyncCRM(ctx context.Context, in *servicePb.ReSyncCRMRequest) (*servicePb.ReSyncCRMResponse, error) {
	return client.client.ReSyncCRM(ctx, in)
}

func (client *OrchardClient) UpdateGroupSyncState(ctx context.Context, in *servicePb.UpdateGroupSyncStateRequest) (*servicePb.UpdateGroupSyncStateResponse, error) {
	return client.client.UpdateGroupSyncState(ctx, in)
}

func (client *OrchardClient) UpdateGroupSyncMetadata(ctx context.Context, in *servicePb.UpdateGroupSyncMetadataRequest) (*servicePb.UpdateGroupSyncMetadataResponse, error) {
	return client.client.UpdateGroupSyncMetadata(ctx, in)
}

func (client *OrchardClient) GetLegacyTeamStructure(ctx context.Context, in *servicePb.GetLegacyTeamStructureRequest) (*servicePb.GetLegacyTeamStructureResponse, error) {
	return client.client.GetLegacyTeamStructure(ctx, in)
}

func (client *OrchardClient) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	return client.client.CreateSystemRole(ctx, in)
}

func (client *OrchardClient) GetSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.SystemRole, error) {
	return client.client.GetSystemRoleById(ctx, in)
}

func (client *OrchardClient) GetSystemRoles(ctx context.Context, in *servicePb.GetSystemRolesRequest) (*servicePb.GetSystemRolesResponse, error) {
	return client.client.GetSystemRoles(ctx, in)
}

func (client *OrchardClient) UpdateSystemRole(ctx context.Context, in *servicePb.UpdateSystemRoleRequest) (*servicePb.UpdateSystemRoleResponse, error) {
	return client.client.UpdateSystemRole(ctx, in)
}

func (client *OrchardClient) DeleteSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return client.client.DeleteSystemRoleById(ctx, in)
}

func (client *OrchardClient) UpsertCRMRoles(ctx context.Context, in *servicePb.UpsertCRMRolesRequest) (*servicePb.UpsertCRMRolesResponse, error) {
	return client.client.UpsertCRMRoles(ctx, in)
}

func (client *OrchardClient) GetCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.CRMRole, error) {
	return client.client.GetCRMRoleById(ctx, in)
}

func (client *OrchardClient) GetCRMRoles(ctx context.Context, in *servicePb.GetCRMRolesRequest) (*servicePb.GetCRMRolesResponse, error) {
	return client.client.GetCRMRoles(ctx, in)
}

func (client *OrchardClient) GetUnsyncedCRMRoles(ctx context.Context, in *servicePb.GetUnsyncedCRMRolesRequest) (*servicePb.GetUnsyncedCRMRolesResponse, error) {
	return client.client.GetUnsyncedCRMRoles(ctx, in)
}

func (client *OrchardClient) DeleteCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return client.client.DeleteCRMRoleById(ctx, in)
}

func (client *OrchardClient) InsertGroupViewer(ctx context.Context, in *servicePb.InsertGroupViewerRequest) (*servicePb.InsertGroupViewerResponse, error) {
	return client.client.InsertGroupViewer(ctx, in)
}

func (client *OrchardClient) GetGroupViewers(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetGroupViewersResponse, error) {
	return client.client.GetGroupViewers(ctx, in)
}

func (client *OrchardClient) GetPersonViewableGroups(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetPersonViewableGroupsResponse, error) {
	return client.client.GetPersonViewableGroups(ctx, in)
}

func (client *OrchardClient) UpdateGroupViewer(ctx context.Context, in *servicePb.UpdateGroupViewerRequest) (*servicePb.UpdateGroupViewerResponse, error) {
	return client.client.UpdateGroupViewer(ctx, in)
}

func (client *OrchardClient) DeleteGroupViewerById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return client.client.DeleteGroupViewerById(ctx, in)
}

func (client *OrchardClient) CreatePerson(ctx context.Context, in *servicePb.CreatePersonRequest) (*servicePb.CreatePersonResponse, error) {
	return client.client.CreatePerson(ctx, in)
}

func (client *OrchardClient) UpsertPeople(ctx context.Context, in *servicePb.UpsertPeopleRequest) (*servicePb.UpsertPeopleResponse, error) {
	return client.client.UpsertPeople(ctx, in)
}

func (client *OrchardClient) GetPersonById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Person, error) {
	return client.client.GetPersonById(ctx, in)
}

func (client *OrchardClient) SearchPeople(ctx context.Context, in *servicePb.SearchPeopleRequest) (*servicePb.SearchPeopleResponse, error) {
	return client.client.SearchPeople(ctx, in)
}

func (client *OrchardClient) GetGroupMembers(ctx context.Context, in *servicePb.GetGroupMembersRequest) (*servicePb.GetGroupMembersResponse, error) {
	return client.client.GetGroupMembers(ctx, in)
}

func (client *OrchardClient) GetUngroupedPeople(ctx context.Context, in *servicePb.GetUngroupedPeopleRequest) (*servicePb.GetUngroupedPeopleResponse, error) {
	return client.client.GetUngroupedPeople(ctx, in)
}

func (client *OrchardClient) GetVirtualUsers(ctx context.Context, in *servicePb.GetVirtualUsersRequest) (*servicePb.GetVirtualUsersResponse, error) {
	return client.client.GetVirtualUsers(ctx, in)
}

func (client *OrchardClient) UpdatePerson(ctx context.Context, in *servicePb.UpdatePersonRequest) (*servicePb.UpdatePersonResponse, error) {
	return client.client.UpdatePerson(ctx, in)
}

func (client *OrchardClient) DeletePersonById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return client.client.DeletePersonById(ctx, in)
}

func (client *OrchardClient) HardDeletePersonById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return client.client.HardDeletePersonById(ctx, in)
}

func (client *OrchardClient) CreateGroup(ctx context.Context, in *servicePb.CreateGroupRequest) (*servicePb.CreateGroupResponse, error) {
	return client.client.CreateGroup(ctx, in)
}

func (client *OrchardClient) GetGroupById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {
	return client.client.GetGroupById(ctx, in)
}

func (client *OrchardClient) GetGroups(ctx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {
	return client.client.GetGroups(ctx, in)
}

func (client *OrchardClient) GetGroupSubTree(ctx context.Context, in *servicePb.GetGroupSubTreeRequest) (*servicePb.GetGroupSubTreeResponse, error) {
	return client.client.GetGroupSubTree(ctx, in)
}

func (client *OrchardClient) UpdateGroup(ctx context.Context, in *servicePb.UpdateGroupRequest) (*servicePb.UpdateGroupResponse, error) {
	return client.client.UpdateGroup(ctx, in)
}

func (client *OrchardClient) UpdateGroupTypes(ctx context.Context, in *servicePb.UpdateGroupTypesRequest) (*servicePb.UpdateGroupTypesResponse, error) {
	return client.client.UpdateGroupTypes(ctx, in)
}

func (client *OrchardClient) DeleteGroupById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return client.client.DeleteGroupById(ctx, in)
}

func (client *OrchardClient) ClonePerson(ctx context.Context, in *servicePb.ClonePersonRequest) (*servicePb.ClonePersonResponse, error) {
	return client.client.ClonePerson(ctx, in)
}

func (client *OrchardClient) ConvertVirtualUsers(ctx context.Context, in *servicePb.ConvertVirtualUsersRequest) (*servicePb.ConvertVirtualUsersResponse, error) {
	return client.client.ConvertVirtualUsers(ctx, in)
}

func (client *OrchardClient) GetPeopleByEmail(ctx context.Context, in *servicePb.GetPeopleByEmailRequest) (*servicePb.GetPeopleByEmailResponse, error) {
	return client.client.GetPeopleByEmail(ctx, in)
}
