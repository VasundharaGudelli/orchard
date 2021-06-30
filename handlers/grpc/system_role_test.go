package grpchandlers

import (
	"context"
	"encoding/json"
	"testing"

	configUtil "github.com/loupe-co/go-common/config"
	"github.com/loupe-co/go-common/fixtures"
	"github.com/loupe-co/orchard/config"
	"github.com/loupe-co/orchard/db"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func setup() (*OrchardGRPCServer, error) {
	fixtures.InitTestFixtures("../../fixtures", "../../fixtures/results")
	cfg := config.Config{}
	err := configUtil.Load(
		&cfg,
		configUtil.FromENV(),
		configUtil.SetDefaultENV("project", "local"),
		configUtil.SetDefaultENV("PROJECT_ID", "loupe-dev"),
		configUtil.SetDefaultENV("DB_HOST", "35.245.37.78"),
		configUtil.SetDefaultENV("DB_PASSWORD", "jLariybb1oe5FbDz"),
		configUtil.SetDefaultENV("DB_MAX_CONNECTIONS", "10"),
	)
	if err != nil {
		panic("Error parsing config from environment")
	}
	if err := db.Init(cfg); err != nil {
		return nil, err
	}
	return &OrchardGRPCServer{cfg: cfg}, nil
}

func TestCreateSystemRole(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestCreateSystemRole"]
	req := &servicePb.CreateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.CreateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_create_system_role.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestCreateSystemRoleBadRequestNonEmptyID(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestCreateSystemRoleBadRequestNonEmptyID"]
	req := &servicePb.CreateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.CreateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_create_system_role_bad_request_non_empty_id.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestCreateSystemRoleBadRequestNilSystemRole(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestCreateSystemRoleBadRequestNilSystemRole"]
	req := &servicePb.CreateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.CreateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_create_system_role_bad_request_nil_system_role.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetSystemRoleByID(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestGetSystemRoleByID"]
	req := &servicePb.IdRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.GetSystemRoleById(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_get_system_role_by_id.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetSystemRoleByIDBadRequestEmptyID(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestGetSystemRoleByIDBadRequestEmptyID"]
	req := &servicePb.IdRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.GetSystemRoleById(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_get_system_role_by_id_bad_request_empty_id.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetSystemRoles(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestGetSystemRoles"]
	req := &servicePb.GetSystemRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.GetSystemRoles(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_get_system_roles.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetSystemRolesWithSearch(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestGetSystemRolesWithSearch"]
	req := &servicePb.GetSystemRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.GetSystemRoles(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_get_system_roles_with_search.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetSystemRolesNoResults(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestGetSystemRolesNoResults"]
	req := &servicePb.GetSystemRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.GetSystemRoles(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_get_system_roles_no_results.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateSystemRole(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestUpdateSystemRole"]
	req := &servicePb.UpdateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.UpdateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_update_system_role.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateSystemRoleWithOnlyFields(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestUpdateSystemRoleWithOnlyFields"]
	req := &servicePb.UpdateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.UpdateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_update_system_role_with_only_fields.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateSystemRoleBadRequestNilSystemRole(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestUpdateSystemRoleBadRequestNilSystemRole"]
	req := &servicePb.UpdateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.UpdateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_update_system_role_bad_request_nil_system_role.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateSystemRoleBadRequestEmptyID(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestUpdateSystemRoleBadRequestEmptyID"]
	req := &servicePb.UpdateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.UpdateSystemRole(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_update_system_role_bad_request_empty_id.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestDeleteSystemRoleByID(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestDeleteSystemRoleByID"]
	req := &servicePb.IdRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.DeleteSystemRoleById(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_delete_system_role_by_id.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestDeleteSystemRoleByIdNOOP(t *testing.T) {
	server, err := setup()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	testData := fixtures.Data["TestDeleteSystemRoleByIdNOOP"]
	req := &servicePb.IdRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := server.DeleteSystemRoleById(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/test_delete_system_role_by_id_noop.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
