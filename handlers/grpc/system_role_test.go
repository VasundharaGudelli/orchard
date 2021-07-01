package grpchandlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/buger/jsonparser"
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
		configUtil.SetDefaultENV("SERVER_NAME", "orchard"),
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestCreateSystemRole")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	if res == nil || res.SystemRole == nil {
		t.Log("expected response to be non-nil, but got nil")
		t.Fail()
		return
	}

	if res.SystemRole.Id == "" {
		t.Log("expected systemRole id to be not empty, but is empty")
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestCreateSystemRoleBadRequestNonEmptyID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.CreateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = server.CreateSystemRole(context.Background(), req)
	if err == nil {
		t.Log("expected server to return an error, but got nil error")
		t.Fail()
		return
	}

	if !strings.Contains(err.Error(), "Bad Request") {
		t.Log("expected error to contain 'Bad Request', but didn't")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(err.Error(), "", "  ")
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestCreateSystemRoleBadRequestNilSystemRole")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.CreateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = server.CreateSystemRole(context.Background(), req)
	if err == nil {
		t.Log("expected server to return an error, but got nil error")
		t.Fail()
		return
	}

	if !strings.Contains(err.Error(), "Bad Request") {
		t.Log("expected error to contain 'Bad Request', but didn't")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(err.Error(), "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestGetSystemRoleByID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestGetSystemRoleByIDBadRequestEmptyID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.IdRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = server.GetSystemRoleById(context.Background(), req)
	if err == nil {
		t.Log("expected server to return an error, but got nil error")
		t.Fail()
		return
	}

	if !strings.Contains(err.Error(), "Bad Request") {
		t.Log("expected error to contain 'Bad Request', but didn't")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(err.Error(), "", "  ")
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestGetSystemRoles")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestGetSystemRolesWithSearch")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestGetSystemRolesNoResults")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestUpdateSystemRole")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestUpdateSystemRoleWithOnlyFields")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestUpdateSystemRoleBadRequestNilSystemRole")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = server.UpdateSystemRole(context.Background(), req)
	if err == nil {
		t.Log("expected server to return an error, but got nil error")
		t.Fail()
		return
	}

	if !strings.Contains(err.Error(), "Bad Request") {
		t.Log("expected error to contain 'Bad Request', but didn't")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(err.Error(), "", "  ")
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestUpdateSystemRoleBadRequestEmptyID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateSystemRoleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = server.UpdateSystemRole(context.Background(), req)
	if err == nil {
		t.Log("expected server to return an error, but got nil error")
		t.Fail()
		return
	}

	if !strings.Contains(err.Error(), "Bad Request") {
		t.Log("expected error to contain 'Bad Request', but didn't")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(err.Error(), "", "  ")
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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestDeleteSystemRoleByID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

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

	testData, _, _, err := jsonparser.Get(fixtures.Data["system_roles"], "TestDeleteSystemRoleByIdNOOP")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.IdRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = server.DeleteSystemRoleById(context.Background(), req)
	if err == nil {
		t.Log("expected server to return an error, but got nil error")
		t.Fail()
		return
	}

	if !strings.Contains(err.Error(), "affected 0") {
		t.Log("expected error to contain 'affected 0', but didn't")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(err.Error(), "", "  ")
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
