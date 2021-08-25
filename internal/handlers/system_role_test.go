package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/loupe-co/go-common/fixtures"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func TestCreateSystemRole(t *testing.T) {
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

	res, err := testServer.CreateSystemRole(context.Background(), req)
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

	_, err = testServer.CreateSystemRole(context.Background(), req)
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

	_, err = testServer.CreateSystemRole(context.Background(), req)
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

	res, err := testServer.GetSystemRoleById(context.Background(), req)
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

	_, err = testServer.GetSystemRoleById(context.Background(), req)
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

	res, err := testServer.GetSystemRoles(context.Background(), req)
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

	res, err := testServer.GetSystemRoles(context.Background(), req)
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

	res, err := testServer.GetSystemRoles(context.Background(), req)
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

	res, err := testServer.UpdateSystemRole(context.Background(), req)
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

	res, err := testServer.UpdateSystemRole(context.Background(), req)
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

	_, err = testServer.UpdateSystemRole(context.Background(), req)
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

	_, err = testServer.UpdateSystemRole(context.Background(), req)
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

	res, err := testServer.DeleteSystemRoleById(context.Background(), req)
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

	_, err = testServer.DeleteSystemRoleById(context.Background(), req)
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
