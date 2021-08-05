package grpchandlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/loupe-co/go-common/fixtures"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func TestUpsertCRMRoles(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "UpsertCRMRoles")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpsertCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpsertCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/UpsertCRMRoles.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestUpsertCRMRolesBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "UpsertCRMRolesBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpsertCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpsertCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/UpsertCRMRolesBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestUpsertCRMRolesNoRecords(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "UpsertCRMRolesNoRecords")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpsertCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpsertCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/UpsertCRMRolesNoRecords.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestGetCRMRoleById(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "GetCRMRoleById")
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

	res, err := testServer.GetCRMRoleById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/GetCRMRoleById.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestGetCRMRoleByIdBadRequestEmptyID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "GetCRMRoleByIdBadRequestEmptyID")
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

	res, err := testServer.GetCRMRoleById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/GetCRMRoleByIdBadRequestEmptyID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestGetCRMRoleByIdBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "GetCRMRoleByIdBadRequestEmptyTenantID")
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

	res, err := testServer.GetCRMRoleById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/GetCRMRoleByIdBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestGetCRMRoles(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "GetCRMRoles")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/GetCRMRoles.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetCRMRolesWithSearch(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "GetCRMRolesWithSearch")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/GetCRMRolesWithSearch.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestGetCRMRolesBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "GetCRMRolesBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/GetCRMRolesBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetUnsyncedCRMRoles(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "TestGetUnsyncedCRMRoles")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetUnsyncedCRMRolesRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetUnsyncedCRMRoles(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetUnsyncedCRMRoles.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestDeleteCRMRoleById(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "DeleteCRMRoleById")
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

	res, err := testServer.DeleteCRMRoleById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/DeleteCRMRoleById.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestDeleteCRMRoleByIdBadRequestEmptyID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "DeleteCRMRoleByIdBadRequestEmptyID")
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

	res, err := testServer.DeleteCRMRoleById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/DeleteCRMRoleByIdBadRequestEmptyID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
func TestDeleteCRMRoleByIdBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["crm_role"], "DeleteCRMRoleByIdBadRequestEmptyTenantID")
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

	res, err := testServer.DeleteCRMRoleById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/DeleteCRMRoleByIdBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
