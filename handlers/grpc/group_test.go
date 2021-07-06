package grpchandlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/loupe-co/go-common/fixtures"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func TestCreateGroup(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestCreateGroup")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.CreateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.CreateGroup(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if res == nil || res.Group == nil {
		t.Log("Expected result to be non-nil, but got nil")
		t.Fail()
		return
	}

	if res.Group.Id == "" {
		t.Log("Expected result.Group.Id to be non-empty, but got empty")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/TestCreateGroup.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestCreateGroupBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestCreateGroupBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.CreateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.CreateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestCreateGroupBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestCreateGroupBadRequestNilGroup(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestCreateGroupBadRequestNilGroup")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.CreateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.CreateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestCreateGroupBadRequestNilGroup.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestCreateGroupBadRequestNonEmptyID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestCreateGroupBadRequestNonEmptyID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.CreateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.CreateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestCreateGroupBadRequestNonEmptyID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupById(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupById")
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

	res, err := testServer.GetGroupById(context.Background(), req)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if res == nil {
		t.Log("expected result to be non-nil, but got nil")
		t.Fail()
		return
	}

	// TODO: put in correct group id for check once seed data is filled out
	if res.Id == "" {
		t.Log("expected result id to be {id}, but got {id2}")
		t.Fail()
		return
	}

	rawResult, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupById.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupByIdBadRequestEmptyID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupByIdBadRequestEmptyID")
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

	_, err = testServer.GetGroupById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupByIdBadRequestEmptyID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupByIdBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupByIdBadRequestEmptyTenantID")
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

	_, err = testServer.GetGroupById(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupByIdBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroups(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroups")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupsRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetGroups(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroups.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupsWithSearch(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupsWithSearch")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupsRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetGroups(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupsWithSearch.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupsBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupsBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupsRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.GetGroups(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupsBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupSubTree(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupSubTree")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupSubTreeRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetGroupSubTree(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupSubTree.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupSubTreeWithDepth(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupSubTreeWithDepth")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupSubTreeRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetGroupSubTree(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupSubTreeWithDepth.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupSubTreeWithHydrateUsers(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupSubTreeWithHydrateUsers")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupSubTreeRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetGroupSubTree(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupSubTreeWithHydrateUsers.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupSubTreeBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupSubTreeBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupSubTreeRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.GetGroupSubTree(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupSubTreeBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupSubTreeBadRequestEmptyGroupID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestGetGroupSubTreeBadRequestEmptyGroupID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupSubTreeRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.GetGroupSubTree(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupSubTreeBadRequestEmptyGroupID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateGroup(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestUpdateGroup")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpdateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpdateGroup.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateGroupWithOnlyFields(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestUpdateGroupWithOnlyFields")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpdateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpdateGroupWithOnlyFields.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateGroupBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestUpdateGroupBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.UpdateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpdateGroupBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateGroupBadRequestEmptyID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestUpdateGroupBadRequestEmptyID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.UpdateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpdateGroupBadRequestEmptyID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpdateGroupBadRequestNilGroup(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestUpdateGroupBadRequestNilGroup")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpdateGroupRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.UpdateGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpdateGroupBadRequestNilGroup.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestDeleteGroup(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestDeleteGroup")
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

	res, err := testServer.DeleteGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestDeleteGroup.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestDeleteGroupBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestDeleteGroupBadRequestEmptyTenantID")
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

	_, err = testServer.DeleteGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestDeleteGroupBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestDeleteGroupBadRequestEmptyID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["group"], "TestDeleteGroupBadRequestEmptyID")
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

	_, err = testServer.DeleteGroup(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestDeleteGroupBadRequestEmptyID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
