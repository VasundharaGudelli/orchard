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

func TestUpsertPeople(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestUpsertPeople")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpsertPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpsertPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpsertPeople.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpsertPeopleNoPeople(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestUpsertPeopleNoPeople")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpsertPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.UpsertPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpsertPeopleNoPeople.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestUpsertPeopleBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestUpsertPeopleBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.UpsertPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.UpsertPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestUpsertPeopleBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestSearchPeople(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestSearchPeople")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.SearchPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.SearchPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestSearchPeople.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestSearchPeopleWithSearch(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestSearchPeopleWithSearch")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.SearchPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.SearchPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestSearchPeopleWithSearch.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestSearchPeopleWithPaging(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestSearchPeopleWithPaging")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.SearchPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.SearchPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestSearchPeopleWithPaging.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestSearchPeopleBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestSearchPeopleBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.SearchPeopleRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.SearchPeople(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestSearchPeopleBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupMembers(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestGetGroupMembers")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupMembersRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetGroupMembers(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupMembers.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupMembersBadRequestEmptyTenantID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestGetGroupMembersBadRequestEmptyTenantID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupMembersRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.GetGroupMembers(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupMembersBadRequestEmptyTenantID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestGetGroupMembersBadRequestEmptyGroupID(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["person"], "TestGetGroupMembersBadRequestEmptyGroupID")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetGroupMembersRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	_, err = testServer.GetGroupMembers(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetGroupMembersBadRequestEmptyGroupID.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
