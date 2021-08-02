package grpchandlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/loupe-co/go-common/fixtures"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func TestGetLegacyTeamStructure(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["get_legacy_team"], "TestGetLegacyTeamStructure")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.GetLegacyTeamStructureRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.GetLegacyTeamStructure(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestGetLegacyTeamStructure.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}
