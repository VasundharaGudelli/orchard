package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/loupe-co/go-common/fixtures"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func TestSync(t *testing.T) {
	testData, _, _, err := jsonparser.Get(fixtures.Data["sync"], "TestSync")
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	req := &servicePb.SyncRequest{}
	if err := json.Unmarshal(testData, req); err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	res, err := testServer.Sync(context.Background(), req)
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

	if err := fixtures.WriteTestResult("../../fixtures/results/TestSync.json", rawResult); err != nil {
		t.Log(err)
		t.Fail()
		return
	}
}

func TestReSyncCRM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	res, err := testServer.ReSyncCRM(ctx, &servicePb.ReSyncCRMRequest{TenantId: "5aa7aabb-12ea-4c6e-ac71-35a8dcfdb5ac"})
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	t.Log(res.Status)
}
