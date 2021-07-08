package grpchandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/buger/jsonparser"
	configUtil "github.com/loupe-co/go-common/config"
	"github.com/loupe-co/go-common/fixtures"
	"github.com/loupe-co/orchard/config"
	"github.com/loupe-co/orchard/db"
	"github.com/loupe-co/orchard/models"
)

var testServer *OrchardGRPCServer
var generatedTestIDs = map[string][]string{
	"system_role":  []string{},
	"crm_role":     []string{},
	"group":        []string{},
	"person":       []string{},
	"group_viewer": []string{},
}

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
		configUtil.SetDefaultENV("DB_DEBUG", "true"),
		configUtil.SetDefaultENV("TENANT_SERVICE_ADDR", "true"),
		configUtil.SetDefaultENV("CRM_SERVICE_ADDR", "true"),
	)
	if err != nil {
		panic("Error parsing config from environment")
	}
	if err := db.Init(cfg); err != nil {
		return nil, err
	}
	if err := seed(); err != nil {
		return nil, err
	}
	return &OrchardGRPCServer{cfg: cfg}, nil
}

func seed() error {
	// CRM Role
	crmRolesRaw, _, _, err := jsonparser.Get(fixtures.Data["seed"], "crm_roles")
	if err != nil {
		fmt.Println("error parsing crm_role seed data")
		return err
	}
	crmRoles := []*models.CRMRole{}
	if err := json.Unmarshal(crmRolesRaw, &crmRoles); err != nil {
		fmt.Println("error parsing crm_role seed data")
		return err
	}
	crmSvc := db.NewCRMRoleService()
	for _, role := range crmRoles {
		if err := crmSvc.Insert(context.Background(), role); err != nil {
			fmt.Println("error inserting crm_role")
			return err
		}
		generatedTestIDs["crm_role"] = append(generatedTestIDs["crm_role"], role.ID)
	}

	// Group
	groupsRaw, _, _, err := jsonparser.Get(fixtures.Data["seed"], "groups")
	if err != nil {
		fmt.Println("error parsing group seed data")
		return err
	}
	groups := []*models.Group{}
	if err := json.Unmarshal(groupsRaw, &groups); err != nil {
		fmt.Println("error parsing group seed data")
		return err
	}
	groupSvc := db.NewGroupService()
	for _, group := range groups {
		if err := groupSvc.Insert(context.Background(), group); err != nil {
			fmt.Println("error inserting group")
			return err
		}
		generatedTestIDs["group"] = append(generatedTestIDs["group"], group.ID)
	}

	// Person
	peopleRaw, _, _, err := jsonparser.Get(fixtures.Data["seed"], "people")
	if err != nil {
		fmt.Println("error parsing person seed data")
		return err
	}
	people := []*models.Person{}
	if err := json.Unmarshal(peopleRaw, &people); err != nil {
		fmt.Println("error parsing person seed data")
		return err
	}
	personSvc := db.NewPersonService()
	for _, person := range people {
		if err := personSvc.Insert(context.Background(), person); err != nil {
			fmt.Println("error inserting person")
			return err
		}
		generatedTestIDs["person"] = append(generatedTestIDs["person"], person.ID)
	}

	// Group Viewer
	viewersRaw, _, _, err := jsonparser.Get(fixtures.Data["seed"], "group_viewers")
	if err != nil {
		fmt.Println("error parsing groupViewer seed data")
		return err
	}
	viewers := []*models.GroupViewer{}
	if err := json.Unmarshal(viewersRaw, &viewers); err != nil {
		fmt.Println("error parsing groupViewer seed data")
		return err
	}
	viewerSvc := db.NewGroupViewerService()
	for _, viewer := range viewers {
		if err := viewerSvc.Insert(context.Background(), viewer); err != nil {
			fmt.Println("error inserting groupViewer")
			return err
		}
		generatedTestIDs["group_viewer"] = append(generatedTestIDs["group_viewer"], fmt.Sprintf("%s:%s", viewer.GroupID, viewer.PersonID))
	}

	return nil
}

func teardown() error {
	failedIDs := map[string][]string{
		"system_role":  []string{},
		"crm_role":     []string{},
		"group":        []string{},
		"person":       []string{},
		"group_viewer": []string{},
	}
	for object, ids := range generatedTestIDs {
		switch object {
		case "system_role":
			svc := db.NewSystemRoleService()
			for _, id := range ids {
				if err := svc.DeleteByID(context.Background(), id); err != nil {
					failedIDs[object] = append(failedIDs[object], id)
				}
			}
		case "crm_role":
			svc := db.NewCRMRoleService()
			for _, id := range ids {
				if err := svc.DeleteByID(context.Background(), id, "00000000-0000-0000-0000-000000000000"); err != nil {
					failedIDs[object] = append(failedIDs[object], id)
				}
			}
		case "group":
			svc := db.NewGroupService()
			for _, id := range ids {
				if err := svc.DeleteByID(context.Background(), id, "00000000-0000-0000-0000-000000000000"); err != nil {
					failedIDs[object] = append(failedIDs[object], id)
				}
			}
		case "person":
			svc := db.NewPersonService()
			for _, id := range ids {
				if err := svc.DeleteByID(context.Background(), id, "00000000-0000-0000-0000-000000000000"); err != nil {
					failedIDs[object] = append(failedIDs[object], id)
				}
			}
		case "group_viewer":
			svc := db.NewGroupViewerService()
			for _, id := range ids {
				idParts := strings.Split(id, ":")
				groupID := idParts[0]
				personID := idParts[1]
				if err := svc.DeleteByID(context.Background(), "00000000-0000-0000-0000-000000000000", groupID, personID); err != nil {
					failedIDs[object] = append(failedIDs[object], id)
				}
			}
		default:
			failedIDs[object] = ids
		}
	}
	for object, ids := range failedIDs {
		if len(ids) > 0 {
			fmt.Printf("object type %s failed to cleanup some ids:\n\t", object)
			fmt.Println(strings.Join(ids, "\n"))
		}
	}
	return nil
}

func TestMain(m *testing.M) {
	server, err := setup()
	if err != nil {
		fmt.Println(err)
		if err := teardown(); err != nil {
			fmt.Println(err)
		}
		os.Exit(2)
	}
	testServer = server
	code := m.Run()
	if err := teardown(); err != nil {
		fmt.Println(err)
	}
	os.Exit(code)
}
