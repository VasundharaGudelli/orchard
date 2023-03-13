package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/loupe-co/go-common/fixtures"
	commonSync "github.com/loupe-co/go-common/sync"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/clients"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/models"
	"github.com/loupe-co/protos/src/common/orchard"
	"github.com/urfave/cli/v2"
)

func GetImportAuth0Command() *cli.Command {
	return &cli.Command{
		Name:  "auth0",
		Usage: "import provisioned users from auth0 into orchard",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "debug",
				Aliases: []string{"d"},
				Usage:   "if debug is passed, then no data will be upserted and will instead be written to a debug.json file in the same location as the cwd",
				Value:   false,
			},
		},
		Action: func(c *cli.Context) error {
			env := c.String("env")
			debug := c.Bool("debug")
			fmt.Printf("Attempting to import provisioned users from auth0 to orchard (debug:%v)\n", debug)
			return importAuth0Users(context.Background(), env, debug)
		},
	}
}

func importAuth0Users(ctx context.Context, env string, debug bool) error {
	ctx, span := log.StartSpan(ctx, "ImportAuth0Users")
	defer span.End()

	setAuth0Roles(env)

	dbClient, err := GetOrchardDB(env)
	if err != nil {
		return err
	}

	auth0Client, err := GetAuth0Client(env)
	if err != nil {
		return err
	}

	fmt.Println("Getting active tenants...")
	tenantSvc := dbClient.NewTenantService()
	tenants, err := tenantSvc.GetActiveTenants(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Getting user role map...")
	roleUserMap := map[string][]*orchard.Person{}
	for _, roleID := range auth0RoleIDs {
		people, err := auth0Client.GetRoleUsers(ctx, roleID)
		if err != nil {
			return err
		}
		roleUserMap[roleID] = people
	}
	userRoles := getAuth0UserRoleMap(roleUserMap)
	rawUserRoles, _ := json.MarshalIndent(userRoles, "", "  ")
	fixtures.WriteTestResult("./userRoles.json", rawUserRoles)

	parGroup, _ := commonSync.NewWorkerPool(ctx, 1)
	for _, tenant := range tenants {
		parGroup.Go(runTenantAuth0Users(ctx, env, tenant.ID, dbClient, auth0Client, userRoles, debug))
	}
	if err := parGroup.Close(); err != nil {
		return err
	}

	return nil
}

func runTenantAuth0Users(ctx context.Context, env, tenantID string, dbClient *db.DB, auth0Client *clients.Auth0Client, userRoles map[string]string, debug bool) func() error {
	ctx, span := log.StartSpan(ctx, "RunTenantAuth0Users")
	defer span.End()

	return func() error {
		fmt.Println("Importing users for tenant", tenantID)
		people, err := auth0Client.ImportUsers(ctx, tenantID)
		if err != nil {
			return err
		}
		people = mergeAuth0Users(people)
		people = mergeAuth0UsersByID(people)

		ids := make([]interface{}, len(people))
		for i, person := range people {
			ids[i] = person.Id
		}

		personSvc := dbClient.NewPersonService()

		currentPeople, err := personSvc.GetByIDs(ctx, tenantID, ids...)
		if err != nil {
			return err
		}
		existingPeople := make(map[string]*models.Person, len(currentPeople))
		for _, person := range currentPeople {
			existingPeople[person.ID] = person
		}

		batch := make([]*models.Person, len(people))
		for i, person := range people {
			p := personSvc.FromProto(person)
			if p.ID == "" {
				p.ID = db.MakeID()
			}
			p.TenantID = tenantID
			p.CreatedBy = db.DefaultTenantID
			p.CreatedAt = time.Now().UTC()
			p.UpdatedBy = db.DefaultTenantID
			p.UpdatedAt = time.Now().UTC()
			p.IsProvisioned = true
			p.IsSynced = false
			p.Status = strings.ToLower(orchard.BasicStatus_Active.String())
			if current, ok := existingPeople[person.Id]; ok {
				if current.Name.Valid {
					p.Name = current.Name
				}
				if current.FirstName.Valid {
					p.FirstName = current.FirstName
				}
				if current.LastName.Valid {
					p.LastName = current.LastName
				}
				if current.Email.Valid {
					p.Email = current.Email
				}
				if current.PhotoURL.Valid {
					p.PhotoURL = current.PhotoURL
				}
				if len(current.RoleIds) > 0 {
					p.RoleIds = current.RoleIds
				}
				if len(p.CRMRoleIds) == 0 {
					p.CRMRoleIds = current.CRMRoleIds
				}
				if !p.GroupID.Valid || p.GroupID.String == "" {
					p.GroupID = current.GroupID
				}
				p.IsSynced = current.IsSynced
				p.Status = current.Status
				p.Type = current.Type
			}
			role := userRoles[p.Email.String]
			if role != "" {
				p.RoleIds = []string{role}
			}
			p.Type = roleTypes[role]
			batch[i] = p
		}

		fmt.Println("Upserting provisioned users for tenant", tenantID)
		if !debug {
			if len(batch) == 0 {
				return nil
			}
			if err := personSvc.UpsertAll(ctx, batch); err != nil {
				return err
			}
		} else {
			debugResult := map[string][]*models.Person{"people": batch}
			debugData, err := json.MarshalIndent(debugResult, "", "  ")
			if err != nil {
				return err
			}
			if err := fixtures.WriteTestResult(fmt.Sprintf("./debug_%s.json", tenantID), debugData); err != nil {
				return err
			}
		}

		return nil
	}
}

func mergeAuth0Users(people []*orchard.Person) []*orchard.Person {
	mergeMap := make(map[string][]*orchard.Person)
	for _, person := range people {
		key := person.Email
		if _, ok := mergeMap[key]; !ok {
			mergeMap[key] = []*orchard.Person{}
		}
		mergeMap[key] = append(mergeMap[key], person)
	}
	merged := make([]*orchard.Person, len(mergeMap))
	i := 0
	for _, samePeeps := range mergeMap {
		merged[i] = mergeUsers(samePeeps)
		i++
	}
	return merged
}

func mergeAuth0UsersByID(people []*orchard.Person) []*orchard.Person {
	mergeMap := make(map[string][]*orchard.Person)
	for _, person := range people {
		key := person.Id
		if _, ok := mergeMap[key]; !ok {
			mergeMap[key] = []*orchard.Person{}
		}
		mergeMap[key] = append(mergeMap[key], person)
	}
	merged := make([]*orchard.Person, len(mergeMap))
	i := 0
	for _, samePeeps := range mergeMap {
		merged[i] = mergeUsers(samePeeps)
		i++
	}
	return merged
}

func mergeUsers(people []*orchard.Person) *orchard.Person {
	person := &orchard.Person{}
	for _, p := range people {
		switch {
		case person.Id == "" && p.Id != "":
			person.Id = p.Id
		case person.TenantId == "" && p.TenantId != "":
			person.TenantId = p.TenantId
		case person.Name == "" && p.Name != "":
			person.Name = p.Name
		case person.FirstName == "" && p.FirstName != "":
			person.FirstName = p.FirstName
		case person.LastName == "" && p.LastName != "":
			person.LastName = p.LastName
		case person.Email == "" && p.Email != "":
			person.Email = p.Email
		case person.PhotoUrl == "" && p.PhotoUrl != "":
			person.PhotoUrl = p.PhotoUrl
		}
	}
	return person
}

func setAuth0Roles(env string) {
	switch env {
	case "dev":
		auth0RoleIDs = [4]string{"rol_6tBbx6gNRYgb47wM", "rol_510TUetL44xR7zmm", "rol_42KN8JcK3EgysI0Q", "rol_JbKBz2HaApjrd7yW"}
		roleMap = map[string]string{
			"rol_6tBbx6gNRYgb47wM": "a2e39cf5-e016-44a4-b037-057a16fe14fc", // Auth0 Admin -> Orchard Admin
			"rol_510TUetL44xR7zmm": "a2e39cf5-e016-44a4-b037-057a16fe14fc", // Auth0 Manager -> Orchard Manager
			"rol_42KN8JcK3EgysI0Q": "aaff61e7-d5e1-4cf6-9682-00f4f38bf1f5", // Auth0 Super Admin -> Orchard System Admin
			"rol_JbKBz2HaApjrd7yW": "6f71019d-25cf-4c6a-a31e-bdd25472ba26", // Auth0 User -> Orchard Rep
		}
	}
}

var auth0RoleIDs = [...]string{"rol_UIi2xZo0vnZaqRnt", "rol_TCmq5pc9HcZmtDuE", "rol_3kCcp3iLbE6ydZgh", "rol_IEdn24FuqAqxbVro"}

var roleMap = map[string]string{
	"rol_UIi2xZo0vnZaqRnt": "a2e39cf5-e016-44a4-b037-057a16fe14fc", // Auth0 Admin -> Orchard Admin
	"rol_TCmq5pc9HcZmtDuE": "a2e39cf5-e016-44a4-b037-057a16fe14fc", // Auth0 Manager -> Orchard Manager
	"rol_3kCcp3iLbE6ydZgh": "aaff61e7-d5e1-4cf6-9682-00f4f38bf1f5", // Auth0 Super Admin -> Orchard System Admin
	"rol_IEdn24FuqAqxbVro": "6f71019d-25cf-4c6a-a31e-bdd25472ba26", // Auth0 User -> Orchard Rep
}

var rolePriorities = map[string]int{
	"6f71019d-25cf-4c6a-a31e-bdd25472ba26": 1, // Rep
	"a2e39cf5-e016-44a4-b037-057a16fe14fc": 2, // Manager
	"8d94bd88-78a5-467c-a0d8-079f26b412d9": 3, // Admin
	"aaff61e7-d5e1-4cf6-9682-00f4f38bf1f5": 4, // Super Admin
}

var roleTypes = map[string]string{
	"6f71019d-25cf-4c6a-a31e-bdd25472ba26": "ic",       // Rep
	"a2e39cf5-e016-44a4-b037-057a16fe14fc": "manager",  // Manager
	"8d94bd88-78a5-467c-a0d8-079f26b412d9": "manager",  // Admin
	"aaff61e7-d5e1-4cf6-9682-00f4f38bf1f5": "internal", // Super Admin
}

func getAuth0UserRoleMap(roleUsers map[string][]*orchard.Person) map[string]string {
	userRoleMap := map[string]string{}
	for roleID, people := range roleUsers {
		for _, person := range people {
			key := person.Email
			newSystemRole := roleMap[roleID]
			if systemRole, ok := userRoleMap[key]; !ok || rolePriorities[newSystemRole] > rolePriorities[systemRole] {
				userRoleMap[key] = newSystemRole
			}
		}
	}
	return userRoleMap
}
