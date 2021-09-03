package commands

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"
)

func GetUpdateGroupTypesCommand() *cli.Command {
	return &cli.Command{
		Name:  "update-group-types",
		Usage: "update all group types automatically",
		Action: func(c *cli.Context) error {
			env := c.String("env")
			fmt.Println("Attempting to update all group types")
			return updateGroupTypes(context.Background(), env)
		},
	}
}

func updateGroupTypes(ctx context.Context, env string) error {
	setAuth0Roles(env)

	dbClient, err := GetOrchardDB(env)
	if err != nil {
		return err
	}

	tenantSvc := dbClient.NewTenantService()
	groupSvc := dbClient.NewGroupService()

	tenants, err := tenantSvc.GetActiveTenants(ctx)
	if err != nil {
		return err
	}

	for _, tenant := range tenants {
		fmt.Println("Updating group types for tenant", tenant.ID)
		if err := groupSvc.UpdateGroupTypes(ctx, tenant.ID); err != nil {
			return err
		}
	}

	fmt.Println("Done updating group types")

	return nil
}
