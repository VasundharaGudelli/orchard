package grpchandlers

import (
	"context"

	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) GetUserTeam(ctx context.Context, in *servicePb.GetUserTeamRequest) (*servicePb.GetUserTeamResponse, error) {
	return nil, nil
}
