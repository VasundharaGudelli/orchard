package handlers

import (
	"context"
	"sync"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) GetLegacyTeamStructure(ctx context.Context, in *servicePb.GetLegacyTeamStructureRequest) (*servicePb.GetLegacyTeamStructureResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	personSvc := h.db.NewPersonService()

	flatGroups, err := svc.GetFullTenantTree(ctx, in.TenantId, true)
	if err != nil {
		err := errors.Wrap(err, "error getting full tenant team tree from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Convert db models to protos
	flatProtos := make([]*servicePb.GroupWithMembers, len(flatGroups))
	for i, g := range flatGroups {
		group, err := svc.ToProto(&g.Group)
		if err != nil {
			err := errors.Wrap(err, "error converting group db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		members := make([]*orchardPb.Person, len(g.Members))
		for j, p := range g.Members {
			members[j], err = personSvc.ToProto(&p)
			if err != nil {
				err := errors.Wrap(err, "error converting person db model to proto")
				logger.Error(err)
				return nil, err.AsGRPC()
			}
		}
		flatProtos[i] = &servicePb.GroupWithMembers{
			Group:    group,
			Members:  members,
			Children: []*servicePb.GroupWithMembers{},
		}
	}

	// Form tree structure
	roots := []*servicePb.GroupWithMembers{}
	for _, g := range flatProtos {
		if g.Group.ParentId == "" {
			roots = append(roots, g)
		}
	}
	if len(roots) == 0 {
		return &servicePb.GetLegacyTeamStructureResponse{
			LegacyTeam: []*servicePb.GroupWithMembers{},
		}, nil
	}

	wg := sync.WaitGroup{}
	for _, root := range roots {
		wg.Add(1)
		go func(w *sync.WaitGroup, r *servicePb.GroupWithMembers, all []*servicePb.GroupWithMembers) {
			recursivelyGetGroupChildren(r, all, 1, false)
			w.Done()
		}(&wg, root, flatProtos)
	}
	wg.Wait()

	return &servicePb.GetLegacyTeamStructureResponse{
		LegacyTeam: roots,
	}, nil
}
