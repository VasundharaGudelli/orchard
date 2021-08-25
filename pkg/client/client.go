package client

import (
	configUtil "github.com/loupe-co/go-common/config"
	"github.com/loupe-co/go-common/errors"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc"
)

type OrchardClientConfig struct {
	Addr string `env:"ORCHARD_ADDR" envDefault:"" json:"orchardAddr" yaml:"orchardAddr"`
}

type OrchardClient struct {
	conn   *grpc.ClientConn
	client servicePb.OrchardClient
}

func New(addr string) (*OrchardClient, error) {
	cfg := OrchardClientConfig{
		Addr: addr,
	}

	// If no address was passed then look for config values in the env automatically
	if addr == "" {
		if err := configUtil.Load(&cfg, configUtil.FromENV()); err != nil {
			return nil, errors.Wrap(err, "error loading orchard client config from env")
		}
	}

	// Establish grpc connection with bouncer service, used for refreshing auth data in cache
	conn, err := grpc.Dial(cfg.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrap(err, "error getting bouncer service connection")
	}

	return &OrchardClient{
		conn:   conn,
		client: servicePb.NewOrchardClient(conn),
	}, nil
}
