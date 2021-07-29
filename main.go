package main

import (
	"math"
	"os"
	"syscall"

	common "github.com/loupe-co/go-common"
	configUtil "github.com/loupe-co/go-common/config"
	ekg "github.com/loupe-co/go-common/ekg"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/clients"
	"github.com/loupe-co/orchard/config"
	"github.com/loupe-co/orchard/db"
	grpcHandlers "github.com/loupe-co/orchard/handlers/grpc"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc"
)

func main() {
	// Get config
	cfg := config.Config{}
	err := configUtil.Load(&cfg, configUtil.FromENV(), configUtil.SetDefaultENV("project", "local"))
	if err != nil {
		panic("Error parsing config from environment")
	}

	// Initialize/Get logger
	l := log.InitLogger()
	defer l.Close()

	// Init db/sqlboiler
	if err := db.Init(cfg); err != nil {
		log.Errorf("error initializing database connection: %s", err.Error())
		return
	}

	// Get service clients
	tenantClient, err := clients.NewTenantClient(cfg)
	if err != nil {
		log.Errorf("error getting tenant-service client: %s", err.Error())
		return
	}

	crmClient, err := clients.NewCRMClient(cfg)
	if err != nil {
		log.Errorf("error getting crm client: %s", err.Error())
		return
	}

	// Create grpc server
	orchardServer := grpcHandlers.NewOrchardGRPCServer(cfg, tenantClient, crmClient)
	grpcServer := common.NewGRPCServer(cfg.GRPCHost, cfg.GRPCPort, grpc.MaxRecvMsgSize(math.MaxInt32), grpc.MaxSendMsgSize(math.MaxInt32))
	grpcServer.Register(func(server *grpc.Server) {
		servicePb.RegisterOrchardServer(server, orchardServer)
	})

	// Create EKG server (AKA health checks)
	ekgServer := ekg.New()
	ekgServer.Handle("sql", db.DefaultHealthCheckPolicy, db.HealthCheck)

	// Setup os signal server/handler
	sigServer := common.NewSigServer()
	sigServer.Handle(func(sig os.Signal) error {
		log.Infof("handling os signal %s", sig.String())
		// TODO: how do we handle these signals in this service, may not matter?
		return nil
	}, os.Interrupt, syscall.SIGTERM)

	// Start all servers and wait for any to error
	log.Info("Server starting")
	if err := common.ServerListenMux(sigServer, grpcServer); err != nil {
		log.Error(err)
	}
	log.Info("Server exiting")
}
