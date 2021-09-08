package main

import (
	"math"
	"os"
	"syscall"

	bouncer "github.com/loupe-co/bouncer/pkg/client"
	common "github.com/loupe-co/go-common"
	configUtil "github.com/loupe-co/go-common/config"
	ekg "github.com/loupe-co/go-common/ekg"
	"github.com/loupe-co/go-loupe-logger/log"
	grpcHandlers "github.com/loupe-co/orchard/cmd/server/grpc"
	"github.com/loupe-co/orchard/internal/clients"
	"github.com/loupe-co/orchard/internal/config"
	"github.com/loupe-co/orchard/internal/db"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc"
)

func main() {
	// Get the service config
	cfg := config.Config{}
	err := configUtil.Load(
		&cfg,
		configUtil.FromENV(),
		configUtil.FromLocalYAML("/var/app-secrets/credentials_postgres.yaml"),
		configUtil.FromLocalYAML("/var/app-secrets/credentials_redis.yaml"),
		configUtil.FromLocalYAML("/var/app-secrets/credentials_sentry.yaml"),
		configUtil.FromLocalYAML("/var/app-secrets/credentials_auth0.yaml"),
		configUtil.SetDefaultENV("project", "local"),
		configUtil.SetExportENVFromConfig(true),
	)

	if err != nil {
		panic("Error parsing config from environment")
	}

	// Initialize/Get logger
	l := log.InitLogger()
	defer l.Close()

	// Init db/sqlboiler
	dbClient, err := db.New(cfg)
	if err != nil {
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

	bouncerClient, err := bouncer.NewBouncerClient(
		bouncer.SetBouncerAddr(cfg.BouncerAddr),
		bouncer.SetRedisHost(cfg.RedisHost),
		bouncer.SetRedisPass(cfg.RedisPassword),
	)
	if err != nil {
		log.Errorf("error getting bouncer client: %s", err.Error())
		return
	}

	auth0Client := clients.NewAuth0Client(cfg)

	// Create grpc server
	orchardServer := grpcHandlers.New(cfg, dbClient, tenantClient, crmClient, auth0Client, bouncerClient)
	grpcServer := common.NewGRPCServer(cfg.GRPCHost, cfg.GRPCPort, grpc.MaxRecvMsgSize(math.MaxInt32), grpc.MaxSendMsgSize(math.MaxInt32))
	grpcServer.Register(func(server *grpc.Server) {
		servicePb.RegisterOrchardServer(server, orchardServer)
	})

	// Create EKG server (AKA health checks)
	ekgServer := ekg.New()
	ekgServer.Handle("sql", db.DefaultHealthCheckPolicy, dbClient.HealthCheck)

	// Setup os signal server/handler
	sigServer := common.NewSigServer()
	sigServer.Handle(func(sig os.Signal) error {
		log.WithCustom("signal", sig.String()).Info("received os signal")
		// TODO: how do we handle these signals in this service, may not matter?
		return nil
	}, os.Interrupt, syscall.SIGTERM)

	// Start all servers and wait for any to error
	log.Info("Server starting")
	if err := common.ServerListenMux(sigServer, grpcServer, ekgServer); err != nil {
		log.Error(err)
	}
	log.Info("Server exiting")
}
