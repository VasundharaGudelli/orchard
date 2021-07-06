package config

type Config struct {
	ProjectID        string `env:"PROJECT_ID" envDefault:"local"`
	GRPCHost         string `env:"GRPC_HOST" envDefault:"127.0.0.1"`
	GRPCPort         int    `env:"GRPC_PORT" envDefault:"50051"`
	DBHost           string `env:"DB_HOST" envDefault:"127.0.0.1"`
	DBPort           int    `env:"DB_PORT" envDefault:"5432"`
	DBPassword       string `env:"DB_PASSWORD" envDefault:""`
	DBUser           string `env:"DB_USER" envDefault:"postgres"`
	DBName           string `env:"DB_NAME" envDefault:"tenant"`
	DBMaxConnections int    `env:"DB_MAX_CONNECTIONS" envDefault:"25"`
	DBDebug          bool   `env:"DB_DEBUG" envDefault:"false"`
}
