package config

type Config struct {
	ProjectID         string `env:"PROJECT_ID" envDefault:"local"`
	GRPCHost          string `env:"GRPC_HOST" envDefault:"127.0.0.1"`
	GRPCPort          int    `env:"GRPC_PORT" envDefault:"50051"`
	DBHost            string `env:"DB_HOST" envDefault:"127.0.0.1"`
	DBPort            int    `env:"DB_PORT" envDefault:"5432"`
	DBPassword        string `env:"DB_PASSWORD" envDefault:""`
	DBUser            string `env:"DB_USER" envDefault:"postgres"`
	DBName            string `env:"DB_NAME" envDefault:"tenant"`
	DBMaxConnections  int    `env:"DB_MAX_CONNECTIONS" envDefault:"25"`
	DBDebug           bool   `env:"DB_DEBUG" envDefault:"false"`
	TenantServiceAddr string `env:"TENANT_SERVICE_ADDR" envDefault:""`
	CRMServiceAddr    string `env:"CRM_SERVICE_ADDR" envDefault:""`
	Auth0Issuer       string `env:"AUTH_0_ISSUER" envDefault:"auth.loupe.co"`
	Auth0Audience     string `env:"AUTH_0_AUDIENCE" envDefault:"Ub9IKZnGYUh7oM42iPBumI32cLWmVNWC"`
	Auth0Domain       string `env:"AUTH_0_DOMAIN" envDefault:"https://loupe.auth0.com/"`
	Auth0ClientID     string `env:"AUTH_0_CLIENT_ID" envDefault:""`
	Auth0ClientSecret string `env:"AUTH_0_CLIENT_SECRET" envDefault:""`
	BouncerAddr       string `env:"BOUNCER_ADDR" envDefault:"" json:"bouncerAddr"`
	RedisHost         string `env:"REDIS_HOST" envDefault:""`
	RedisUser         string `env:"REDIS_USER" envDefault:""`
	RedisPassword     string `env:"REDIS_PASS" envDefault:""`
}
