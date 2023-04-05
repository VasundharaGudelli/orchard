package config

type Config struct {
	ProjectID             string `env:"PROJECT_ID" envDefault:"local"`
	GRPCHost              string `env:"GRPC_HOST" envDefault:"127.0.0.1"`
	GRPCPort              int    `env:"GRPC_PORT" envDefault:"50051"`
	DBHost                string `env:"DB_HOST" envDefault:"127.0.0.1"`
	DBPort                int    `env:"DB_PORT" envDefault:"5432"`
	DBPassword            string `env:"DB_PASSWORD" envDefault:"" yaml:"postgres_password"`
	DBUser                string `env:"DB_USER" envDefault:"postgres" yaml:"postgres_username"`
	DBName                string `env:"DB_NAME" envDefault:"tenant"`
	DBMaxConnections      int    `env:"DB_MAX_CONNECTIONS" envDefault:"25"`
	DBDebug               bool   `env:"DB_DEBUG" envDefault:"false"`
	TenantServiceAddr     string `env:"TENANT_SERVICE_ADDR" envDefault:""`
	CRMServiceAddr        string `env:"CRM_SERVICE_ADDR" envDefault:""`
	Auth0Issuer           string `env:"AUTH_0_ISSUER" envDefault:"auth.loupe.co"`
	Auth0Audience         string `env:"AUTH_0_AUDIENCE" envDefault:"Ub9IKZnGYUh7oM42iPBumI32cLWmVNWC"`
	Auth0Domain           string `env:"AUTH_0_DOMAIN" envDefault:"https://loupe.auth0.com/"`
	Auth0ClientID         string `env:"AUTH_0_CLIENT_ID" envDefault:"" yaml:"auth0_key"`
	Auth0ClientSecret     string `env:"AUTH_0_CLIENT_SECRET" envDefault:"" yaml:"auth0_secret"`
	Auth0RoleIDSuperAdmin string `env:"AUTH_0_ROLE_SUPER_ADMIN" envDefault:"rol_42KN8JcK3EgysI0Q"`
	Auth0RoleIDAdmin      string `env:"AUTH_0_ROLE_ADMIN" envDefault:"rol_6tBbx6gNRYgb47wM"`
	Auth0RoleIDManager    string `env:"AUTH_0_ROLE_MANAGER" envDefault:"rol_510TUetL44xR7zmm"`
	Auth0RoleIDUser       string `env:"AUTH_0_ROLE_USER" envDefault:"rol_JbKBz2HaApjrd7yW"`
	BouncerAddr           string `env:"BOUNCER_ADDR" envDefault:"" json:"bouncerAddr"`
	RedisHost             string `env:"REDIS_HOST" envDefault:""`
	RedisUser             string `env:"REDIS_USER" envDefault:""`
	RedisPassword         string `env:"REDIS_PASS" envDefault:"" yaml:"redis_password"`
	SentryDSN             string `env:"SENTRY_DSN" envDefault:"" yaml:"sentry_dsn" exportENV:"SENTRY_DSN"`
	SyncUsersBatchSize    int    `env:"SYNC_USERS_BATCH_SIZE" envDefault:"1000"`
}
