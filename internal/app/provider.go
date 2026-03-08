package app

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"gosdk/internal/app/bootstrap"
	"gosdk/internal/cfg"
	"gosdk/internal/service/auth"
	"gosdk/internal/service/session"
	"gosdk/pkg/cache"
	"gosdk/pkg/db"
	"gosdk/pkg/logger"
	"gosdk/pkg/oauth2"
)

// Infrastructure holds all infrastructure dependencies (databases, caches, external services).
// These are stateful resources that require lifecycle management (initialization and shutdown).
type Infrastructure struct {
	DB             db.DB
	Cache          cache.Cache
	OAuth2Manager  *oauth2.Manager
	Logger         *logger.AppLogger
	MetricsHandler http.Handler
	shutdownOTel   func(context.Context) error
}

// Close gracefully shuts down all infrastructure resources.
// Resources are closed in reverse order of initialization.
func (i *Infrastructure) Close(ctx context.Context) error {
	var errs []error

	if i.DB != nil {
		i.Logger.Info(ctx, "Closing database connections")
		if err := i.DB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("database shutdown: %w", err))
		}
	}

	if i.Cache != nil {
		i.Logger.Info(ctx, "Closing cache connections")
		if err := i.Cache.Close(); err != nil {
			errs = append(errs, fmt.Errorf("cache shutdown: %w", err))
		}
	}

	if i.shutdownOTel != nil {
		i.Logger.Info(ctx, "Shutting down observability")
		if err := i.shutdownOTel(ctx); err != nil {
			errs = append(errs, fmt.Errorf("observability shutdown: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("infrastructure shutdown errors: %v", errs)
	}

	return nil
}

// Services holds all domain service dependencies.
// These are stateless business logic components that depend on Infrastructure.
type Services struct {
	Auth    *auth.Service
	Session session.Client
}

// Provider is the composition root that wires Infrastructure and Services together.
// It serves as the dependency injection container for the application.
type Provider struct {
	Infra    *Infrastructure
	Services *Services
	Config   *cfg.Config
}

// NewProvider creates and initializes all application dependencies.
// This is the single place where the dependency graph is constructed.
func NewProvider(ctx context.Context, config *cfg.Config) (*Provider, error) {
	appLogger := logger.NewLogger(config.AppEnv)
	appLogger.Info(ctx, "Initializing application provider...")

	shutdownOTel, metricsHandler, err := bootstrap.InitOtel(ctx, &config.Observability, config.Observability.SamplerRatio)
	if err != nil {
		return nil, fmt.Errorf("observability setup: %w", err)
	}

	infra, err := initBaseInfrastructure(config, appLogger, shutdownOTel, metricsHandler)
	if err != nil {
		if shutdownErr := shutdownOTel(ctx); shutdownErr != nil {
			log.Printf("Warning: failed to shutdown OTel during init failure: %v", shutdownErr)
		}
		return nil, fmt.Errorf("infrastructure initialization: %w", err)
	}

	sessionStore := session.NewRedisStore(infra.Cache)

	// Create auth service with OAuth2 callback handler
	// Note: We pass nil for OAuth2 manager initially, it will be set after OAuth2 initialization
	authService := auth.NewService(sessionStore, infra.DB)
	oauth2Manager, err := bootstrap.InitOAuth2(ctx, &config.OAuth2, authService.OAuthCallbackHandler())
	if err != nil {
		// return nil, fmt.Errorf("oauth2 initialization: %w", err)
	} else {
		infra.OAuth2Manager = oauth2Manager
		authService.SetOAuth2Manager(oauth2Manager)
	}

	services := &Services{
		Auth:    authService,
		Session: sessionStore,
	}

	appLogger.Info(ctx, "Application provider initialized successfully")

	return &Provider{
		Infra:    infra,
		Services: services,
		Config:   config,
	}, nil
}

// initBaseInfrastructure initializes infrastructure dependencies except OAuth2 and Kafka.
// Order matters: resources are initialized from bottom up.
func initBaseInfrastructure(
	config *cfg.Config,
	appLogger *logger.AppLogger,
	shutdownOTel func(context.Context) error,
	metricsHandler http.Handler,
) (*Infrastructure, error) {
	infra := &Infrastructure{
		Logger:         appLogger,
		MetricsHandler: metricsHandler,
		shutdownOTel:   shutdownOTel,
	}

	dsn := buildPostgresDSN(&config.Postgres)
	connConfig := db.ConnectionConfig{
		MaxOpenConns:    config.Postgres.MaxOpenConns,
		MaxIdleConns:    config.Postgres.MaxIdleConns,
		ConnMaxLifetime: config.Postgres.ConnMaxLifetime,
		ConnMaxIdleTime: config.Postgres.ConnMaxIdleTime,
		QueryTimeout:    config.Postgres.QueryTimeout,
	}

	dbClient, err := bootstrap.InitDatabase(dsn, connConfig)
	if err != nil {
		return nil, fmt.Errorf("database initialization: %w", err)
	}
	infra.DB = dbClient

	bootstrap.RunMigrations(dsn)

	infra.Cache = bootstrap.InitCache(config.Redis.Host, config.Redis.Port)

	return infra, nil
}

// buildPostgresDSN builds PostgreSQL connection string from config.
func buildPostgresDSN(cfg *cfg.PostgresConfig) string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.DBName,
		cfg.SSLMode,
	)
}
