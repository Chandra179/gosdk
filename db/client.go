package db

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrDatabaseTimeout is returned when a database operation times out
var ErrDatabaseTimeout = errors.New("database operation timed out")

// ErrDuplicateKey is returned when a unique constraint is violated
var ErrDuplicateKey = errors.New("duplicate key violation")

// ErrConnectionFailed is returned when database connection fails
var ErrConnectionFailed = errors.New("database connection failed")

// DBTX is the interface for database operations that both *sql.DB and *sql.Tx implement.
// This is compatible with sqlc's generated interface and allows using either
// a connection pool or a transaction.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// SQLExecutor extends DBTX with transaction management capabilities.
// This interface is used by repositories that need transaction support.
type SQLExecutor interface {
	DBTX
	// WithTransaction executes a function within a database transaction.
	// The transaction is automatically committed if the function returns nil,
	// or rolled back if the function returns an error.
	WithTransaction(ctx context.Context, isolationLevel sql.IsolationLevel, fn func(ctx context.Context, tx *sql.Tx) error) error
}

// ConnectionConfig holds connection pool configuration
type ConnectionConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	QueryTimeout    time.Duration
}

// DB is the database client interface that provides both query capabilities
// and connection management
type DB interface {
	SQLExecutor
	Close() error
	Ping(ctx context.Context) error
	PingContext(ctx context.Context) error
}
