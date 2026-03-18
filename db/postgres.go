package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

// postgresClient implements the DB interface for PostgreSQL
type postgresClient struct {
	db     *sql.DB
	config ConnectionConfig
}

// NewPostgresClient creates a new PostgreSQL database client with the given configuration
func NewPostgresClient(dsn string, config ConnectionConfig) (DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	// Configure connection pool
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	}
	if config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("%w: %v", ErrConnectionFailed, err)
	}

	return &postgresClient{
		db:     db,
		config: config,
	}, nil
}

// ExecContext executes a query without returning any rows
func (c *postgresClient) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// PrepareContext creates a prepared statement
func (c *postgresClient) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return c.db.PrepareContext(ctx, query)
}

// QueryContext executes a query that returns multiple rows
func (c *postgresClient) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that returns a single row
func (c *postgresClient) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

// WithTransaction executes a function within a database transaction
func (c *postgresClient) WithTransaction(ctx context.Context, isolationLevel sql.IsolationLevel, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: isolationLevel,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(ctx, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("transaction failed: %v, rollback failed: %w", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Close closes the database connection
func (c *postgresClient) Close() error {
	return c.db.Close()
}

// Ping verifies the database connection
func (c *postgresClient) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// PingContext verifies the database connection (alias for Ping)
func (c *postgresClient) PingContext(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// IsTimeoutError checks if an error is a database timeout error
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context deadline exceeded
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for PostgreSQL timeout errors
	if pqErr, ok := err.(*pq.Error); ok {
		// PostgreSQL error codes for timeout-related errors
		timeoutCodes := map[string]bool{
			"57014": true, // query_canceled
			"57013": true, // statement_timeout
			"57012": true, // canceling_statement_due_to_timeout
			"40001": true, // serialization_failure (often due to lock timeout)
		}
		if timeoutCodes[string(pqErr.Code)] {
			return true
		}
	}

	// Check error message for timeout-related strings
	errStr := strings.ToLower(err.Error())
	timeoutKeywords := []string{
		"timeout",
		"deadline exceeded",
		"context deadline",
		"connection timed out",
		"i/o timeout",
		"query_canceled",
	}

	for _, keyword := range timeoutKeywords {
		if strings.Contains(errStr, keyword) {
			return true
		}
	}

	return false
}

// IsDuplicateKeyError checks if an error is a duplicate key/unique constraint violation
func IsDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}

	// Check for PostgreSQL unique violation
	if pqErr, ok := err.(*pq.Error); ok {
		// 23505 is PostgreSQL's unique_violation error code
		if pqErr.Code == "23505" {
			return true
		}
	}

	// Check error message for duplicate key indicators
	errStr := strings.ToLower(err.Error())
	duplicateKeywords := []string{
		"duplicate key",
		"unique constraint",
		"unique violation",
		"already exists",
		"23505",
	}

	for _, keyword := range duplicateKeywords {
		if strings.Contains(errStr, keyword) {
			return true
		}
	}

	return false
}
