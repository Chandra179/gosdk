package cache

import (
	"context"
	"time"
)

type Cache interface {
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, key string) error
	Ping(ctx context.Context) error
	Close() error
}
