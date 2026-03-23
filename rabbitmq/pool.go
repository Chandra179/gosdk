package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

// channelPool manages a pool of pre-allocated AMQP channels for publishing.
// It is safe for concurrent use.
type channelPool struct {
	channels chan *amqp.Channel
	size     int
	closed   atomic.Bool
	mu       sync.Mutex
}

func newChannelPool(size int) *channelPool {
	return &channelPool{
		channels: make(chan *amqp.Channel, size),
		size:     size,
	}
}

// fill populates the pool with channels from the given connection.
func (p *channelPool) fill(conn *amqp.Connection, enableConfirms bool) error {
	for i := 0; i < p.size; i++ {
		ch, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to create channel %d: %w", i, err)
		}

		if enableConfirms {
			if err := ch.Confirm(false); err != nil {
				ch.Close()
				return fmt.Errorf("failed to enable confirms on channel %d: %w", i, err)
			}
		}

		p.channels <- ch
	}
	return nil
}

// get retrieves a channel from the pool, respecting context cancellation.
func (p *channelPool) get(ctx context.Context) (*amqp.Channel, error) {
	if p.closed.Load() {
		return nil, fmt.Errorf("channel pool is closed")
	}

	select {
	case ch := <-p.channels:
		if ch != nil && !ch.IsClosed() {
			return ch, nil
		}
		return nil, fmt.Errorf("channel is closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// returnCh returns a channel to the pool. Closed or nil channels are discarded.
func (p *channelPool) returnCh(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}

	if p.closed.Load() {
		ch.Close()
		return
	}

	select {
	case p.channels <- ch:
	default:
		ch.Close()
	}
}

// drain removes and closes all channels currently in the pool.
func (p *channelPool) drain() {
	for {
		select {
		case ch := <-p.channels:
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
		default:
			return
		}
	}
}

// close permanently shuts down the pool and closes all remaining channels.
// After close, get returns an error and returnCh discards channels.
func (p *channelPool) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.channels)
	for ch := range p.channels {
		if ch != nil && !ch.IsClosed() {
			ch.Close()
		}
	}
}
