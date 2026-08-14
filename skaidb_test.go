package skaidb

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A context override picks the level for one statement; anything else keeps
// the connection's. An unrecognised level must ERROR rather than silently
// falling back — a read that quietly used the wrong consistency is worse
// than one that fails.
func TestConsistencyForResolvesPerStatement(t *testing.T) {
	c := &conn{consistency: consistencyQuorum}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want byte
	}{
		{"nil context", nil, consistencyQuorum},
		{"no override", context.Background(), consistencyQuorum},
		{"one", WithConsistency(context.Background(), "one"), consistencyOne},
		{"all", WithConsistency(context.Background(), "all"), consistencyAll},
		{"quorum", WithConsistency(context.Background(), "quorum"), consistencyQuorum},
		{"case-insensitive", WithConsistency(context.Background(), "ONE"), consistencyOne},
	} {
		got, err := c.consistencyFor(tc.ctx)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
	if _, err := c.consistencyFor(WithConsistency(context.Background(), "eventual")); err == nil {
		t.Fatal("an unknown consistency level must be rejected, not ignored")
	}
}

// Cancelling the context must unblock a read that would otherwise hang,
// report ctx.Err() rather than a raw i/o timeout, and retire the connection:
// the statement was interrupted mid-frame, so the stream can no longer be
// trusted and database/sql must not hand it to the next caller.
func TestApplyContextCancelUnblocksAndRetiresConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &conn{nc: client, consistency: consistencyQuorum}

	ctx, cancel := context.WithCancel(context.Background())
	finish := c.applyContext(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Nothing is ever written to the pipe, so this blocks until the watcher
	// forces a deadline.
	buf := make([]byte, 1)
	_, readErr := client.Read(buf)
	if readErr == nil {
		t.Fatal("expected the blocked read to fail once the context was cancelled")
	}
	if err := finish(readErr); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if c.IsValid() {
		t.Fatal("an interrupted connection must not be reused: IsValid should be false")
	}
}

// A deadline that has already passed behaves the same way and surfaces as
// context.DeadlineExceeded.
func TestApplyContextDeadlineSurfacesAsDeadlineExceeded(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &conn{nc: client, consistency: consistencyQuorum}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	finish := c.applyContext(ctx)

	buf := make([]byte, 1)
	_, readErr := client.Read(buf)
	if readErr == nil {
		t.Fatal("expected the blocked read to time out")
	}
	if err := finish(readErr); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
}

// Without a context there is nothing to watch and no deadline to clear, and
// the connection stays usable.
func TestApplyContextBackgroundLeavesConnUsable(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &conn{nc: client, consistency: consistencyQuorum}

	finish := c.applyContext(context.Background())
	if err := finish(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !c.IsValid() {
		t.Fatal("an uncancelled statement must leave the connection usable")
	}
}
