package sundial_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sundayfun/sundial"
	staticprovider "github.com/sundayfun/sundial/provider/static"
	providertesting "github.com/sundayfun/sundial/provider/testing"
)

// Failure cases: Close returns before Watch exits; canceling one client cancels
// its parent; concurrent/repeated Close panics or blocks; polling continues after
// Close; parent cancellation prevents Close from returning; failed New starts Watch.
type delayedCloseWatcher struct {
	*providertesting.Provider
	canceled chan struct{}
	release  chan struct{}
}

func (p *delayedCloseWatcher) Watch(ctx context.Context, _ func() error) error {
	<-ctx.Done()
	close(p.canceled)
	<-p.release
	return ctx.Err()
}

func TestCloseWaitsForWatcherAndPreservesParent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := &delayedCloseWatcher{
			Provider: providertesting.New([]byte(`{"counter":7}`)),
			canceled: make(chan struct{}), release: make(chan struct{}),
		}
		client, err := sundial.New[testConfig](parent, provider)
		require.NoError(t, err)
		closed := make(chan struct{})
		go func() { client.Close(); close(closed) }()
		<-provider.canceled
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("Close returned before Watch exited")
		default:
		}
		require.NoError(t, parent.Err())
		close(provider.release)
		<-closed
		assert.Equal(t, 7, client.Get().Counter)
		assert.Equal(t, 7, client.GetEntry().Value.Counter)
		client.Close()
	})
}

func TestCloseStopsPolling(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		provider := providertesting.New([]byte(`{"counter":1}`))
		client, err := sundial.New[testConfig](t.Context(), provider)
		require.NoError(t, err)
		synctest.Wait()
		client.Close()
		reads := provider.GetCount()
		provider.SetData([]byte(`{"counter":2}`))
		time.Sleep(time.Minute)
		assert.Equal(t, reads, provider.GetCount())
		assert.Equal(t, 1, client.Get().Counter)
		// Explicit operations continue to use their own contexts after Close.
		_, err = client.Update(t.Context(), func(cfg *testConfig) error { cfg.Counter = 3; return nil })
		require.ErrorIs(t, err, sundial.ErrConflict)
	})
}

func TestCloseConcurrentStaticClient(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client, err := sundial.New[testConfig](t.Context(), staticprovider.NewProvider([]byte(`{"counter":1}`)))
		require.NoError(t, err)
		var group sync.WaitGroup
		for range 8 {
			group.Go(client.Close)
		}
		group.Wait()
		client.Close()
	})
}

func TestCloseAfterParentCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := &cancellationWatcher{Provider: providertesting.New([]byte(`{}`)), stopped: make(chan struct{})}
		client, err := sundial.New[testConfig](ctx, provider)
		require.NoError(t, err)
		synctest.Wait()
		cancel()
		<-provider.stopped
		client.Close()
	})
}

func TestFailedNewDoesNotStartWatcher(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		provider := &delayedCloseWatcher{Provider: providertesting.New(nil), canceled: make(chan struct{}), release: make(chan struct{})}
		client, err := sundial.New[testConfig](t.Context(), provider)
		require.ErrorIs(t, err, sundial.ErrNotFound)
		assert.Nil(t, client)
		// Any Watch goroutine here would remain blocked and fail the synctest bubble.
	})
}
