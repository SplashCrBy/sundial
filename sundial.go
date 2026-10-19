package sundial

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/sundayfun/sundial/codec"
)

// Client manages one typed configuration document and its in-memory state.
type Client[T any] struct {
	provider Provider
	codec    codec.Codec
	logger   *slog.Logger

	cancelWatch context.CancelFunc
	watchDone   chan struct{}

	writeMu  sync.Mutex
	snapshot atomic.Pointer[snapshot[T]]
}

// Entry pairs a configuration value with its revision.
type Entry[T any] struct {
	// Value is shared and read-only when returned by GetEntry, Update,
	// RestoreRevision or OnChange.
	Value T
	// Revision describes the immutable revision paired with Value.
	Revision Revision
}

// New loads an existing configuration and reloads it until ctx is canceled or Close is called.
// Get and GetEntry return shared read-only values; use Update to publish changes.
func New[T any](ctx context.Context, provider Provider, opts ...Option[T]) (*Client[T], error) {
	normalized := normalizeOptions(opts)
	s := &Client[T]{
		provider:    provider,
		codec:       normalized.Codec,
		logger:      normalized.Logger,
		cancelWatch: nil,
		watchDone:   nil,
		writeMu:     sync.Mutex{},
		snapshot:    atomic.Pointer[snapshot[T]]{},
	}

	loaded, err := s.loadSnapshot(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "load configuration", "error", err)
		return nil, err
	}
	s.snapshot.Store(loaded)
	s.logger.DebugContext(ctx, "loaded configuration", "revision_id", loaded.revision.ID)

	watchCtx, cancel := context.WithCancel(ctx)
	s.cancelWatch = cancel
	s.watchDone = make(chan struct{})
	go func() {
		defer close(s.watchDone)
		defer cancel()
		s.watch(watchCtx, normalized.Reload)
	}()

	return s, nil
}

// Close stops automatic reloads and waits for the background watcher to exit.
// It is safe to call repeatedly or concurrently and does not cancel the parent
// context. Close always returns nil. Reads retain the last snapshot; explicit
// operations such as Update and Reload still use their own contexts.
// Close must not be called synchronously from a provider Watch method or an
// OnChange/OnError callback, because it waits for that same watcher to return.
func (s *Client[T]) Close() error {
	s.cancelWatch()
	<-s.watchDone
	return nil
}

// Get returns the current shared read-only configuration without encoding or decoding.
// Callers must not modify its maps, slices, pointers or other referenced data.
// Use GetEntry when the configuration and its revision are needed together.
func (s *Client[T]) Get() T {
	return s.snapshot.Load().value
}

// GetEntry returns the current configuration and its revision from one atomic read.
// Its Value follows the same shared read-only contract as Get.
func (s *Client[T]) GetEntry() Entry[T] {
	return s.entry(s.snapshot.Load())
}

// Update decodes the current snapshot's source document into an independent draft, applies modify,
// and publishes it using that snapshot's revision. Local writes and reloads are
// serialized for the entire operation; storage still enforces revision CAS.
// If modify, encoding, decoding or publication fails, the accepted snapshot is preserved.
// The returned Entry is shared and read-only. modify must not call Update,
// Reload or RestoreRevision on this Client because they acquire the same lock.
func (s *Client[T]) Update(ctx context.Context, modify func(*T) error) (Entry[T], error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	current := s.snapshot.Load()
	draft, err := decodeConfig[T](s.codec, current.data)
	if err != nil {
		updateErr := fmt.Errorf("sundial: decode draft: %w", err)
		s.logger.ErrorContext(ctx, "update configuration", "error", updateErr)
		return Entry[T]{}, updateErr
	}
	if modifyErr := modify(&draft); modifyErr != nil {
		return Entry[T]{}, modifyErr
	}

	data, err := s.codec.Encode(draft)
	if err != nil {
		updateErr := fmt.Errorf("sundial: encode configuration: %w", err)
		s.logger.ErrorContext(ctx, "update configuration", "error", updateErr)
		return Entry[T]{}, updateErr
	}
	var zeroRevision Revision
	next, err := s.decodeSnapshot(
		data,
		zeroRevision,
	)
	if err != nil {
		s.logger.ErrorContext(ctx, "update configuration", "error", err)
		return Entry[T]{}, err
	}
	revision, err := s.provider.PutIfRevision(ctx, data, current.revision.ID)
	if err != nil {
		updateErr := fmt.Errorf("sundial: update configuration: %w", err)
		s.logger.ErrorContext(ctx, "update configuration", "error", updateErr)
		return Entry[T]{}, updateErr
	}

	next.revision = revision
	s.snapshot.Store(next)
	s.logger.DebugContext(ctx, "update configuration", "revision_id", revision.ID)
	return s.entry(next), nil
}

func (s *Client[T]) loadSnapshot(ctx context.Context) (*snapshot[T], error) {
	data, revision, err := s.provider.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("sundial: get configuration: %w", err)
	}

	return s.decodeSnapshot(data, revision)
}
