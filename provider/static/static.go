// Package static provides a read-only Sundial Provider for a fixed document.
package static

import (
	"bytes"
	"context"

	"github.com/sundayfun/sundial"
)

// Provider holds an immutable document with a zero revision and no history.
type Provider struct {
	data []byte
}

var (
	_ sundial.Provider = (*Provider)(nil)
	_ sundial.Watcher  = (*Provider)(nil)
)

// NewProvider creates a read-only Provider from a copy of data.
// The Client's codec decodes and validates the document when loading it.
func NewProvider(data []byte) *Provider {
	return &Provider{data: bytes.Clone(data)}
}

// Get returns a copy of the fixed document and a zero revision.
func (p *Provider) Get(ctx context.Context) ([]byte, sundial.Revision, error) {
	if err := ctx.Err(); err != nil {
		return nil, sundial.Revision{}, err
	}
	var revision sundial.Revision
	return bytes.Clone(p.data), revision, nil
}

// Put rejects writes without changing the document.
func (*Provider) Put(context.Context, []byte) (sundial.Revision, error) {
	return sundial.Revision{}, sundial.ErrReadOnly
}

// PutIfRevision rejects writes regardless of the supplied revision.
func (*Provider) PutIfRevision(context.Context, []byte, string) (sundial.Revision, error) {
	return sundial.Revision{}, sundial.ErrReadOnly
}

// Watch notifies once, then waits for cancellation without polling.
func (*Provider) Watch(ctx context.Context, notify func() error) error {
	if err := notify(); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
