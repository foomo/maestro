package localfs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/foomo/maestro"
	"github.com/foomo/maestro/pkg/blobstore"
)

// Client is a read-only blob source backed by a remote localfs Store HTTP server.
// It satisfies blobstore.BlobReader and is intended for Player wiring.
type Client struct {
	// BaseURL is the URL at which the [Store.Handler] is mounted, without a
	// trailing slash.
	BaseURL string
	// HTTP performs the requests. It must not be nil.
	HTTP *http.Client
}

var _ blobstore.BlobReader = (*Client)(nil)

// NewClient returns a Client for the Store served at baseURL, using
// [http.DefaultClient].
func NewClient(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTP: http.DefaultClient}
}

// Reader fetches file name of version v from the server. It returns an error
// for any response status other than 200 OK. The size is the response's
// Content-Length, or -1 if unknown.
func (c *Client) Reader(ctx context.Context, v maestro.Version, name string) (io.ReadCloser, int64, error) {
	u := fmt.Sprintf("%s/versions/%s/files/%s", c.BaseURL, url.PathEscape(string(v)), name)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	return resp.Body, resp.ContentLength, nil
}
