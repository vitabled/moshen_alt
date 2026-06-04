package s3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Client manages S3 HTTP interactions and produces MuxStream instances.
// It uses a raw net/http.Client with hand-rolled AWS Signature V4
// signing — no AWS SDK dependency to keep the binary small.
type Client struct {
	opts       Options
	httpClient *http.Client
	parsedURL  *url.URL // parsed Endpoint

	muxSession *MuxSession
	muxMu      sync.Mutex
}

// NewClient creates an S3 client from parsed options.
func NewClient(opts Options) (*Client, error) {
	opts.Defaults()

	parsed, err := url.Parse(opts.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("s3: invalid endpoint %q: %w", opts.Endpoint, err)
	}
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}

	return &Client{
		opts:      opts,
		parsedURL: parsed,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

// Dial creates a new MuxStream (multiplexed over the single virtual S3 session).
func (c *Client) Dial(ctx context.Context) (net.Conn, error) {
	c.muxMu.Lock()
	defer c.muxMu.Unlock()

	if c.muxSession == nil || c.muxSession.isClosed() {
		sessionID := c.opts.ClientID
		if sessionID == "" {
			var err error
			sessionID, err = generateSessionID()
			if err != nil {
				return nil, fmt.Errorf("s3: failed to generate session ID: %w", err)
			}
		}
		conn := newS3Conn(c, sessionID)
		c.muxSession = NewMuxSession(conn)
	}

	return c.muxSession.OpenStream()
}

// Close releases HTTP client resources.
func (c *Client) Close() error {
	c.muxMu.Lock()
	if c.muxSession != nil {
		_ = c.muxSession.Close()
		c.muxSession = nil
	}
	c.muxMu.Unlock()
	c.httpClient.CloseIdleConnections()
	return nil
}

// ---------- S3 operations ----------

// putObject uploads data to the specified object key.
func (c *Client) putObject(ctx context.Context, key string, data []byte) error {
	objectURL := c.objectURL(key)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, objectURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	c.signRequest(req, data)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("s3: PUT %s returned %d", key, resp.StatusCode)
	}
	return nil
}

// getObject downloads the specified object key.
// Returns the body bytes and true if the object exists.
// Returns nil, false if the object does not exist (404/NoSuchKey).
func (c *Client) getObject(ctx context.Context, key string) ([]byte, bool, error) {
	objectURL := c.objectURL(key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, objectURL, nil)
	if err != nil {
		return nil, false, err
	}

	c.signRequest(req, nil)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		io.Copy(io.Discard, resp.Body)
		return nil, false, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		io.Copy(io.Discard, resp.Body)
		return nil, false, fmt.Errorf("s3: GET %s returned %d", key, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}
	if len(body) == 0 {
		return nil, false, nil
	}
	return body, true, nil
}

// deleteObject removes the specified object key (best-effort cleanup).
func (c *Client) deleteObject(ctx context.Context, key string) error {
	objectURL := c.objectURL(key)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, objectURL, nil)
	if err != nil {
		return err
	}

	c.signRequest(req, nil)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

// ---------- helpers ----------

// objectURL returns the full URL for an object key.
// Uses path-style addressing: https://endpoint/bucket/key
func (c *Client) objectURL(key string) string {
	return fmt.Sprintf("%s/%s/%s",
		strings.TrimRight(c.parsedURL.String(), "/"),
		c.opts.Bucket,
		key,
	)
}

// c2sKey returns the object key for client-to-server data.
func (c *Client) c2sKey(sessionID string, seq uint64) string {
	return fmt.Sprintf("%s%s/c2s/%06d.bin", c.opts.Prefix, sessionID, seq)
}

// s2cKey returns the object key for server-to-client data.
func (c *Client) s2cKey(sessionID string, seq uint64) string {
	return fmt.Sprintf("%s%s/s2c/%06d.bin", c.opts.Prefix, sessionID, seq)
}

func generateSessionID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ========== AWS Signature V4 (lightweight, no SDK) ==========

// signRequest signs an HTTP request using AWS Signature V4.
func (c *Client) signRequest(req *http.Request, payload []byte) {
	now := time.Now().UTC()
	datestamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	// Payload hash
	var payloadHash string
	if payload != nil {
		h := sha256.Sum256(payload)
		payloadHash = hex.EncodeToString(h[:])
	} else {
		// empty body hash
		h := sha256.Sum256([]byte{})
		payloadHash = hex.EncodeToString(h[:])
	}

	// Required headers
	host := req.URL.Host
	if host == "" {
		host = req.URL.Hostname()
	}
	req.Header.Set("Host", host)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if payload != nil {
		req.Header.Set("Content-Length", fmt.Sprintf("%d", len(payload)))
	}

	// Canonical request
	signedHeaders, canonicalHeaders := c.buildCanonicalHeaders(req)
	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuerystring := req.URL.RawQuery

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuerystring,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	// String to sign
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", datestamp, c.opts.Region)
	canonHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hex.EncodeToString(canonHash[:]),
	}, "\n")

	// Signing key
	signingKey := c.deriveSigningKey(datestamp)
	signature := hmacSHA256(signingKey, []byte(stringToSign))

	// Authorization header
	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.opts.AccessKey,
		credentialScope,
		signedHeaders,
		hex.EncodeToString(signature),
	)
	req.Header.Set("Authorization", authHeader)
}

// buildCanonicalHeaders builds the canonical headers string and the
// signed headers list from the request, as required by AWS SigV4.
func (c *Client) buildCanonicalHeaders(req *http.Request) (signedHeaders, canonicalHeaders string) {
	// Collect headers to sign (lowercase)
	type hdr struct {
		key, val string
	}
	var hdrs []hdr
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		// Only sign host, x-amz-*, and content-type
		if lk == "host" || strings.HasPrefix(lk, "x-amz-") || lk == "content-type" {
			hdrs = append(hdrs, hdr{lk, strings.TrimSpace(vs[0])})
		}
	}
	sort.Slice(hdrs, func(i, j int) bool { return hdrs[i].key < hdrs[j].key })

	var canonical, signed []string
	for _, h := range hdrs {
		canonical = append(canonical, h.key+":"+h.val)
		signed = append(signed, h.key)
	}

	// Canonical headers must end with a newline
	canonicalHeaders = strings.Join(canonical, "\n") + "\n"
	signedHeaders = strings.Join(signed, ";")
	return
}

// deriveSigningKey computes the AWS SigV4 signing key for a given date.
func (c *Client) deriveSigningKey(datestamp string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+c.opts.SecretKey), []byte(datestamp))
	kRegion := hmacSHA256(kDate, []byte(c.opts.Region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
