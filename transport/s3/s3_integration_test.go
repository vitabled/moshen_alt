//go:build s3test
// +build s3test

// Integration test for the S3 transport layer.
// Run with:  go test -v -tags s3test -run TestS3 -count=1 ./transport/s3/
//
// Fill in your credentials below before running.

package s3

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ========== FILL THESE IN ==========
const (
	testEndpoint  = "https://s3.ru1.storage.beget.cloud" // your S3 endpoint
	testRegion    = "ru1"                                // your region
	testBucket    = "304a6d5bed63-gymlogappbucket"       // your bucket name
	testAccessKey = "UMP8RBNHXZEHRPL1TVGT"
	testSecretKey = "LQzo4eDzFlMGkmVCLbDhDAS6vHWrQtFGV2YHON97"
	testPrefix    = "test-tunnel/"
)

// ===================================

func testClient(t *testing.T) *Client {
	t.Helper()
	opts := Options{
		Endpoint:  testEndpoint,
		Region:    testRegion,
		Bucket:    testBucket,
		AccessKey: testAccessKey,
		SecretKey: testSecretKey,
		Prefix:    testPrefix,
		PollMinMs: 100,
		PollMaxMs: 1000,
		ChunkSize: 4096,
	}
	c, err := NewClient(opts)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	return c
}

// TestS3PutGet verifies that SigV4 signing works and we can PUT/GET objects.
func TestS3PutGet(t *testing.T) {
	c := testClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	key := testPrefix + "sigv4-test.bin"
	payload := []byte("Beget S3 Go Test — SigV4 OK")

	// --- PUT ---
	t.Logf("PUT %s/%s (%d bytes)", testBucket, key, len(payload))
	err := c.putObject(ctx, key, payload)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	t.Log("PUT succeeded ✓")

	// --- GET ---
	t.Logf("GET %s/%s", testBucket, key)
	data, found, err := c.getObject(ctx, key)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if !found {
		t.Fatal("GET returned 404 — object not found after PUT")
	}
	if string(data) != string(payload) {
		t.Fatalf("GET data mismatch:\n  want: %q\n  got:  %q", payload, data)
	}
	t.Logf("GET succeeded ✓ — got %d bytes: %q", len(data), data)

	// --- DELETE (cleanup) ---
	t.Logf("DELETE %s/%s", testBucket, key)
	err = c.deleteObject(ctx, key)
	if err != nil {
		t.Logf("DELETE warning (non-fatal): %v", err)
	} else {
		t.Log("DELETE succeeded ✓")
	}

	// --- Verify deleted ---
	_, found, _ = c.getObject(ctx, key)
	if found {
		t.Log("⚠ Object still exists after DELETE (eventual consistency)")
	} else {
		t.Log("Object confirmed deleted ✓")
	}
}

// TestS3ConnWriteRead tests the full S3Conn (net.Conn) flow:
// Dial → Write (c2s) → manually PUT a s2c object → Read it back.
func TestS3ConnWriteRead(t *testing.T) {
	c := testClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Dial ---
	conn, err := c.Dial(ctx)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	s3c := conn.(*S3Conn)
	t.Logf("Session ID: %s", s3c.sessionID)

	// --- Write (client → server via c2s) ---
	msg := []byte("Beget S3 Go Test")
	n, err := conn.Write(msg)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	t.Logf("Write succeeded ✓ — %d bytes, key: %s", n, c.c2sKey(s3c.sessionID, 0))

	// Verify the c2s object was actually created
	data, found, err := c.getObject(ctx, c.c2sKey(s3c.sessionID, 0))
	if err != nil {
		t.Fatalf("Verify c2s GET failed: %v", err)
	}
	if !found {
		t.Fatal("c2s object not found after Write()")
	}
	t.Logf("c2s object verified ✓ — %d bytes: %q", len(data), data)

	// --- Simulate server response (PUT a s2c object) ---
	response := []byte("Hello from server via s2c")
	s2cKey := c.s2cKey(s3c.sessionID, 0)
	t.Logf("Simulating server PUT to %s", s2cKey)
	err = c.putObject(ctx, s2cKey, response)
	if err != nil {
		t.Fatalf("Server simulation PUT failed: %v", err)
	}
	t.Log("Server simulation PUT succeeded ✓")

	// --- Read (server → client via s2c) ---
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 1024)
	n, err = conn.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	got := string(buf[:n])
	if got != string(response) {
		t.Fatalf("Read data mismatch:\n  want: %q\n  got:  %q", response, got)
	}
	t.Logf("Read succeeded ✓ — %d bytes: %q", n, got)

	// --- Cleanup ---
	for _, key := range []string{
		c.c2sKey(s3c.sessionID, 0),
		c.s2cKey(s3c.sessionID, 0),
	} {
		_ = c.deleteObject(ctx, key)
	}
	t.Log("Cleanup done ✓")
}

// TestS3KeyFormat just prints the key format for visual verification.
func TestS3KeyFormat(t *testing.T) {
	c := testClient(t)
	defer c.Close()

	sid := "abcdef0123456789"
	t.Log("Key format examples:")
	t.Logf("  c2s: %s", c.c2sKey(sid, 0))
	t.Logf("  c2s: %s", c.c2sKey(sid, 42))
	t.Logf("  s2c: %s", c.s2cKey(sid, 0))
	t.Logf("  s2c: %s", c.s2cKey(sid, 999))
	t.Logf("  URL: %s", c.objectURL(c.c2sKey(sid, 0)))

	expected := fmt.Sprintf("%s%s/c2s/%06d.bin", testPrefix, sid, 0)
	got := c.c2sKey(sid, 0)
	if got != expected {
		t.Fatalf("Key format wrong: want %q, got %q", expected, got)
	}
	t.Log("Key format verified ✓")
}

// TestS3FullPipelineEndToEnd tests the end-to-end flow with a real running Python server.
// It writes "PING_FROM_GO_CLIENT" to the c2s path, and blocks reading from s2c
// waiting for a real server response forwarded from Xray.
func TestS3FullPipelineEndToEnd(t *testing.T) {
	c := testClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := c.Dial(ctx)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	s3c := conn.(*S3Conn)
	t.Logf("Started End-to-End session ID: %s", s3c.sessionID)
	t.Log("Writing 'PING_FROM_GO_CLIENT' to S3...")

	payload := []byte("PING_FROM_GO_CLIENT")
	n, err := conn.Write(payload)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	t.Logf("Wrote %d bytes successfully. Now waiting for real server response via s2c...", n)

	// Block reading response from the real server.
	conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	buf := make([]byte, 4096)
	rn, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read from real server failed: %v (Is the Python server running and connected to Xray?)", err)
	}

	t.Logf("Received response from real server: %q (%d bytes)", string(buf[:rn]), rn)
}

