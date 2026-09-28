//go:build s3test
// +build s3test

// Integration test for the S3 transport layer.
// Run with:  go test -v -tags s3test -run TestS3 -count=1 ./transport/s3/
//
// Fill in your credentials below before running.

package s3

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
)

// ========== CREDENTIALS FROM ENVIRONMENT ==========
// Never commit real keys: set them in the environment before running the test.
//
//	export S3_TEST_ENDPOINT=https://s3.example.cloud
//	export S3_TEST_REGION=ru1
//	export S3_TEST_BUCKET=my-bucket
//	export S3_TEST_ACCESS_KEY=...
//	export S3_TEST_SECRET_KEY=...
var (
	testEndpoint  = os.Getenv("S3_TEST_ENDPOINT")
	testRegion    = os.Getenv("S3_TEST_REGION")
	testBucket    = os.Getenv("S3_TEST_BUCKET")
	testAccessKey = os.Getenv("S3_TEST_ACCESS_KEY")
	testSecretKey = os.Getenv("S3_TEST_SECRET_KEY")
	testPrefix    = "vpn-sessions/"
)

// ===================================

func testClient(t *testing.T) *Client {
	t.Helper()
	if testEndpoint == "" || testBucket == "" || testAccessKey == "" || testSecretKey == "" {
		t.Skip("S3 credentials not set: export S3_TEST_ENDPOINT/S3_TEST_REGION/S3_TEST_BUCKET/S3_TEST_ACCESS_KEY/S3_TEST_SECRET_KEY")
	}
	opts := Options{
		Endpoint:  testEndpoint,
		Region:    testRegion,
		Bucket:    testBucket,
		AccessKey: testAccessKey,
		SecretKey: testSecretKey,
		Prefix:    testPrefix,
		PollMinMs: 100,
		PollMaxMs: 500,
		ChunkSize: 4096,
		ClientID:  "ivan",
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

// TestS3ConnWriteRead tests the full MuxStream/MuxSession flow:
// Dial → Write (c2s with frames) → manually PUT a s2c frame → Read it back.
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

	s3c := conn.(*MuxStream)
	sessionID := s3c.session.conn.(*S3Conn).sessionID
	t.Logf("Session ID: %s, Stream ID: %d", sessionID, s3c.id)

	// --- Write (client → server via c2s) ---
	msg := []byte("Beget S3 Go Test")
	n, err := conn.Write(msg)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	t.Logf("Write succeeded ✓ — %d bytes", n)

	// Verify the c2s object was actually created (retry loop to account for async write batching)
	var data []byte
	var found bool
	for i := 0; i < 30; i++ {
		time.Sleep(50 * time.Millisecond)
		data, found, err = c.getObject(ctx, c.c2sKey(sessionID, 0))
		if err == nil && found {
			break
		}
	}
	if err != nil {
		t.Fatalf("Verify c2s GET failed: %v", err)
	}
	if !found {
		t.Fatal("c2s object not found after Write() (even after retries)")
	}

	// We expect the c2s object to contain two frames: OPEN frame (type=2) and DATA frame (type=1)
	// Open frame: 7 bytes (StreamID, Type=2, Len=0)
	// Data frame: 7 + 16 bytes (StreamID, Type=1, Len=16, Payload="Beget S3 Go Test")
	if len(data) < 30 {
		t.Fatalf("c2s object too small: got %d bytes, expected >= 30", len(data))
	}
	t.Logf("c2s object verified ✓ — got %d bytes", len(data))

	// --- Simulate server response (PUT a s2c object as a binary frame) ---
	responsePayload := []byte("Hello from server via s2c")
	responseLen := len(responsePayload)
	frame := make([]byte, 7+responseLen)
	binary.BigEndian.PutUint32(frame[0:4], s3c.id)
	frame[4] = 1 // Type = DATA
	binary.BigEndian.PutUint16(frame[5:7], uint16(responseLen))
	copy(frame[7:], responsePayload)

	s2cKey := c.s2cKey(sessionID, 0)
	t.Logf("Simulating server PUT to %s", s2cKey)
	err = c.putObject(ctx, s2cKey, frame)
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
	if got != string(responsePayload) {
		t.Fatalf("Read data mismatch:\n  want: %q\n  got:  %q", responsePayload, got)
	}
	t.Logf("Read succeeded ✓ — %d bytes: %q", n, got)

	// --- Cleanup ---
	for _, key := range []string{
		c.c2sKey(sessionID, 0),
		c.s2cKey(sessionID, 0),
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

	s3c := conn.(*MuxStream)
	t.Logf("Started End-to-End session ID: %s", s3c.session.conn.(*S3Conn).sessionID)
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

// TestS3LatencyBenchmark measures timing for every stage of the S3 transport pipeline.
// Requires the real Python server to be running for the round-trip test.
//
// Run with:  go test -v -tags s3test -run TestS3LatencyBenchmark -count=1 ./transport/s3/
func TestS3LatencyBenchmark(t *testing.T) {
	c := testClient(t)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	t.Log("╔══════════════════════════════════════════════════════════╗")
	t.Log("║         S3 TRANSPORT LATENCY BENCHMARK                  ║")
	t.Log("╚══════════════════════════════════════════════════════════╝")

	// ───────────────────────────────────────────────
	// 1. Raw S3 PUT latency (5 iterations)
	// ───────────────────────────────────────────────
	t.Log("\n── Stage 1: Raw S3 PUT latency ──")
	const putIterations = 5
	putTimes := make([]time.Duration, putIterations)
	for i := 0; i < putIterations; i++ {
		key := fmt.Sprintf("%sbenchmark/put_test_%d.bin", testPrefix, i)
		payload := []byte(fmt.Sprintf("PUT_BENCH_PAYLOAD_%d_%d", i, time.Now().UnixNano()))

		start := time.Now()
		err := c.putObject(ctx, key, payload)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("PUT #%d failed: %v", i, err)
		}
		putTimes[i] = elapsed
		t.Logf("  PUT #%d: %6.1f ms  (%d bytes)", i, float64(elapsed.Microseconds())/1000.0, len(payload))

		// cleanup
		_ = c.deleteObject(ctx, key)
	}

	var putTotal time.Duration
	var putMin, putMax time.Duration
	putMin = putTimes[0]
	for _, d := range putTimes {
		putTotal += d
		if d < putMin {
			putMin = d
		}
		if d > putMax {
			putMax = d
		}
	}
	putAvg := putTotal / time.Duration(putIterations)
	t.Logf("  ┌─ PUT Summary: avg=%6.1f ms  min=%6.1f ms  max=%6.1f ms",
		float64(putAvg.Microseconds())/1000.0,
		float64(putMin.Microseconds())/1000.0,
		float64(putMax.Microseconds())/1000.0)

	// ───────────────────────────────────────────────
	// 2. Raw S3 GET latency (5 iterations)
	// ───────────────────────────────────────────────
	t.Log("\n── Stage 2: Raw S3 GET latency ──")
	// First, place an object to read
	getKey := fmt.Sprintf("%sbenchmark/get_test_object.bin", testPrefix)
	getPayload := []byte("GET_BENCHMARK_PAYLOAD_DATA_1234567890")
	if err := c.putObject(ctx, getKey, getPayload); err != nil {
		t.Fatalf("Setup PUT for GET benchmark failed: %v", err)
	}

	const getIterations = 5
	getTimes := make([]time.Duration, getIterations)
	for i := 0; i < getIterations; i++ {
		start := time.Now()
		data, found, err := c.getObject(ctx, getKey)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("GET #%d failed: %v", i, err)
		}
		if !found {
			t.Fatalf("GET #%d: object not found", i)
		}
		getTimes[i] = elapsed
		t.Logf("  GET #%d: %6.1f ms  (%d bytes)", i, float64(elapsed.Microseconds())/1000.0, len(data))
	}
	_ = c.deleteObject(ctx, getKey)

	var getTotal time.Duration
	var getMin, getMax time.Duration
	getMin = getTimes[0]
	for _, d := range getTimes {
		getTotal += d
		if d < getMin {
			getMin = d
		}
		if d > getMax {
			getMax = d
		}
	}
	getAvg := getTotal / time.Duration(getIterations)
	t.Logf("  ┌─ GET Summary: avg=%6.1f ms  min=%6.1f ms  max=%6.1f ms",
		float64(getAvg.Microseconds())/1000.0,
		float64(getMin.Microseconds())/1000.0,
		float64(getMax.Microseconds())/1000.0)

	// ───────────────────────────────────────────────
	// 3. S3 GET on non-existent key (404 latency)
	// ───────────────────────────────────────────────
	t.Log("\n── Stage 3: S3 GET 404 (poll miss) latency ──")
	const missIterations = 3
	missTimes := make([]time.Duration, missIterations)
	for i := 0; i < missIterations; i++ {
		fakeKey := fmt.Sprintf("%sbenchmark/nonexistent_%d.bin", testPrefix, i)
		start := time.Now()
		_, found, err := c.getObject(ctx, fakeKey)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("GET-miss #%d error: %v", i, err)
		}
		if found {
			t.Fatalf("GET-miss #%d: unexpectedly found", i)
		}
		missTimes[i] = elapsed
		t.Logf("  GET-404 #%d: %6.1f ms", i, float64(elapsed.Microseconds())/1000.0)
	}
	var missTotal time.Duration
	for _, d := range missTimes {
		missTotal += d
	}
	missAvg := missTotal / time.Duration(missIterations)
	t.Logf("  ┌─ 404 Summary: avg=%6.1f ms  (this is the cost of each poll miss)",
		float64(missAvg.Microseconds())/1000.0)

	// ───────────────────────────────────────────────
	// 4. Full round-trip: Write → Python server → Xray → Read
	//    (requires real server to be running!)
	// ───────────────────────────────────────────────
	t.Log("\n── Stage 4: Full round-trip via real server ──")
	t.Log("  ⚠ This stage requires the Python bridge + Xray to be running!")

	// Clean up stale objects from previous runs to prevent false cache hits
	_ = c.deleteObject(ctx, c.c2sKey("ivan", 0))
	_ = c.deleteObject(ctx, c.s2cKey("ivan", 0))
	time.Sleep(1 * time.Second) // wait for S3 eventual consistency delete

	conn, err := c.Dial(ctx)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	s3c := conn.(*MuxStream)
	t.Logf("  Session ID: %s", s3c.session.conn.(*S3Conn).sessionID)

	rtPayload := []byte("LATENCY_BENCHMARK_PING")

	writeStart := time.Now()
	_, err = conn.Write(rtPayload)
	writeElapsed := time.Since(writeStart)
	if err != nil {
		t.Fatalf("  Write failed: %v", err)
	}
	t.Logf("  Upload (conn.Write):  %6.1f ms", float64(writeElapsed.Microseconds())/1000.0)

	// Now measure the time from the moment Write() completed until Read() returns
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	buf := make([]byte, 4096)

	readStart := time.Now()
	rn, err := conn.Read(buf)
	readElapsed := time.Since(readStart)
	if err != nil {
		t.Fatalf("  Read failed (is the Python server running?): %v", err)
	}

	totalRT := writeElapsed + readElapsed
	t.Logf("  Download (conn.Read): %6.1f ms  (%d bytes: %q)",
		float64(readElapsed.Microseconds())/1000.0, rn, string(buf[:rn]))
	t.Logf("  ┌─ Total round-trip:  %6.1f ms", float64(totalRT.Microseconds())/1000.0)

	// ───────────────────────────────────────────────
	// 5. Summary table
	// ───────────────────────────────────────────────
	t.Log("\n╔══════════════════════════════════════════════════════════╗")
	t.Log("║                  RESULTS SUMMARY                       ║")
	t.Log("╠══════════════════════════════════════════════════════════╣")
	t.Logf("║  Raw PUT (avg/min/max):    %6.0f / %4.0f / %4.0f ms       ║",
		float64(putAvg.Microseconds())/1000.0,
		float64(putMin.Microseconds())/1000.0,
		float64(putMax.Microseconds())/1000.0)
	t.Logf("║  Raw GET (avg/min/max):    %6.0f / %4.0f / %4.0f ms       ║",
		float64(getAvg.Microseconds())/1000.0,
		float64(getMin.Microseconds())/1000.0,
		float64(getMax.Microseconds())/1000.0)
	t.Logf("║  GET 404 (poll miss avg):  %6.0f ms                    ║",
		float64(missAvg.Microseconds())/1000.0)
	t.Logf("║  conn.Write() latency:     %6.0f ms                    ║",
		float64(writeElapsed.Microseconds())/1000.0)
	t.Logf("║  conn.Read()  latency:     %6.0f ms                    ║",
		float64(readElapsed.Microseconds())/1000.0)
	t.Logf("║  Full round-trip:          %6.0f ms                    ║",
		float64(totalRT.Microseconds())/1000.0)
	t.Log("╚══════════════════════════════════════════════════════════╝")
}
