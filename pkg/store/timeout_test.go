package store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestRequestTimeout: an endpoint that accepts the connection but never
// answers must not block a call forever. Regression test for the incident
// where a half-dead S3 endpoint wedged a node's unstage path for 21h
// because requests had no timeout at all.
func TestRequestTimeout(t *testing.T) {
	isolateAWSEnv(t)
	t.Setenv("AWS_MAX_ATTEMPTS", "1") // one attempt: the timeout alone must bound the call

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // answer never; unblock when the client gives up
	}))
	defer srv.Close()

	st, err := New(context.Background(), Config{
		Bucket: "b", Endpoint: srv.URL, Region: "us-east-1", PathStyle: true,
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := st.Head(context.Background(), "k")
		done <- err
	}()
	start := time.Now()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Head against a silent endpoint succeeded")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("Head took %v, want it bounded by the 200ms request timeout", elapsed)
		}
	case <-time.After(15 * time.Second):
		srv.CloseClientConnections() // unblock the handler so the deferred Close returns
		t.Fatal("Head did not return: the request timeout is not applied")
	}
}
