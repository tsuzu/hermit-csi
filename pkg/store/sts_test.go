package store

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSTS answers AssumeRoleWithWebIdentity and AssumeRole, issuing numbered
// credentials (AKID1, AKID2, ...) that expire after ttl.
type fakeSTS struct {
	mu    sync.Mutex
	ttl   time.Duration
	calls []map[string]string // form values + "auth" (Authorization header)
}

func (f *fakeSTS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	call := map[string]string{"auth": r.Header.Get("Authorization")}
	for k := range r.Form {
		call[k] = r.Form.Get(k)
	}
	f.calls = append(f.calls, call)
	n := len(f.calls)
	f.mu.Unlock()

	action := r.Form.Get("Action")
	if action != "AssumeRoleWithWebIdentity" && action != "AssumeRole" {
		http.Error(w, "unexpected action "+action, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<%[1]sResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <%[1]sResult>
    <Credentials>
      <AccessKeyId>AKID%[2]d</AccessKeyId>
      <SecretAccessKey>SECRET%[2]d</SecretAccessKey>
      <SessionToken>TOKEN%[2]d</SessionToken>
      <Expiration>%[3]s</Expiration>
    </Credentials>
    <AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/hermit/s</Arn><AssumedRoleId>AROA:s</AssumedRoleId></AssumedRoleUser>
  </%[1]sResult>
  <ResponseMetadata><RequestId>req-%[2]d</RequestId></ResponseMetadata>
</%[1]sResponse>`, action, n, time.Now().Add(f.ttl).UTC().Format(time.RFC3339))
}

func (f *fakeSTS) call(i int) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i >= len(f.calls) {
		return nil
	}
	return f.calls[i]
}

func (f *fakeSTS) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeS3 accepts PUTs and records which credentials signed them.
type fakeS3 struct {
	mu   sync.Mutex
	keys []string // access key id of each request
	toks []string // X-Amz-Security-Token of each request
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	_, after, ok := strings.Cut(auth, "Credential=")
	key, _, _ := strings.Cut(after, "/")
	f.mu.Lock()
	f.keys = append(f.keys, key)
	f.toks = append(f.toks, r.Header.Get("X-Amz-Security-Token"))
	f.mu.Unlock()
	if !ok || r.Method != http.MethodPut {
		http.Error(w, "unexpected request", http.StatusBadRequest)
		return
	}
	w.Header().Set("ETag", `"etag"`)
}

func (f *fakeS3) last() (string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keys[len(f.keys)-1], f.toks[len(f.toks)-1]
}

// isolateAWSEnv makes the default credential chain see only static keys.
func isolateAWSEnv(t *testing.T) {
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "none"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_ROLE_ARN", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "STATIC")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "STATICSECRET")
	t.Setenv("AWS_SESSION_TOKEN", "")
}

func TestWebIdentityRole(t *testing.T) {
	isolateAWSEnv(t) // static keys present: the explicit role must still win
	stsSrv := &fakeSTS{ttl: 2 * time.Second}
	sts := httptest.NewServer(stsSrv)
	defer sts.Close()
	s3 := &fakeS3{}
	s3Srv := httptest.NewServer(s3)
	defer s3Srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token-1"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	st, err := New(ctx, Config{
		Bucket: "b", Endpoint: s3Srv.URL, Region: "us-east-1", PathStyle: true,
		RoleARN: "arn:aws:iam::123456789012:role/hermit", WebIdentityTokenFile: tokenFile,
		RoleSessionName: "node-a", STSEndpoint: sts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, "k", []byte("x"), PutOpts{}); err != nil {
		t.Fatal(err)
	}
	c := stsSrv.call(0)
	if c == nil || c["Action"] != "AssumeRoleWithWebIdentity" || c["RoleArn"] != "arn:aws:iam::123456789012:role/hermit" ||
		c["WebIdentityToken"] != "sa-token-1" || c["RoleSessionName"] != "node-a" {
		t.Fatalf("STS request = %v", c)
	}
	if c["auth"] != "" {
		t.Fatalf("AssumeRoleWithWebIdentity must be unsigned, got Authorization %q", c["auth"])
	}
	if key, tok := s3.last(); key != "AKID1" || tok != "TOKEN1" {
		t.Fatalf("S3 request signed with %q / token %q, want the assumed role's AKID1 / TOKEN1", key, tok)
	}

	// Kubelet rotates projected tokens: once the credentials expire, the new
	// token from the file is exchanged.
	if err := os.WriteFile(tokenFile, []byte("sa-token-2"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	if _, err := st.Put(ctx, "k", []byte("y"), PutOpts{}); err != nil {
		t.Fatal(err)
	}
	if n := stsSrv.count(); n != 2 {
		t.Fatalf("STS calls = %d, want a refresh after expiry", n)
	}
	if c := stsSrv.call(1); c["WebIdentityToken"] != "sa-token-2" {
		t.Fatalf("refresh used token %q, want the rotated one", c["WebIdentityToken"])
	}
	if key, _ := s3.last(); key != "AKID2" {
		t.Fatalf("after refresh S3 signed with %q, want AKID2", key)
	}
}

func TestAssumeRoleWithDefaultCredentials(t *testing.T) {
	isolateAWSEnv(t)
	stsSrv := &fakeSTS{ttl: time.Hour}
	sts := httptest.NewServer(stsSrv)
	defer sts.Close()
	s3 := &fakeS3{}
	s3Srv := httptest.NewServer(s3)
	defer s3Srv.Close()

	ctx := context.Background()
	st, err := New(ctx, Config{
		Bucket: "b", Endpoint: s3Srv.URL, Region: "us-east-1", PathStyle: true,
		RoleARN: "arn:aws:iam::210987654321:role/cross-account", STSEndpoint: sts.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, "k", []byte("x"), PutOpts{}); err != nil {
		t.Fatal(err)
	}
	c := stsSrv.call(0)
	if c["Action"] != "AssumeRole" || c["RoleSessionName"] != "hermit-csi" || !strings.Contains(c["auth"], "Credential=STATIC/") {
		t.Fatalf("STS request = %v, want AssumeRole signed with the static key", c)
	}
	if key, _ := s3.last(); key != "AKID1" {
		t.Fatalf("S3 signed with %q, want AKID1", key)
	}
}

func TestTokenFileNeedsRole(t *testing.T) {
	isolateAWSEnv(t)
	if _, err := New(context.Background(), Config{Bucket: "b", Region: "us-east-1", WebIdentityTokenFile: "/tmp/token"}); err == nil {
		t.Fatal("token file without role ARN accepted")
	}
}

func TestApplyRoleEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

	// EKS IRSA webhook variables are used when nothing else is configured.
	c := Config{}
	c.ApplyRoleEnv(env(map[string]string{"AWS_ROLE_ARN": "arn:irsa", "AWS_WEB_IDENTITY_TOKEN_FILE": "/irsa/token"}))
	if c.RoleARN != "arn:irsa" || c.WebIdentityTokenFile != "/irsa/token" {
		t.Fatalf("IRSA env: %+v", c)
	}
	// AWS_ROLE_ARN alone is not IRSA: leave it to the default chain.
	c = Config{}
	c.ApplyRoleEnv(env(map[string]string{"AWS_ROLE_ARN": "arn:irsa"}))
	if c.RoleARN != "" {
		t.Fatalf("AWS_ROLE_ARN alone: %+v", c)
	}
	// The hermit ConfigMap variables win over the webhook's, flags over both.
	c = Config{RoleSessionName: "from-flag"}
	c.ApplyRoleEnv(env(map[string]string{
		"S3_ROLE_ARN": "arn:hermit", "S3_WEB_IDENTITY_TOKEN_FILE": "/hermit/token", "S3_STS_ENDPOINT": "http://minio:9000",
		"S3_ROLE_SESSION_NAME": "from-env", "AWS_ROLE_ARN": "arn:irsa", "AWS_WEB_IDENTITY_TOKEN_FILE": "/irsa/token",
	}))
	want := Config{RoleARN: "arn:hermit", WebIdentityTokenFile: "/hermit/token", STSEndpoint: "http://minio:9000", RoleSessionName: "from-flag"}
	if c != want {
		t.Fatalf("precedence: %+v, want %+v", c, want)
	}
}
