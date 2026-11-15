// Package store is a thin wrapper around S3 that exposes exactly the
// operations the driver needs, including conditional writes used for fencing.
package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var (
	// ErrNotFound is returned when the object does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrPrecondition is returned when an If-Match / If-None-Match check fails.
	// For the driver this means "someone else wrote it" – i.e. we are fenced.
	ErrPrecondition = errors.New("precondition failed")
)

// Config holds S3 connection settings.
type Config struct {
	Bucket    string
	Prefix    string // optional key prefix, e.g. "cluster-a"
	Endpoint  string // optional, for MinIO / Ceph RGW etc.
	Region    string
	PathStyle bool
}

// Store is an S3 bucket (plus optional prefix).
type Store struct {
	c      *s3.Client
	bucket string
	prefix string
}

// ObjectInfo is a listing entry.
type ObjectInfo struct {
	Key          string // key relative to the store prefix
	LastModified time.Time
	Size         int64
}

// New creates a Store using the default AWS credential chain
// (env vars, IRSA, instance profile, ...).
func New(ctx context.Context, cfg Config) (*Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	})
	return &Store{c: c, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

func (s *Store) key(k string) string {
	if s.prefix == "" {
		return k
	}
	full := path.Join(s.prefix, k)
	if strings.HasSuffix(k, "/") { // keep "dir/" list prefixes exact
		full += "/"
	}
	return full
}

func (s *Store) rel(full string) string {
	if s.prefix == "" {
		return full
	}
	p := s.prefix + "/"
	if len(full) >= len(p) && full[:len(p)] == p {
		return full[len(p):]
	}
	return full
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		case "PreconditionFailed", "ConditionalRequestConflict":
			return fmt.Errorf("%w: %v", ErrPrecondition, err)
		}
	}
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		switch re.HTTPStatusCode() {
		case http.StatusNotFound:
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		case http.StatusPreconditionFailed, http.StatusConflict:
			return fmt.Errorf("%w: %v", ErrPrecondition, err)
		}
	}
	return err
}

// Get returns the object body and its ETag.
func (s *Store) Get(ctx context.Context, k string) ([]byte, string, error) {
	out, err := s.c.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.key(k))})
	if err != nil {
		return nil, "", classify(err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", err
	}
	return b, aws.ToString(out.ETag), nil
}

// PutOpts controls conditional writes.
type PutOpts struct {
	IfMatch     string // write only if current ETag equals this
	IfNoneMatch bool   // write only if the object does not exist
}

// Put writes an object and returns its new ETag.
func (s *Store) Put(ctx context.Context, k string, data []byte, o PutOpts) (string, error) {
	in := &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           aws.String(s.key(k)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	}
	if o.IfMatch != "" {
		in.IfMatch = aws.String(o.IfMatch)
	}
	if o.IfNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}
	out, err := s.c.PutObject(ctx, in)
	if err != nil {
		return "", classify(err)
	}
	return aws.ToString(out.ETag), nil
}

// Head returns object metadata, or ErrNotFound.
func (s *Store) Head(ctx context.Context, k string) (ObjectInfo, error) {
	out, err := s.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: aws.String(s.key(k))})
	if err != nil {
		return ObjectInfo{}, classify(err)
	}
	return ObjectInfo{Key: k, LastModified: aws.ToTime(out.LastModified), Size: aws.ToInt64(out.ContentLength)}, nil
}

// Delete removes an object. Deleting a missing object is not an error.
func (s *Store) Delete(ctx context.Context, k string) error {
	_, err := s.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: aws.String(s.key(k))})
	if err = classify(err); errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// List calls fn for every object under prefix.
func (s *Store) List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error {
	p := s3.NewListObjectsV2Paginator(s.c, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: aws.String(s.key(prefix)),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return classify(err)
		}
		for _, o := range page.Contents {
			if err := fn(ObjectInfo{
				Key:          s.rel(aws.ToString(o.Key)),
				LastModified: aws.ToTime(o.LastModified),
				Size:         aws.ToInt64(o.Size),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
