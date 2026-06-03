package s3

import "time"

// Options holds the S3 transport configuration parsed from the YAML
// `s3-opts` block inside a proxy definition.
type Options struct {
	Endpoint  string `proxy:"endpoint"`              // full URL, e.g. "https://s3.us-east-1.amazonaws.com"
	Region    string `proxy:"region,omitempty"`       // AWS region, default "us-east-1"
	Bucket    string `proxy:"bucket"`                 // bucket name
	AccessKey string `proxy:"access-key"`             // AWS access key ID
	SecretKey string `proxy:"secret-key"`             // AWS secret access key
	Prefix    string `proxy:"prefix,omitempty"`       // object key prefix, default "tunnel/"

	// Polling tuning
	PollMinMs int `proxy:"poll-min-ms,omitempty"` // aggressive poll floor in ms (default 50)
	PollMaxMs int `proxy:"poll-max-ms,omitempty"` // idle backoff ceiling in ms (default 2000)

	// Transfer tuning
	ChunkSize int `proxy:"chunk-size,omitempty"` // max bytes per PUT (default 32768)
}

// Defaults fills zero-valued fields with sensible defaults.
func (o *Options) Defaults() {
	if o.Region == "" {
		o.Region = "us-east-1"
	}
	if o.Prefix == "" {
		o.Prefix = "tunnel/"
	}
	if o.PollMinMs <= 0 {
		o.PollMinMs = 50
	}
	if o.PollMaxMs <= 0 {
		o.PollMaxMs = 2000
	}
	if o.ChunkSize <= 0 {
		o.ChunkSize = 32768
	}
}

// PollMin returns the minimum polling interval as a time.Duration.
func (o *Options) PollMin() time.Duration {
	return time.Duration(o.PollMinMs) * time.Millisecond
}

// PollMax returns the maximum polling interval as a time.Duration.
func (o *Options) PollMax() time.Duration {
	return time.Duration(o.PollMaxMs) * time.Millisecond
}
