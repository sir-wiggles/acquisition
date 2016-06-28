package config

type Config struct {
	ProcessedBucket string
	FtpBucket       string

	NewContentQueue string
	ProcessedQueue  string

	Key    string
	Secret string
	Region string
}
