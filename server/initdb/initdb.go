package main

import (
	"flag"
	"github.com/service"
	"log"
)

const (
	defaultRegion = "us-east-1"
)

func main() {
	var region string
	var endpointURL string

	flag.StringVar(&region, "region", defaultRegion, "AWS region (overridden by AWS_REGION env if set)")
	flag.StringVar(&endpointURL, "endpoint", "", "Custom DynamoDB endpoint (e.g., http://localhost:8000 for LocalStack)")
	flag.Parse()

	var svc = service.NewService(endpointURL, region)
	svc.CreateTables()
	log.Println("Done.")
}
