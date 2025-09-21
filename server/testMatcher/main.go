package main

import (
	"flag"
	"github.com/service"
	"log"
	"time"
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
	events, err := svc.MatchEvents(service.MatchingRequest{service.Date{Time: time.Date(2025, 8, 31, 0, 0, 0, 0, time.UTC)},
		service.Date{Time: time.Date(2025, 9, 30, 0, 0, 0, 0, time.UTC)},
		"music",
		"I want to go to a punk or indie concert preferably at a small venue",
		[]string{"The Landsdowne Hotel", "Oxford art factory"}})

	if err != nil {
		log.Printf(err.Error())
		return
	}

	for _, r := range events {
		log.Printf("%v", r)
	}
}
