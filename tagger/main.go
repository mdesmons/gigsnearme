package main

import (
	"flag"
	"fmt"
	"github.com/dbschema"
	"github.com/pipeline"
)

// TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>
const (
	defaultRegion = "us-east-1"
)

func main() {
	var region string
	var endpointURL string

	flag.StringVar(&region, "region", defaultRegion, "AWS region (overridden by AWS_REGION env if set)")
	flag.StringVar(&endpointURL, "endpoint", "", "Custom DynamoDB endpoint (e.g., http://localhost:8000 for LocalStack)")
	flag.Parse()
	//TIP <p>Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined text
	// to see how GoLand suggests fixing the warning.</p><p>Alternatively, if available, click the lightbulb to view possible fixes.</p>
	var client, context, _ = dbschema.InitDb(endpointURL, region)
	tagger := pipeline.NewTagger(context, client)

	events, err := dbschema.QueryUntaggedEvents(context, client, "moshtix")
	fmt.Printf("Found %d untagged events\n", len(events))
	if err == nil {
		slices := len(events) / 10
		for i := 0; i <= slices; i++ {
			start := i * 10
			end := start + 10
			if end > len(events) {
				end = len(events)
			}
			batch := events[start:end]
			fmt.Printf("Tagging batch %d: %d events\n", i, len(batch))
			err = tagger.Tag(batch)
			if err != nil {
				fmt.Printf("Error tagging batch %d: %s\n", i, err.Error())
			}
		}
	}
}
