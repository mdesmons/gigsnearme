package main

import (
	"flag"
	"github.com/dbschema"
	"github.com/moshtix"
	"github.com/pipeline"
	"log"
)

// TIP <p>To run your code, right-click the code and select <b>Run</b>.</p> <p>Alternatively, click
// the <icon src="AllIcons.Actions.Execute"/> icon in the gutter and select the <b>Run</b> menu item from here.</p>
const (
	defaultRegion = "us-east-1"
)

func main() {
	//TIP <p>Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined text
	// to see how GoLand suggests fixing the warning.</p><p>Alternatively, if available, click the lightbulb to view possible fixes.</p>
	var region string
	var endpointURL string
	var createOnly bool

	flag.StringVar(&region, "region", defaultRegion, "AWS region (overridden by AWS_REGION env if set)")
	flag.StringVar(&endpointURL, "endpoint", "", "Custom DynamoDB endpoint (e.g., http://localhost:8000 for LocalStack)")
	flag.BoolVar(&createOnly, "create-only", false, "Only create the table; skip sample put")
	flag.Parse()

	var client, context, _ = dbschema.InitDb(endpointURL, region)
	pipeline := pipeline.NewPipeline(context, client)
	err := moshtix.Scrape(pipeline)
	if err != nil {
		log.Printf(err.Error())
	}
}
