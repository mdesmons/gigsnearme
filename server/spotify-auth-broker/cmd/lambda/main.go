package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"os"
	"spotify-auth-broker/internal/http"
)

func main() {
	// quick env sanity
	required := []string{
		"SPOTIFY_CLIENT_ID",
		"SPOTIFY_REDIRECT_URI",
		"DDB_TABLE",
		"KMS_KEY_ID",
		"SPA_SUCCESS_URL",
		"APP_JWT_SECRET",
	}
	for _, k := range required {
		if os.Getenv(k) == "" {
			panic("missing env var: " + k)
		}
	}

	router := http.NewRouter()

	lambda.Start(func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		return router.Serve(ctx, req)
	})
}
