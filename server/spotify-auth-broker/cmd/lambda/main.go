package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"spotify-auth-broker/internal/http"
	"time"
)

func main() {
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339, NoColor: true})

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

	logger.Info().Msg("Starting lambda")
	lambda.Start(func(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
		return router.Serve(ctx, req)
	})
}
