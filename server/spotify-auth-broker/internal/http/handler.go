package http

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"spotify-auth-broker/internal/auth"
	"spotify-auth-broker/internal/spotify"
	"spotify-auth-broker/internal/store"
	"spotify-auth-broker/internal/util"
)

type Router struct {
	store   *store.DDB
	client  *spotify.Client
	session *auth.Session
}

func NewRouter() *Router {
	s := store.MustNew()
	return &Router{
		store:   s,
		client:  spotify.NewClient(os.Getenv("SPOTIFY_CLIENT_ID"), os.Getenv("SPOTIFY_REDIRECT_URI")),
		session: auth.NewSession(os.Getenv("APP_JWT_SECRET")),
	}
}

func (r *Router) Serve(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	path := req.Path
	method := req.HTTPMethod

	switch {
	case method == "POST" && path == "/auth/spotify/start":
		return r.startAuth(ctx, req)
	case method == "GET" && path == "/auth/spotify/callback":
		return r.callback(ctx, req)
	case method == "POST" && path == "/auth/spotify/unlink":
		return r.unlink(ctx, req)
	case method == "GET" && path == "/spotify/liked":
		return r.getLiked(ctx, req)
	default:
		return util.JSON(404, util.M{"error": "not found"}), nil
	}
}

// POST /auth/spotify/start
func (r *Router) startAuth(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Require app session (replace this with Cognito if you use it)
	_, ok := r.session.Require(req.Headers["Cookie"])
	if !ok {
		return util.JSON(401, util.M{"error": "unauthorized"}), nil
	}

	state := auth.RandomString(24)
	verifier := auth.RandomString(64)
	challenge := auth.CodeChallengeS256(verifier)

	// Store temp cookies (10 min) — alternative: store in Dynamo with TTL
	h := util.NewCookieHeaders().
		SetCookie("sp_state", state, 10*time.Minute).
		SetCookie("sp_cv", verifier, 10*time.Minute).
		H()

	authURL := r.client.AuthorizeURL(state, challenge, "user-library-read user-top-read")

	// 302 Redirect to Spotify authorize
	return events.APIGatewayProxyResponse{
		StatusCode: 302,
		Headers:    util.MergeHeaders(h, map[string]string{"Location": authURL}),
	}, nil
}

// GET /auth/spotify/callback
func (r *Router) callback(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	/*	q, _ := url.ParseQuery(req.QueryStringParameters.Encode())
		code := q.Get("code")
		state := q.Get("state")
	*/
	//q, _ := url.ParseQuery(req.QueryStringParameters.Encode())
	code := req.QueryStringParameters["code"]
	state := req.QueryStringParameters["state"]
	if code == "" || state == "" {
		return util.JSON(400, util.M{"error": "missing code/state"}), nil
	}

	cookies := util.ParseCookie(req.Headers["Cookie"])
	if cookies["sp_state"] != state {
		return util.JSON(400, util.M{"error": "invalid state"}), nil
	}
	codeVerifier := cookies["sp_cv"]
	if codeVerifier == "" {
		return util.JSON(400, util.M{"error": "missing code_verifier"}), nil
	}

	// Require app session (you may allow linking during sign-in if preferred)
	userID, ok := r.session.Require(req.Headers["Cookie"])
	if !ok {
		return util.JSON(401, util.M{"error": "unauthorized"}), nil
	}

	// Exchange code -> tokens (PKCE, no client secret)
	tok, err := r.client.ExchangeCode(ctx, code, codeVerifier)
	if err != nil {
		return util.JSON(502, util.M{"error": "token exchange failed", "detail": err.Error()}), nil
	}
	if tok.RefreshToken == "" {
		return util.JSON(502, util.M{"error": "no refresh_token returned"}), nil
	}

	// Save encrypted refresh token
	if err := r.store.UpsertRefreshToken(ctx, userID, tok.RefreshToken, tok.Scope); err != nil {
		return util.JSON(500, util.M{"error": "persist failed", "detail": err.Error()}), nil
	}

	// Clear temporary cookies + refresh app session
	h := util.NewCookieHeaders().
		ClearCookie("sp_state").
		ClearCookie("sp_cv").
		SetCookie("app_sess", r.session.Mint(userID, 60*time.Minute), 60*time.Minute).
		H()

	return events.APIGatewayProxyResponse{
		StatusCode: 302,
		Headers:    util.MergeHeaders(h, map[string]string{"Location": os.Getenv("SPA_SUCCESS_URL")}),
	}, nil
}

// POST /auth/spotify/unlink
func (r *Router) unlink(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	userID, ok := r.session.Require(req.Headers["Cookie"])
	if !ok {
		return util.JSON(401, util.M{"error": "unauthorized"}), nil
	}
	if err := r.store.DeleteLink(ctx, userID); err != nil {
		return util.JSON(500, util.M{"error": "delete failed", "detail": err.Error()}), nil
	}
	return util.JSON(204, nil), nil
}

// GET /spotify/liked
func (r *Router) getLiked(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	userID, ok := r.session.Require(req.Headers["Cookie"])
	if !ok {
		return util.JSON(401, util.M{"error": "unauthorized"}), nil
	}

	link, err := r.store.GetLink(ctx, userID)
	if err != nil {
		return util.JSON(404, util.M{"error": "not linked"}), nil
	}

	access, err := r.client.EnsureAccessToken(ctx, link.RefreshToken)
	if err != nil {
		return util.JSON(502, util.M{"error": "refresh failed", "detail": err.Error()}), nil
	}

	tracks, err := r.client.GetLikedTracks(ctx, access, 50, 0) // expand with pagination on your side
	if err != nil {
		return util.JSON(502, util.M{"error": "spotify error", "detail": err.Error()}), nil
	}
	return util.JSON(200, tracks), nil
}
