package spotify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	authURL  = "https://accounts.spotify.com/authorize"
	tokenURL = "https://accounts.spotify.com/api/token"
	apiBase  = "https://api.spotify.com/v1"
)

type Client struct {
	clientID    string
	redirectURI string
	http        *http.Client
}

// Put near your other types in internal/spotify/client.go

// TimeRange represents Spotify's window for "top" endpoints.
type TimeRange string

const (
	TimeRangeShort  TimeRange = "short_term"  // ~last 4 weeks
	TimeRangeMedium TimeRange = "medium_term" // ~last 6 months
	TimeRangeLong   TimeRange = "long_term"   // several years
)

// Minimal track/artist payloads (expand if you need more fields)
type TopTracksResponse struct {
	Items []struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		URI   string `json:"uri"`
		Href  string `json:"href"`
		Album struct {
			ID     string `json:"id"`
			Name   string `json:"name"`
			Images []struct {
				URL string `json:"url"`
			} `json:"images"`
		} `json:"album"`
		Artists []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			URI  string `json:"uri"`
		} `json:"artists"`
		// Popularity, duration_ms etc. available if you want them
	} `json:"items"`
	Total int    `json:"total"`
	Next  string `json:"next"`
}

type TopArtistsResponse struct {
	Items []struct {
		ID     string   `json:"id"`
		Name   string   `json:"name"`
		URI    string   `json:"uri"`
		Href   string   `json:"href"`
		Genres []string `json:"genres"`
		Images []struct {
			URL string `json:"url"`
		} `json:"images"`
		// Popularity, followers, etc. available if needed
	} `json:"items"`
	Total int    `json:"total"`
	Next  string `json:"next"`
}

func NewClient(clientID, redirectURI string) *Client {
	return &Client{
		clientID:    clientID,
		redirectURI: redirectURI,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) AuthorizeURL(state, codeChallenge, scope string) string {
	v := url.Values{
		"client_id":             {c.clientID},
		"response_type":         {"code"},
		"redirect_uri":          {c.redirectURI},
		"code_challenge_method": {"S256"},
		"code_challenge":        {codeChallenge},
		"state":                 {state},
		"scope":                 {scope},
		"show_dialog":           {"false"},
	}
	return authURL + "?" + v.Encode()
}

type tokenResp struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	Scope        string `json:"scope"`
}

func (c *Client) ExchangeCode(ctx context.Context, code, codeVerifier string) (*tokenResp, error) {
	data := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {c.redirectURI},
		"client_id":     {c.clientID},
		"code_verifier": {codeVerifier},
	}
	return c.postToken(ctx, data)
}

func (c *Client) Refresh(ctx context.Context, refresh string) (*tokenResp, error) {
	data := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refresh},
		"client_id":     {c.clientID},
	}
	return c.postToken(ctx, data)
}

func (c *Client) postToken(ctx context.Context, data url.Values) (*tokenResp, error) {
	req, _ := http.NewRequestWithContext(ctx, "POST", tokenURL, io.NopCloser(stringsReader(data.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		b, _ := io.ReadAll(res.Body)
		return nil, errors.New("token error: " + strconv.Itoa(res.StatusCode) + " " + string(b))
	}
	var tr tokenResp
	if err := json.NewDecoder(res.Body).Decode(&tr); err != nil {
		return nil, err
	}
	return &tr, nil
}

// EnsureAccessToken: takes a refresh token, returns a fresh access token (string).
func (c *Client) EnsureAccessToken(ctx context.Context, refresh string) (string, error) {
	tr, err := c.Refresh(ctx, refresh)
	if err != nil {
		return "", err
	}
	return tr.AccessToken, nil
}

// Liked Tracks (Saved Tracks)
type SavedTracksResponse struct {
	Items []struct {
		AddedAt time.Time `json:"added_at"`
		Track   struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			URI   string `json:"uri"`
			Href  string `json:"href"`
			Album struct {
				ID     string `json:"id"`
				Name   string `json:"name"`
				URI    string `json:"uri"`
				Images []struct {
					URL string `json:"url"`
				} `json:"images"`
			} `json:"album"`
			Artists []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
				URI  string `json:"uri"`
			} `json:"artists"`
		} `json:"track"`
	} `json:"items"`
	Next  string `json:"next"`
	Total int    `json:"total"`
}

func (c *Client) GetLikedTracks(ctx context.Context, access string, limit, offset int) (*SavedTracksResponse, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	u := apiBase + "/me/tracks?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("Authorization", "Bearer "+access)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		b, _ := io.ReadAll(res.Body)
		return nil, errors.New("spotify api error: " + strconv.Itoa(res.StatusCode) + " " + string(b))
	}
	var out SavedTracksResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTopTracks fetches the user's top tracks for the given time range.
// limit: 1..50 (default 20). offset: 0+.
func (c *Client) GetTopTracks(ctx context.Context, access string, tr TimeRange, limit, offset int) (*TopTracksResponse, error) {
	if tr == "" {
		tr = TimeRangeMedium
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	u := apiBase + "/me/top/tracks?time_range=" + string(tr) +
		"&limit=" + strconv.Itoa(limit) +
		"&offset=" + strconv.Itoa(offset)

	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("Authorization", "Bearer "+access)

	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		b, _ := io.ReadAll(res.Body)
		return nil, errors.New("spotify api error: " + strconv.Itoa(res.StatusCode) + " " + string(b))
	}
	var out TopTracksResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetTopArtists fetches the user's top artists for the given time range.
// limit: 1..50 (default 20). offset: 0+.
func (c *Client) GetTopArtists(ctx context.Context, access string, tr TimeRange, limit, offset int) (*TopArtistsResponse, error) {
	if tr == "" {
		tr = TimeRangeMedium
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	u := apiBase + "/me/top/artists?time_range=" + string(tr) +
		"&limit=" + strconv.Itoa(limit) +
		"&offset=" + strconv.Itoa(offset)

	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("Authorization", "Bearer "+access)

	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		b, _ := io.ReadAll(res.Body)
		return nil, errors.New("spotify api error: " + strconv.Itoa(res.StatusCode) + " " + string(b))
	}
	var out TopArtistsResponse
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// helpers
func stringsReader(s string) *stringsReaderType { return &stringsReaderType{str: s, i: 0} }

type stringsReaderType struct {
	str string
	i   int
}

func (r *stringsReaderType) Read(p []byte) (int, error) {
	if r.i >= len(r.str) {
		return 0, io.EOF
	}
	n := copy(p, r.str[r.i:])
	r.i += n
	return n, nil
}
