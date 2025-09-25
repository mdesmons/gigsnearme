package auth

import (
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"net/http"
	"strings"
	"time"
)

type Session struct {
	secret []byte
}

func NewSession(secret string) *Session { return &Session{secret: []byte(secret)} }

// go.mod: add github.com/google/uuid v1.6.0 (or similar)

// NewUserID generates an anonymous user id.
func NewUserID() string { return uuid.NewString() }

func (s *Session) Mint(userID string, ttl time.Duration) string {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub": userID,
		"iat": now.Unix(),
		"exp": now.Add(ttl).Unix(),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	ss, _ := t.SignedString(s.secret)
	return ss
}

func (s *Session) Require(cookieHeader string) (string, bool) {
	userID, err := s.userFromCookie(cookieHeader)
	return userID, err == nil && userID != ""
}

func (s *Session) userFromCookie(cookieHeader string) (string, error) {
	if cookieHeader == "" {
		return "", errors.New("no cookies")
	}
	// Expect "app_sess=...; ..."
	var token string
	for _, p := range strings.Split(cookieHeader, ";") {
		p = strings.TrimSpace(p)
		if strings.HasPrefix(p, "app_sess=") {
			token = strings.TrimPrefix(p, "app_sess=")
			break
		}
	}
	if token == "" {
		return "", errors.New("missing app_sess")
	}
	parsed, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("bad method")
		}
		return s.secret, nil
	})
	if err != nil || !parsed.Valid {
		return "", errors.New("invalid token")
	}
	claims := parsed.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)
	return sub, nil
}

// Helper to build Set-Cookie value with flags
func Cookie(name, value string, maxAgeSec int) string {
	if value == "" {
		// immediate expiry
		return (&http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		}).String()
	}
	return (&http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAgeSec,
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	}).String()
}
