package util

import (
	"strconv"
	"strings"
	"time"

	"spotify-auth-broker/internal/auth"
)

type cookieHeaders struct {
	cookies []string
}

func NewCookieHeaders() *cookieHeaders { return &cookieHeaders{cookies: []string{}} }

func (c *cookieHeaders) SetCookie(name, value string, ttl time.Duration) *cookieHeaders {
	c.cookies = append(c.cookies, auth.Cookie(name, value, int(ttl.Seconds())))
	return c
}
func (c *cookieHeaders) ClearCookie(name string) *cookieHeaders {
	c.cookies = append(c.cookies, auth.Cookie(name, "", -1))
	return c
}
func (c *cookieHeaders) H() map[string]string {
	h := map[string]string{}
	for i, ck := range c.cookies {
		if i == 0 {
			h["Set-Cookie"] = ck
		} else {
			h[("Set-Cookie-" + strconv.Itoa(i))] = ck // API GW supports multiple Set-Cookie via distinct header keys
		}
	}
	return h
}

func ParseCookie(header string) map[string]string {
	out := map[string]string{}
	parts := strings.Split(header, ";")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || !strings.Contains(p, "=") {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		out[kv[0]] = kv[1]
	}
	return out
}

func MergeHeaders(a, b map[string]string) map[string]string {
	if a == nil && b == nil {
		return map[string]string{}
	}
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}
