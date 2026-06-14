/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package control

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	sessionCookie = "warp_control_session"
	sessionTTL    = 12 * time.Hour
)

// auth provides cookie-session authentication for the control plane. Credentials
// are read from the environment; it is enabled only when both are set, so
// existing local deployments keep working unless configured.
type auth struct {
	enabled bool
	user    string
	pass    string

	mu       sync.Mutex
	sessions map[string]time.Time // token -> expiry
}

// newAuth reads credentials from WARP_CONTROL_USER / WARP_CONTROL_PASSWORD.
func newAuth() *auth {
	u := os.Getenv("WARP_CONTROL_USER")
	p := os.Getenv("WARP_CONTROL_PASSWORD")
	return &auth{
		enabled:  u != "" && p != "",
		user:     u,
		pass:     p,
		sessions: map[string]time.Time{},
	}
}

// validateCredentials checks a username/password using constant-time comparison.
func (a *auth) validateCredentials(u, p string) bool {
	uOK := subtle.ConstantTimeCompare([]byte(u), []byte(a.user)) == 1
	pOK := subtle.ConstantTimeCompare([]byte(p), []byte(a.pass)) == 1
	return uOK && pOK
}

func (a *auth) newSession() string {
	var b [32]byte
	_, _ = rand.Read(b[:])
	tok := hex.EncodeToString(b[:])
	a.mu.Lock()
	a.sessions[tok] = time.Now().Add(sessionTTL)
	a.mu.Unlock()
	return tok
}

func (a *auth) validSession(tok string) bool {
	if tok == "" {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	exp, ok := a.sessions[tok]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(a.sessions, tok)
		return false
	}
	return true
}

func (a *auth) destroySession(tok string) {
	a.mu.Lock()
	delete(a.sessions, tok)
	a.mu.Unlock()
}

func (a *auth) authed(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	return a.validSession(c.Value)
}

// middleware enforces authentication for every request except the login/logout
// endpoints. Browser navigations are redirected to /login; other requests (API,
// assets) get a 401.
func (a *auth) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !a.enabled || r.URL.Path == "/login" || r.URL.Path == "/logout" {
			next.ServeHTTP(w, r)
			return
		}
		if a.authed(r) {
			next.ServeHTTP(w, r)
			return
		}
		if strings.Contains(r.Header.Get("Accept"), "text/html") {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func (a *auth) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if !a.enabled || a.authed(r) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	page, err := staticFiles.ReadFile("static/login.html")
	if err != nil {
		http.Error(w, "login page unavailable", http.StatusInternalServerError)
		return
	}
	errMsg := ""
	if r.URL.Query().Get("error") != "" {
		errMsg = `<div class="err">Invalid username or password</div>`
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(strings.Replace(string(page), "{{ERROR}}", errMsg, 1)))
}

func (a *auth) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !a.enabled {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	_ = r.ParseForm()
	if !a.validateCredentials(r.PostFormValue("username"), r.PostFormValue("password")) {
		http.Redirect(w, r, "/login?error=1", http.StatusFound)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    a.newSession(),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(sessionTTL),
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func (a *auth) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		a.destroySession(c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusFound)
}
