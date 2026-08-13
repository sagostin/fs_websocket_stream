package bridge

import (
	"encoding/json"
	"net/http"
	"strings"
)

// HealthzHandler returns a simple liveness + session count response. Safe
// to expose to load-balancer health checks.
func HealthzHandler(srv *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"ok":       true,
			"sessions": srv.SessionCount(),
		})
	}
}

// AuthMiddleware returns a token-checking middleware for /control and
// /recordings endpoints. If token is empty the middleware is a no-op (use
// for development only). Accepted:
//
//	Authorization: Bearer <token>
//	?token=<token>            (for browsers)
//
// The audio plane (/stream) is NOT covered — auth for the audio plane goes
// through STREAM_EXTRA_HEADERS on the FreeSWITCH side, terminated at the LB.
func AuthMiddleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("token") == token {
			next.ServeHTTP(w, r)
			return
		}
		h := r.Header.Get("Authorization")
		if strings.HasPrefix(h, "Bearer ") && strings.TrimPrefix(h, "Bearer ") == token {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}
