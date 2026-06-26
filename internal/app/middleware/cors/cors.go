package cors

import (
	"net/http"
)

func Cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}

		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, PATCH, DELETE")
			requestedHeaders := r.Header.Get("Access-Control-Request-Headers")
			if requestedHeaders == "" {
				requestedHeaders = "Accept, Content-Type, X-Requested-With, Authorization"
			}
			w.Header().Set("Access-Control-Allow-Headers", requestedHeaders)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Max-Age", "864000")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(&corsWriter{ResponseWriter: w, origin: origin}, r)
	})
}

type corsWriter struct {
	http.ResponseWriter
	origin      string
	wroteHeader bool
}

func (cw *corsWriter) WriteHeader(code int) {
	if !cw.wroteHeader {
		cw.wroteHeader = true
		cw.ResponseWriter.Header().Set("Access-Control-Allow-Origin", cw.origin)
		cw.ResponseWriter.Header().Set("Access-Control-Allow-Credentials", "true")
	}
	cw.ResponseWriter.WriteHeader(code)
}

func (cw *corsWriter) Write(b []byte) (int, error) {
	if !cw.wroteHeader {
		cw.WriteHeader(http.StatusOK)
	}
	return cw.ResponseWriter.Write(b)
}
