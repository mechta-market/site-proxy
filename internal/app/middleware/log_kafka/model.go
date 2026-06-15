package log_kafka

import (
	"bytes"
	"net/http"
	"time"

	"github.com/goccy/go-json"
)

// ResponseWriter

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
		body:           &bytes.Buffer{},
	}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	rw.body.Write(b)
	return rw.ResponseWriter.Write(b)
}

// KafkaMessage payload

type kafkaMessagePayload struct {
	Ts        time.Time      `json:"ts"`
	Method    string         `json:"method"`
	Path      string         `json:"path"`
	Query     string         `json:"query"`
	ReqBody   json.RawMessage `json:"req_body"`
	RepStatus int            `json:"rep_status"`
	RepBody   json.RawMessage `json:"rep_body"`
	Headers   map[string]any `json:"headers"`
}
