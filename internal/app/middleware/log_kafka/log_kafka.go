package log_kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/goccy/go-json"

	"github.com/rendau/hps/internal/app/middleware/log_kafka/producer"
)

type Middleware struct {
	producer producerI
	filter   filterI
}

func New(producer producerI, filter filterI) *Middleware {
	return &Middleware{
		producer: producer,
		filter:   filter,
	}
}

func (m *Middleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.filter.Check(r.Method, r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		var reqBody []byte
		if r.Body != nil {
			reqBody, _ = io.ReadAll(r.Body)
			if reqBody == nil {
				reqBody = make([]byte, 0)
			}
			r.Body = io.NopCloser(bytes.NewBuffer(reqBody))
		}

		rw := newResponseWriter(w)
		next.ServeHTTP(rw, r)

		headers := make(map[string]any, len(r.Header)+1)
		for k, v := range r.Header {
			if len(v) > 0 && k != "Cookie" {
				headers[k] = v[0]
			}
		}
		headers["cookies"] = parseCookies(r.Cookies())

		go m.sendToKafka(&kafkaMessagePayload{
			Ts:        time.Now().UTC(),
			Method:    r.Method,
			Path:      r.URL.Path,
			Query:     r.URL.RawQuery,
			ReqBody:   normalizeJSON(reqBody, false),
			RepStatus: rw.statusCode,
			RepBody:   normalizeJSON(rw.body.Bytes(), rw.Header().Get("Content-Encoding") == "gzip"),
			Headers:   headers,
		})
	})
}

func (m *Middleware) sendToKafka(msg *kafkaMessagePayload) {
	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("failed to marshal log message", "error", err, "msg", msg)
		return
	}

	err = m.producer.Send(
		context.Background(),
		producer.Message{
			Key:  msg.Method + " " + msg.Path,
			Data: data,
		},
	)
	if err != nil {
		slog.Error("failed to write message to kafka", "error", err, "msg", msg)
	}
}

func parseCookies(cookies []*http.Cookie) map[string]string {
	result := make(map[string]string, len(cookies))
	for _, c := range cookies {
		result[c.Name] = c.Value
	}
	return result
}

func normalizeJSON(data []byte, isGzip bool) json.RawMessage {
	if isGzip {
		gzipReader, err := gzip.NewReader(bytes.NewReader(data))
		if err == nil {
			defer gzipReader.Close()
			data, err = io.ReadAll(gzipReader)
			if err != nil {
				data = nil
			}
		} else {
			data = nil
		}
	}
	if len(data) == 0 {
		return json.RawMessage("null")
	}
	if json.Valid(data) {
		return data
	}
	return json.RawMessage("null")
}
