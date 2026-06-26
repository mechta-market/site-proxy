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

		if rw.statusCode != http.StatusOK && rw.statusCode != http.StatusCreated {
			return
		}

		headers := map[string]any{
			"User-Agent":         r.Header.Get("User-Agent"),
			"Referer":            r.Header.Get("Referer"),
			"Accept-Language":    r.Header.Get("Accept-Language"),
			"X-Mechta-App":       r.Header.Get("X-Mechta-App"),
			"X-Mechta-Device-Id": r.Header.Get("X-Mechta-Device-Id"),
			"X-City-Code":        r.Header.Get("X-City-Code"),
			"Cf-Ipcountry":       r.Header.Get("Cf-Ipcountry"),
			"Cf-Connecting-Ip":   r.Header.Get("Cf-Connecting-Ip"),
			"cookies":            parseCookies(r.Cookies()),
		}

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

var allowedCookies = map[string]struct{}{
	"mechtakz_session":       {},
	"user_device_id":         {},
	"platform_type":          {},
	"selectedCity":           {},
	"roistat_visit":          {},
	"roistat_call_tracking":  {},
	"roistat_cookies_to_resave": {},
	"_userGUID":              {},
	"AMP_MKTG_383e593a34":   {},
	"_ga":                    {},
}

func parseCookies(cookies []*http.Cookie) map[string]string {
	result := make(map[string]string)
	for _, c := range cookies {
		if _, ok := allowedCookies[c.Name]; ok {
			result[c.Name] = c.Value
		}
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
