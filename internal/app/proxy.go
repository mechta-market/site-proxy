package app

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httputil"
	"time"
)

func proxyGetHandler() http.Handler {
	return &httputil.ReverseProxy{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			DisableCompression:  true,
			TLSHandshakeTimeout: 2 * time.Second,
			MaxIdleConnsPerHost: 100,
		},
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(conf.TargetUrl)
			if len(conf.TargetHeaders) > 0 {
				for k, v := range conf.TargetHeaders {
					r.Out.Header.Add(k, v)
				}
			}
			r.SetXForwarded()
			if conf.TargetHost != "" {
				r.Out.Host = conf.TargetHost
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			if conf.HttpCors {
				origin := resp.Request.Header.Get("Origin")
				if origin == "" {
					origin = "*"
				}
				resp.Header.Set("Access-Control-Allow-Origin", origin)
				resp.Header.Set("Access-Control-Allow-Credentials", "true")
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
}
