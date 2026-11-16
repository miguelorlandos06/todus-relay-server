package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var EnvelopeMagic = []byte("TDS2")
var TagSize = sha256.Size

type TodusStore struct {
	settings *Settings
	client   *http.Client
}

func NewTodusStore(settings *Settings) *TodusStore {
	tr := &http.Transport{
		MaxIdleConns: 100, MaxIdleConnsPerHost: 50, MaxConnsPerHost: 50,
		IdleConnTimeout: 90 * time.Second, DisableCompression: true,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !settings.VerifyTLS},
	}
	return &TodusStore{settings: settings, client: &http.Client{Transport: tr, Timeout: settings.HTTPTimeout}}
}

func (s *TodusStore) buildURL(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts { parts[i] = url.PathEscape(p) }
	return fmt.Sprintf("%s/%s", s.settings.BaseURL, strings.Join(parts, "/"))
}

func (s *TodusStore) seal(key string, data []byte) []byte {
	h := hmac.New(sha256.New, s.settings.SharedSecret)
	h.Write([]byte(key)); h.Write([]byte{0}); h.Write(data)
	tag := h.Sum(nil)
	out := make([]byte, 4+TagSize+len(data))
	copy(out[0:4], EnvelopeMagic)
	copy(out[4:4+TagSize], tag)
	copy(out[4+TagSize:], data)
	return out
}

func (s *TodusStore) open(key string, envelope []byte) []byte {
	if len(envelope) < 4+TagSize || !bytes.Equal(envelope[:4], EnvelopeMagic) { return nil }
	tag := envelope[4 : 4+TagSize]
	data := envelope[4+TagSize:]
	h := hmac.New(sha256.New, s.settings.SharedSecret)
	h.Write([]byte(key)); h.Write([]byte{0}); h.Write(data)
	if !hmac.Equal(tag, h.Sum(nil)) { return nil }
	return data
}

func (s *TodusStore) Get(key string, retries int) []byte {
	targetURL := s.buildURL(key)
	for attempt := 0; attempt <= retries; attempt++ {
		req, _ := http.NewRequest(http.MethodGet, targetURL, nil)
		req.Header.Set("User-Agent", "curl/8.23.0")
		resp, err := s.client.Do(req)
		if err != nil {
			time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond); continue
		}
		body, _ := io.ReadAll(resp.Body); resp.Body.Close()
		if resp.StatusCode == http.StatusOK { return s.open(key, body) }
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound { return nil }
	}
	return nil
}

func (s *TodusStore) Put(key string, data []byte) bool {
	targetURL := s.buildURL(key)
	sealed := s.seal(key, data)
	for attempt := 0; attempt <= s.settings.HTTPRetries; attempt++ {
		req, _ := http.NewRequest(http.MethodPut, targetURL, bytes.NewReader(sealed))
		req.Header.Set("User-Agent", "curl/8.23.0")
		resp, err := s.client.Do(req)
		if err != nil { time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond); continue }
		io.Copy(io.Discard, resp.Body); resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 { return true }
	}
	return false
}

func (s *TodusStore) Delete(key string) bool {
	targetURL := s.buildURL(key)
	req, err := http.NewRequest(http.MethodDelete, targetURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "curl/8.23.0")
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode == 200 || resp.StatusCode == 202 || resp.StatusCode == 204 || resp.StatusCode == 404
}
