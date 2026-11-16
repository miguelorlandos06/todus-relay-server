package main

import (
	"bufio"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Settings struct {
	BaseURL         string
	SharedSecret    []byte
	VerifyTLS       bool
	PollInterval    time.Duration
	ChunkSize       int
	WindowSize      int
	AckEvery        int
	HTTPTimeout     time.Duration
	HTTPRetries     int
	ConnectTimeout  time.Duration
	QueuePollInterval time.Duration
	MaxSessions     int
	UDPBatchCount   int
	UDPBatchDelayMS time.Duration
	UDPBatchMaxBytes int
	UDPMaxFlows     int
	UDPPollInterval time.Duration
	UDPUploadWorkers int
	DeleteAfterRead bool
}

func loadEnvFile(path string) {
	file, err := os.Open(path); if err != nil { return }
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") { continue }
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k, v := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			v = strings.Trim(v, `"'`)
			if _, exists := os.LookupEnv(k); !exists { os.Setenv(k, v) }
		}
	}
}

func getBool(n string, d bool) bool {
	v, ok := os.LookupEnv(n); if !ok { return d }
	v = strings.ToLower(strings.TrimSpace(v))
	return v == "1" || v == "true" || v == "yes"
}
func getInt(n string, d, min, max int) int {
	v, ok := os.LookupEnv(n); if !ok { return d }
	i, _ := strconv.Atoi(strings.TrimSpace(v))
	if i < min { return min }; if i > max { return max }; return i
}
func getFloat(n string, d, min, max float64) float64 {
	v, ok := os.LookupEnv(n); if !ok { return d }
	f, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if f < min { return min }; if f > max { return max }; return f
}

func LoadSettings() *Settings {
	cwd, _ := os.Getwd()
	loadEnvFile(filepath.Join(cwd, ".env"))
	baseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("TODUS_BASE_URL")), "/")
	if baseURL == "" { baseURL = "https://s3.todus.cu/stickers" }
	secretHex := strings.TrimSpace(os.Getenv("PROXY_SHARED_SECRET"))
	secretBytes, _ := hex.DecodeString(secretHex)
	if len(secretBytes) < 32 { panic("Secreto inválido") }
	return &Settings{
		BaseURL: baseURL, SharedSecret: secretBytes, VerifyTLS: true,
		PollInterval: time.Duration(getFloat("POLL_INTERVAL", 0.05, 0.01, 2.0) * float64(time.Second)),
		ChunkSize: getInt("CHUNK_SIZE", 262144, 4096, 8388608),
		WindowSize: getInt("WINDOW_SIZE", 8, 1, 128),
		AckEvery: getInt("ACK_EVERY", 4, 1, 32),
		HTTPTimeout: time.Duration(getFloat("HTTP_TIMEOUT", 20.0, 2.0, 120.0) * float64(time.Second)),
		HTTPRetries: getInt("HTTP_RETRIES", 3, 0, 10),
		ConnectTimeout: time.Duration(getFloat("CONNECT_TIMEOUT", 30.0, 3.0, 180.0) * float64(time.Second)),
		QueuePollInterval: time.Duration(getFloat("QUEUE_POLL_INTERVAL", 0.5, 0.1, 10.0) * float64(time.Second)),
		MaxSessions: getInt("MAX_SESSIONS", 64, 1, 512),
		UDPBatchCount: getInt("UDP_BATCH_COUNT", 32, 1, 512),
		UDPBatchDelayMS: time.Duration(getInt("UDP_BATCH_DELAY_MS", 5, 0, 100)) * time.Millisecond,
		UDPBatchMaxBytes: getInt("UDP_BATCH_MAX_BYTES", 49152, 1200, 2097152),
		UDPMaxFlows: getInt("UDP_MAX_FLOWS", 128, 1, 4096),
		UDPPollInterval: time.Duration(getFloat("UDP_POLL_INTERVAL", 0.02, 0.005, 1.0) * float64(time.Second)),
		UDPUploadWorkers: getInt("UDP_UPLOAD_WORKERS", 4, 1, 16),
		DeleteAfterRead: getBool("TODUS_DELETE_AFTER_READ", false),
	}
}
