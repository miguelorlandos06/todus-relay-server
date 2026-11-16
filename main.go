package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type SessionTarget struct {
	Version int    `json:"version"`
	Mode    string `json:"mode"`
	Host    string `json:"host,omitempty"`
	Port    int    `json:"port,omitempty"`
}

type StatusMessage struct {
	State   string  `json:"state"`
	Message string  `json:"message,omitempty"`
	Time    float64 `json:"time"`
}

var (
	settings *Settings
	store    *TodusStore
)

func publishStatus(sessionID, state, message string) {
	status := StatusMessage{State: state, Message: message, Time: float64(time.Now().UnixNano()) / 1e9}
	raw, _ := json.Marshal(status)
	store.Put(StatusKey(sessionID), raw)
}

func shortID(id string) string { if len(id) > 8 { return id[:8] }; return id }

func waitForWindow(sessionID, direction string, sequence uint64, stopCh <-chan struct{}, lastAck *uint64) bool {
	ticker := time.NewTicker(settings.PollInterval); defer ticker.Stop()
	for {
		select { case <-stopCh: return false; default: }
		rawAck := store.Get(AckKey(direction, sessionID), 0)
		if rawAck != nil {
			val := DecodeCounter(rawAck)
			for { old := atomic.LoadUint64(lastAck); if val <= old { break }; if atomic.CompareAndSwapUint64(lastAck, old, val) { break } }
		}
		if sequence-atomic.LoadUint64(lastAck) < uint64(settings.WindowSize) { return true }
		if store.Get(CloseKey(sessionID), 0) != nil { return false }
		select { case <-stopCh: return false; case <-ticker.C: }
	}
}

type UDPFlow struct {
	conn *net.UDPConn; replyHost string; replyPort uint16
	responses chan<- UDPPacket; stopCh <-chan struct{}; closed chan struct{}
	closeOnce sync.Once; lastActivity int64
}
func NewUDPFlow(host string, port uint16, responses chan<- UDPPacket, stopCh <-chan struct{}) (*UDPFlow, error) {
	raddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, port)); if err != nil { return nil, err }
	conn, err := net.DialUDP("udp", nil, raddr); if err != nil { return nil, err }
	f := &UDPFlow{conn: conn, replyHost: raddr.IP.String(), replyPort: uint16(raddr.Port), responses: responses, stopCh: stopCh, closed: make(chan struct{}), lastActivity: time.Now().UnixNano()}
	go f.reader(); return f, nil
}
func (f *UDPFlow) Send(payload []byte) error { atomic.StoreInt64(&f.lastActivity, time.Now().UnixNano()); _, err := f.conn.Write(payload); return err }
func (f *UDPFlow) reader() {
	buf := make([]byte, 65535)
	for {
		select { case <-f.stopCh: return; case <-f.closed: return; default: }
		f.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := f.conn.Read(buf)
		if err != nil { var ne net.Error; if errors.As(err, &ne) && ne.Timeout() { continue }; return }
		atomic.StoreInt64(&f.lastActivity, time.Now().UnixNano())
		pkt := UDPPacket{Host: f.replyHost, Port: f.replyPort, Payload: make([]byte, n)}; copy(pkt.Payload, buf[:n])
		select { case f.responses <- pkt: case <-time.After(200 * time.Millisecond): case <-f.stopCh: return; case <-f.closed: return }
	}
}
func (f *UDPFlow) Close() { f.closeOnce.Do(func() { close(f.closed); f.conn.Close() }) }
func (f *UDPFlow) LastActivity() time.Time { return time.Unix(0, atomic.LoadInt64(&f.lastActivity)) }

func udpDownlinkPublisher(sessionID string, responses <-chan UDPPacket, stopCh <-chan struct{}) {
	var seq uint64; sem := make(chan struct{}, settings.UDPUploadWorkers); var wg sync.WaitGroup
	defer func() { wg.Wait(); store.Put(FinKey("s2c", sessionID), EncodeCounter(seq)) }()
	for {
		var first UDPPacket
		select { case <-stopCh: select { case first = <-responses: default: return }; case pkt, ok := <-responses: if !ok { return }; first = pkt; case <-time.After(settings.UDPPollInterval): continue }
		batch := []UDPPacket{first}; bbytes := len(first.Payload); deadline := time.Now().Add(settings.UDPBatchDelayMS)
		for len(batch) < settings.UDPBatchCount && bbytes < settings.UDPBatchMaxBytes {
			rem := time.Until(deadline); if rem <= 0 { break }
			select { case it, ok := <-responses: if !ok { break }; batch = append(batch, it); bbytes += len(it.Payload); case <-time.After(rem): break }
		}
		sem <- struct{}{}; wg.Add(1)
		go func(s uint64, recs []UDPPacket) { defer func() { <-sem; wg.Done() }(); body, _ := EncodeUDPBatch(s, recs); store.Put(UDPDataKey("s2c", sessionID, s), body) }(seq, batch)
		seq++
	}
}

func handleUDPSession(sessionID string) {
	stopCh := make(chan struct{}); var once sync.Once
	stop := func() { once.Do(func() { close(stopCh) }) }
	responses := make(chan UDPPacket, settings.UDPMaxFlows*64)
	flows := make(map[string]*UDPFlow); var fmu sync.Mutex
	var pubWg sync.WaitGroup; pubWg.Add(1)
	go func() { defer pubWg.Done(); udpDownlinkPublisher(sessionID, responses, stopCh) }()
	publishStatus(sessionID, "ready", "")
	var seq uint64; lastAct := time.Now(); closeSeen := false
	defer func() {
		fmu.Lock(); for _, f := range flows { f.Close() }; fmu.Unlock()
		stop(); pubWg.Wait(); publishStatus(sessionID, "closed", "")
	}()
	for {
		select { case <-stopCh: return; default: }
		if store.Get(CloseKey(sessionID), 0) != nil { closeSeen = true }
		raw := store.Get(UDPDataKey("c2s", sessionID, seq), 0)
		if raw == nil { if next := store.Get(UDPDataKey("c2s", sessionID, seq+1), 0); next != nil { seq++; raw = next } }
		if raw == nil { if closeSeen { if fin := store.Get(FinKey("c2s", sessionID), 0); fin != nil && seq >= DecodeCounter(fin) { break } }; if time.Since(lastAct) > 180*time.Second { break }; time.Sleep(settings.UDPPollInterval); continue }
		records, err := DecodeUDPBatch(raw, seq)
		if err == nil {
			if settings.DeleteAfterRead { store.Delete(UDPDataKey("c2s", sessionID, seq)) }
			seq++
			for _, item := range records {
				fk := fmt.Sprintf("%s:%d", item.Host, item.Port)
				fmu.Lock(); flow, ok := flows[fk]
				if !ok { flow, _ = NewUDPFlow(item.Host, item.Port, responses, stopCh); if flow != nil { flows[fk] = flow } }
				fmu.Unlock()
				if flow != nil { if flow.Send(item.Payload) == nil { lastAct = time.Now() } }
			}
		}
	}
}

func handleTCPSession(sessionID string, target SessionTarget) {
	addr := fmt.Sprintf("%s:%d", target.Host, target.Port)
	remote, err := net.DialTimeout("tcp", addr, settings.ConnectTimeout)
	if err != nil { publishStatus(sessionID, "error", err.Error()); return }
	defer remote.Close(); publishStatus(sessionID, "ready", "")
	stopCh := make(chan struct{}); var once sync.Once; stop := func() { once.Do(func() { close(stopCh) }) }
	remoteFin := make(chan struct{}); var rOnce sync.Once; setFin := func() { rOnce.Do(func() { close(remoteFin) }) }
	var lastAct int64; atomic.StoreInt64(&lastAct, time.Now().UnixNano()); var lastAck uint64
	var upWg sync.WaitGroup; upWg.Add(1)
	go func() {
		defer upWg.Done(); defer setFin(); var seq uint64; buf := make([]byte, settings.ChunkSize)
		for {
			select { case <-stopCh: store.Put(FinKey("s2c", sessionID), EncodeCounter(seq)); return; default: }
			remote.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
			n, err := remote.Read(buf)
			if err != nil { var ne net.Error; if errors.As(err, &ne) && ne.Timeout() { continue }; if err != io.EOF {} ; break }
			if n == 0 { break }
			if !waitForWindow(sessionID, "s2c", seq, stopCh, &lastAck) { break }
			if !store.Put(DataKey("s2c", sessionID, seq), EncodeFrame(seq, buf[:n])) { break }
			seq++; atomic.StoreInt64(&lastAct, time.Now().UnixNano())
		}
		store.Put(FinKey("s2c", sessionID), EncodeCounter(seq))
	}()
	var seq uint64; var sinceAck int; lastAckTime := time.Now(); clientFin := false
	defer func() { stop(); remote.Close(); upWg.Wait(); publishStatus(sessionID, "closed", "") }()
	for {
		select { case <-stopCh: return; default: }
		if store.Get(CloseKey(sessionID), 0) != nil { break }
		raw := store.Get(DataKey("c2s", sessionID, seq), 0)
		if raw != nil {
			if payload, err := DecodeFrame(raw, seq); err == nil && len(payload) > 0 { remote.SetWriteDeadline(time.Now().Add(settings.ConnectTimeout)); remote.Write(payload) }
			if settings.DeleteAfterRead { store.Delete(DataKey("c2s", sessionID, seq)) }
			seq++; sinceAck++; atomic.StoreInt64(&lastAct, time.Now().UnixNano())
			if sinceAck >= settings.AckEvery || time.Since(lastAckTime) > time.Second { store.Put(AckKey("c2s", sessionID), EncodeCounter(seq)); sinceAck = 0; lastAckTime = time.Now() }
			continue
		}
		if fin := store.Get(FinKey("c2s", sessionID), 0); fin != nil && seq >= DecodeCounter(fin) && !clientFin {
			if sinceAck > 0 { store.Put(AckKey("c2s", sessionID), EncodeCounter(seq)) }
			clientFin = true
			if tcp, ok := remote.(*net.TCPConn); ok { tcp.CloseWrite() }
		}
		remoteDone := false; select { case <-remoteFin: remoteDone = true; default: }
		if clientFin && remoteDone { break }
		last := time.Unix(0, atomic.LoadInt64(&lastAct)); if time.Since(last) > time.Duration(math.Max(float64(settings.HTTPTimeout*3), 90*float64(time.Second))) { break }
		time.Sleep(settings.PollInterval)
	}
}

func handleSession(sessionID string) {
	defer func() { if r := recover(); r != nil { fmt.Printf("[PANIC %s] %v\n", shortID(sessionID), r) } }()
	var raw []byte; deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) { raw = store.Get(TargetKey(sessionID), 0); if raw != nil { break }; time.Sleep(settings.PollInterval) }
	if raw == nil { publishStatus(sessionID, "error", "target no encontrado"); return }
	var t SessionTarget; if err := json.Unmarshal(raw, &t); err != nil { return }
	if t.Version != 2 { return }
	switch t.Mode { case "udp": handleUDPSession(sessionID); case "tcp": handleTCPSession(sessionID, t); default: publishStatus(sessionID, "error", "modo desconocido") }
}

func worker() {
	active := make(map[string]struct{}); seen := make(map[string]struct{}); var lock sync.Mutex; sem := make(chan struct{}, settings.MaxSessions)
	for {
		for slot := 0; slot < 4; slot++ {
			raw := store.Get(QueueKey(slot), 0)
			if raw != nil {
				for _, line := range strings.Split(string(raw), "\n") {
					id := strings.TrimSpace(line); if id == "" { continue }
					lock.Lock(); _, isSeen := seen[id]
					if isSeen || len(active) >= settings.MaxSessions { lock.Unlock(); continue }
					active[id] = struct{}{}; seen[id] = struct{}{}; lock.Unlock()
					if store.Get(StatusKey(id), 0) != nil { lock.Lock(); delete(active, id); lock.Unlock(); continue }
					sem <- struct{}{}
					go func(sID string) { defer func() { lock.Lock(); delete(active, sID); lock.Unlock(); <-sem }(); handleSession(sID) }(id)
				}
			}
		}
		time.Sleep(settings.QueuePollInterval)
	}
}

func main() { settings = LoadSettings(); store = NewTodusStore(settings); worker() }
