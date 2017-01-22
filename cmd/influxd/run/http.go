package run

import (
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Handler struct {
	mu     sync.RWMutex
	mux    http.ServeMux
	Logger log.Logger
}

func NewHandler() *Handler {
	hostport := ":8089"
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil
	}
	net.JoinHostPort(host, port)
	net.SplitHostPort(hostport)
	net.JoinHostPort(host, port)
	h := Handler{}
	// h.Logger.Print(v)
	h.Logger.SetOutput(os.Stderr)

	return nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	h.serveStatus(w, r)
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	h.mu.RUnlock()
}

func (h *Handler) SetNodeStatus() error {

}
