package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota
	Retry
)

type Server struct {
	URL          *url.URL
	Alive        bool
	mux          sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

func (sr *Server) IsAlive() (alive bool) {
	sr.mux.RLock()
	alive = sr.Alive
	sr.mux.RUnlock()
	return
}

func (sr *Server) SetAlive(alive bool) {
	sr.mux.Lock()
	sr.Alive = alive
	sr.mux.Unlock()
}

type ServerPool struct {
	pool    []*Server
	current uint64
}

func (s *ServerPool) MarkServerStatus(url *url.URL, status bool) {
	for _, s := range s.pool {
		if s.URL.String() == url.String() {
			s.SetAlive(status)
			break
		}
	}
}

func (s *ServerPool) AddServer(sr *Server) {
	s.pool = append(s.pool, sr)
}

func (s *ServerPool) Next() int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.pool)))
}

func (s *ServerPool) NextPeer() *Server {
	next := s.Next()
	l := len(s.pool) + next
	for i := next; i < l; i++ {
		if s.pool[i].IsAlive() {
			if i != next {
				atomic.StoreUint64(&s.current, uint64(i))
			}
			return s.pool[i]
		}
	}
	return nil
}

func (s *ServerPool) HealthCheck() {
	for _, server := range s.pool {
		status := "up"
		alive := server.IsAlive()
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", server.URL, status)
	}
}

func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

func roundRobin(w http.ResponseWriter, r *http.Request) {
	peer := serverPool.NextPeer()
	if peer != nil {
		peer.ReverseProxy.ServeHTTP(w, r)
	}
	http.Error(w, "Service is unavailable", http.StatusServiceUnavailable)
}

func healthCheck() {
	t := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-t.C:
			log.Println("Run health check")
			serverPool.HealthCheck()
			log.Println("End health check")
		}
	}
}

var serverPool ServerPool

func main() {
	var serverList string
	var port int
	flag.StringVar(&serverList, "servers", "", "Load balanced backends, use commas to separate")
	flag.IntVar(&port, "port", 9090, "Port to serve")
	flag.Parse()

	tokens := strings.Split(serverList, ",")

	for _, token := range tokens {
		url, err := url.Parse(token)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(url)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", url.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(writer, request.WithContext(ctx))
				}
				return
			}

			// after 3 retries, mark this backend as down
			serverPool.MarkServerStatus(url, false)

			// if the same request routing for few attempts with different backends, increase the count
			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			roundRobin(writer, request.WithContext(ctx))
		}
		server := Server{
			URL:          url,
			Alive:        true,
			ReverseProxy: proxy,
		}

		serverPool.AddServer(&server)
		log.Printf("Configured server %s\n", url)
	}

	httpServer := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(roundRobin),
	}

	go healthCheck()

	log.Printf("Load Balancer started at :%d\n", port)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}
