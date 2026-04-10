/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves Prometheus metrics on an HTTP endpoint.
type Server struct {
	httpServer *http.Server
	registry   *prometheus.Registry
	collector  *Collector
}

// NewServer creates a new metrics server for the given stats provider.
func NewServer(provider StatsProvider, port int) *Server {
	registry := prometheus.NewRegistry()
	collector := NewCollector(provider)
	registry.MustRegister(collector)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(
		prometheus.Gatherers{registry, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	))

	// Add a simple health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok")) // error handled by http server
	})

	return &Server{
		httpServer: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		registry:  registry,
		collector: collector,
	}
}

// Start starts the metrics server. This method blocks.
func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

// StartAsync starts the metrics server in a goroutine.
func (s *Server) StartAsync() {
	go func() {
		if err := s.Start(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()
}

// Stop gracefully shuts down the metrics server.
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// Address returns the address the server is listening on.
func (s *Server) Address() string {
	return s.httpServer.Addr
}
