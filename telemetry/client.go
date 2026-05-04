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

package telemetry

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"time"
)

const defaultEndpoint = "https://stats.getswytch.com"

type HeartbeatStats struct {
	Nodes          int
	UptimeSeconds  int
	MemoryAvail    int64
	MemoryUsage    int64
	HitCount       uint64
	MissNoData     uint64
	MissRemoteData uint64
}

type Config struct {
	NodeID       string
	Version      string
	Features     string
	AdopterEmail string
	StatsFunc    func() HeartbeatStats
}

type Client struct {
	nodeID   string
	version  string
	goos     string
	goarch   string
	features string

	adopterEmail string
	statsFunc    func() HeartbeatStats

	httpClient        *http.Client
	endpoint          string
	heartbeatInterval time.Duration
}

func New(cfg Config) *Client {
	return &Client{
		nodeID:            cfg.NodeID,
		version:           cfg.Version,
		goos:              runtime.GOOS,
		goarch:            runtime.GOARCH,
		features:          cfg.Features,
		adopterEmail:      cfg.AdopterEmail,
		statsFunc:         cfg.StatsFunc,
		httpClient:        &http.Client{Timeout: 5 * time.Second},
		endpoint:          defaultEndpoint,
		heartbeatInterval: 5 * time.Minute,
	}
}

func (c *Client) Run(ctx context.Context) {
	c.sendStartup(ctx)
	if c.adopterEmail != "" {
		c.sendAdopter(ctx)
	}

	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			c.sendHeartbeat(finalCtx)
			cancel()
			return
		case <-ticker.C:
			c.sendHeartbeat(ctx)
		}
	}
}

func (c *Client) sendStartup(ctx context.Context) {
	params := url.Values{}
	params.Set("ev", "startup")
	params.Set("node_id", c.nodeID)
	params.Set("ver", c.version)
	params.Set("os", c.goos)
	params.Set("arch", c.goarch)
	if c.features != "" {
		params.Set("features", c.features)
	}
	c.doGet(ctx, params)
}

func (c *Client) sendHeartbeat(ctx context.Context) {
	stats := c.statsFunc()
	params := url.Values{}
	params.Set("ev", "heartbeat")
	params.Set("node_id", c.nodeID)
	params.Set("nodes", strconv.Itoa(stats.Nodes))
	params.Set("uptime", strconv.Itoa(stats.UptimeSeconds))
	if stats.MemoryAvail != 0 {
		params.Set("memory_available", strconv.FormatInt(stats.MemoryAvail, 10))
	}
	if stats.MemoryUsage != 0 {
		params.Set("memory_usage", strconv.FormatInt(stats.MemoryUsage, 10))
	}
	if stats.HitCount != 0 {
		params.Set("hit_count", strconv.FormatUint(stats.HitCount, 10))
	}
	if stats.MissNoData != 0 {
		params.Set("miss_no_data_count", strconv.FormatUint(stats.MissNoData, 10))
	}
	if stats.MissRemoteData != 0 {
		params.Set("miss_remote_data_count", strconv.FormatUint(stats.MissRemoteData, 10))
	}
	c.doGet(ctx, params)
}

func (c *Client) sendAdopter(ctx context.Context) {
	params := url.Values{}
	params.Set("ev", "early-adopter")
	params.Set("node_id", c.nodeID)
	params.Set("email", c.adopterEmail)
	c.doGet(ctx, params)
}

func (c *Client) doGet(ctx context.Context, params url.Values) {
	u := c.endpoint + "/t/v1/nearcache?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		slog.Debug("telemetry: build request", "error", err)
		return
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		slog.Debug("telemetry: send", "error", err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
