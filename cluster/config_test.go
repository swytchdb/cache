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

package cluster

import (
	"crypto/tls"
	"testing"
)

func TestPeers(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
			{ID: 2, Address: "10.0.0.3:7000", Region: "eu-west"},
		},
	}

	peers := cfg.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}
	for _, p := range peers {
		if p.ID == 0 {
			t.Fatal("self should not be in peers")
		}
	}
}

func TestSelf(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID: 1,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
		},
	}

	self := cfg.Self()
	if self == nil {
		t.Fatal("expected to find self")
	}
	if self.ID != 1 || self.Address != "10.0.0.2:7000" {
		t.Fatalf("unexpected self: %+v", self)
	}
}

func TestSelfNotFound(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID: 99,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
		},
	}
	if cfg.Self() != nil {
		t.Fatal("expected nil self when ID not in nodes")
	}
}

func TestDiffTopologyAdded(t *testing.T) {
	old := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
		},
	}
	new := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
			{ID: 2, Address: "10.0.0.3:7000", Region: "eu-west"},
		},
	}

	diff := DiffTopology(old, new)
	if len(diff.Added) != 1 || diff.Added[0].ID != 2 {
		t.Fatalf("expected 1 added node (ID=2), got %+v", diff.Added)
	}
	if len(diff.Removed) != 0 {
		t.Fatalf("expected 0 removed, got %+v", diff.Removed)
	}
	if len(diff.Changed) != 0 {
		t.Fatalf("expected 0 changed, got %+v", diff.Changed)
	}
}

func TestDiffTopologyRemoved(t *testing.T) {
	old := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
			{ID: 2, Address: "10.0.0.3:7000", Region: "eu-west"},
		},
	}
	new := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
		},
	}

	diff := DiffTopology(old, new)
	if len(diff.Added) != 0 {
		t.Fatalf("expected 0 added, got %+v", diff.Added)
	}
	if len(diff.Removed) != 1 || diff.Removed[0].ID != 2 {
		t.Fatalf("expected 1 removed (ID=2), got %+v", diff.Removed)
	}
}

func TestDiffTopologyChanged(t *testing.T) {
	old := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
		},
	}
	new := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.99:7000", Region: "us-east"},
		},
	}

	diff := DiffTopology(old, new)
	if len(diff.Changed) != 1 || diff.Changed[0].Address != "10.0.0.99:7000" {
		t.Fatalf("expected 1 changed with new address, got %+v", diff.Changed)
	}
}

func TestDiffTopologyNilOld(t *testing.T) {
	new := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
		},
	}

	diff := DiffTopology(nil, new)
	if len(diff.Added) != 1 {
		t.Fatalf("expected 1 added peer, got %+v", diff.Added)
	}
}

func TestDiffTopologyNilNew(t *testing.T) {
	old := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "10.0.0.1:7000", Region: "us-east"},
			{ID: 1, Address: "10.0.0.2:7000", Region: "us-east"},
		},
	}

	diff := DiffTopology(old, nil)
	if len(diff.Removed) != 1 {
		t.Fatalf("expected 1 removed peer, got %+v", diff.Removed)
	}
}

// --- TLS configuration tests ---

func TestBuildTLSConfigInsecureFallback(t *testing.T) {
	cfg := &ClusterConfig{}
	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg != nil {
		t.Fatal("expected nil TLS config for insecure fallback")
	}
}

func TestBuildClientTLSConfigInsecureFallback(t *testing.T) {
	cfg := &ClusterConfig{}
	tlsCfg, err := cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg != nil {
		t.Fatal("expected nil TLS config for insecure fallback")
	}
}

func TestBuildTLSConfigWithPassphrase(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID:        0,
		Nodes:         []NodeConfig{{ID: 0, Address: "127.0.0.1:7000"}},
		TLSPassphrase: "test-passphrase-for-unit-tests",
	}

	tlsCfg, err := cfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
	if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected RequireAndVerifyClientCert, got %v", tlsCfg.ClientAuth)
	}
	if tlsCfg.ClientCAs == nil {
		t.Fatal("expected ClientCAs to be set")
	}
	if tlsCfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3 minimum, got %d", tlsCfg.MinVersion)
	}
}

func TestBuildClientTLSConfigWithPassphrase(t *testing.T) {
	cfg := &ClusterConfig{
		NodeID:        0,
		Nodes:         []NodeConfig{{ID: 0, Address: "127.0.0.1:7000"}},
		TLSPassphrase: "test-passphrase-for-unit-tests",
	}

	tlsCfg, err := cfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(tlsCfg.Certificates))
	}
	if tlsCfg.RootCAs == nil {
		t.Fatal("expected RootCAs to be set")
	}
	if tlsCfg.ClientCAs != nil {
		t.Fatal("client TLS config should not set ClientCAs")
	}
	if tlsCfg.ClientAuth != tls.NoClientCert {
		t.Fatalf("client TLS config should not require client certs, got %v", tlsCfg.ClientAuth)
	}
	if tlsCfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("expected TLS 1.3 minimum, got %d", tlsCfg.MinVersion)
	}
}
