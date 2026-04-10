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
	"bytes"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
)

func TestDeriveCADeterministic(t *testing.T) {
	key1, cert1, err := DeriveCAFromPassphrase("deterministic-test")
	if err != nil {
		t.Fatalf("first derivation failed: %v", err)
	}
	key2, cert2, err := DeriveCAFromPassphrase("deterministic-test")
	if err != nil {
		t.Fatalf("second derivation failed: %v", err)
	}

	// Same passphrase must produce identical key and certificate bytes.
	if !bytes.Equal(key1.Seed(), key2.Seed()) {
		t.Fatal("same passphrase produced different CA keys")
	}
	if !bytes.Equal(cert1.Raw, cert2.Raw) {
		t.Fatal("same passphrase produced different CA certificate bytes")
	}
}

func TestDeriveCADifferentPassphrases(t *testing.T) {
	key1, _, err := DeriveCAFromPassphrase("passphrase-a")
	if err != nil {
		t.Fatalf("derivation a failed: %v", err)
	}
	key2, _, err := DeriveCAFromPassphrase("passphrase-b")
	if err != nil {
		t.Fatalf("derivation b failed: %v", err)
	}

	if bytes.Equal(key1.Seed(), key2.Seed()) {
		t.Fatal("different passphrases produced the same CA key")
	}
}

func TestDeriveCAProperties(t *testing.T) {
	_, cert, err := DeriveCAFromPassphrase("properties-test")
	if err != nil {
		t.Fatalf("derivation failed: %v", err)
	}

	if !cert.IsCA {
		t.Fatal("CA cert should have IsCA=true")
	}
	if cert.Subject.CommonName != "swytch-cluster-ca" {
		t.Fatalf("expected CN 'swytch-cluster-ca', got %q", cert.Subject.CommonName)
	}
	if cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatal("CA cert should have CertSign key usage")
	}
}

func TestGenerateLeafCert(t *testing.T) {
	caKey, caCert, err := DeriveCAFromPassphrase("leaf-test")
	if err != nil {
		t.Fatalf("CA derivation failed: %v", err)
	}

	leaf, err := GenerateLeafCert(caKey, caCert, "10.0.0.1:7000")
	if err != nil {
		t.Fatalf("leaf generation failed: %v", err)
	}

	if len(leaf.Certificate) != 2 {
		t.Fatalf("expected 2 certs in chain (leaf + CA), got %d", len(leaf.Certificate))
	}

	// Parse the leaf cert and verify it against the CA.
	leafCert, err := x509.ParseCertificate(leaf.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse leaf cert: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := leafCert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("leaf cert does not verify against CA: %v", err)
	}

	// Check that the IP SAN is set.
	if len(leafCert.IPAddresses) != 1 || !leafCert.IPAddresses[0].Equal(net.ParseIP("10.0.0.1")) {
		t.Fatalf("expected IP SAN 10.0.0.1, got %v", leafCert.IPAddresses)
	}
}

func TestGenerateLeafCertDNSSAN(t *testing.T) {
	caKey, caCert, err := DeriveCAFromPassphrase("dns-test")
	if err != nil {
		t.Fatalf("CA derivation failed: %v", err)
	}

	leaf, err := GenerateLeafCert(caKey, caCert, "node1.cluster.local:7000")
	if err != nil {
		t.Fatalf("leaf generation failed: %v", err)
	}

	leafCert, err := x509.ParseCertificate(leaf.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse leaf cert: %v", err)
	}

	if len(leafCert.DNSNames) != 1 || leafCert.DNSNames[0] != "node1.cluster.local" {
		t.Fatalf("expected DNS SAN 'node1.cluster.local', got %v", leafCert.DNSNames)
	}
}

func TestGenerateLeafCertEphemeral(t *testing.T) {
	caKey, caCert, err := DeriveCAFromPassphrase("ephemeral-test")
	if err != nil {
		t.Fatalf("CA derivation failed: %v", err)
	}

	leaf1, err := GenerateLeafCert(caKey, caCert, "127.0.0.1:7000")
	if err != nil {
		t.Fatalf("leaf1 failed: %v", err)
	}
	leaf2, err := GenerateLeafCert(caKey, caCert, "127.0.0.1:7000")
	if err != nil {
		t.Fatalf("leaf2 failed: %v", err)
	}

	// Leaf keys should be different (ephemeral).
	key1 := leaf1.PrivateKey.(ed25519.PrivateKey)
	key2 := leaf2.PrivateKey.(ed25519.PrivateKey)
	if bytes.Equal(key1.Seed(), key2.Seed()) {
		t.Fatal("two leaf certs should have different ephemeral keys")
	}
}

func TestGeneratePassphraseEntropy(t *testing.T) {
	p1, err := GeneratePassphrase()
	if err != nil {
		t.Fatalf("gen 1 failed: %v", err)
	}
	p2, err := GeneratePassphrase()
	if err != nil {
		t.Fatalf("gen 2 failed: %v", err)
	}

	if len(p1) != 43 {
		t.Fatalf("expected 43 chars (base64 RawURL of 32 bytes), got %d", len(p1))
	}
	if p1 == p2 {
		t.Fatal("two generated passphrases should not be identical")
	}
}

func TestPassphraseTLSHandshake(t *testing.T) {
	passphrase := "handshake-test-passphrase"

	// Derive server and client configs from the same passphrase.
	serverCfg := &ClusterConfig{
		NodeID:        0,
		Nodes:         []NodeConfig{{ID: 0, Address: "127.0.0.1:0"}},
		TLSPassphrase: passphrase,
	}
	serverTLS, err := serverCfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("server TLS: %v", err)
	}

	clientCfg := &ClusterConfig{
		NodeID:        1,
		Nodes:         []NodeConfig{{ID: 1, Address: "127.0.0.1:0"}},
		TLSPassphrase: passphrase,
	}
	clientTLS, err := clientCfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf("client TLS: %v", err)
	}

	// Start a TLS listener.
	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		// Force the handshake.
		errCh <- conn.(*tls.Conn).Handshake()
	}()

	// Dial with client TLS.
	clientTLS.ServerName = "" // skip hostname verification for this test
	clientTLS.InsecureSkipVerify = false
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	conn.Close()

	if err := <-errCh; err != nil {
		t.Fatalf("server handshake: %v", err)
	}
}

func TestPassphraseTLSHandshakeMismatch(t *testing.T) {
	// Server and client use different passphrases — handshake should fail.
	serverCfg := &ClusterConfig{
		NodeID:        0,
		Nodes:         []NodeConfig{{ID: 0, Address: "127.0.0.1:0"}},
		TLSPassphrase: "server-passphrase",
	}
	serverTLS, err := serverCfg.BuildTLSConfig()
	if err != nil {
		t.Fatalf("server TLS: %v", err)
	}

	clientCfg := &ClusterConfig{
		NodeID:        1,
		Nodes:         []NodeConfig{{ID: 1, Address: "127.0.0.1:0"}},
		TLSPassphrase: "client-passphrase",
	}
	clientTLS, err := clientCfg.BuildClientTLSConfig()
	if err != nil {
		t.Fatalf("client TLS: %v", err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.(*tls.Conn).Handshake() //nolint:errcheck
		conn.Close()
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err == nil {
		conn.Close()
		t.Fatal("expected handshake to fail with mismatched passphrases")
	}
}
