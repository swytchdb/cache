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
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"time"

	"golang.org/x/crypto/hkdf"
)

var (
	// Fixed epoch for deterministic CA cert validity. All nodes must produce
	// identical CA certificate bytes, so the validity window is anchored to
	// a fixed point in time rather than "now".
	caNotBefore = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	caNotAfter  = caNotBefore.Add(100 * 365 * 24 * time.Hour) // ~100 years

	// Leaf certs are short-lived — regenerated each startup.
	leafValidity = 7 * 24 * time.Hour
)

// DeriveCAFromPassphrase deterministically derives an Ed25519 CA key pair and
// self-signed CA certificate from a passphrase. Every node with the same
// passphrase produces identical output.
func DeriveCAFromPassphrase(passphrase string) (ed25519.PrivateKey, *x509.Certificate, error) {
	// HKDF-SHA256: passphrase → 32-byte Ed25519 seed
	hkdfReader := hkdf.New(sha256.New, []byte(passphrase), []byte("swytch-cluster-ca-v1"), []byte("ca-key"))
	seed := make([]byte, ed25519.SeedSize)
	if _, err := hkdfReader.Read(seed); err != nil {
		return nil, nil, fmt.Errorf("HKDF derivation failed: %w", err)
	}

	caKey := ed25519.NewKeyFromSeed(seed)

	// Deterministic serial number derived from the public key.
	pubHash := sha256.Sum256(caKey.Public().(ed25519.PublicKey))
	serial := new(big.Int).SetBytes(pubHash[:16])

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "swytch-cluster-ca"},
		NotBefore:             caNotBefore,
		NotAfter:              caNotAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	// Self-sign. Because template == parent and the key is deterministic,
	// every node produces byte-identical DER output.
	caDER, err := x509.CreateCertificate(deterministicReader{}, template, template, caKey.Public(), caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	return caKey, caCert, nil
}

// GenerateLeafCert creates an ephemeral leaf certificate signed by the given CA.
// The leaf key is randomly generated (not deterministic) and the cert is
// short-lived. nodeAddr is embedded as a DNS SAN (or IP SAN if it parses as IP).
func GenerateLeafCert(caKey crypto.Signer, caCert *x509.Certificate, nodeAddr string) (tls.Certificate, error) {
	// Ephemeral leaf key — fresh each startup.
	_, leafKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate leaf key: %w", err)
	}

	// Strip port if present.
	host := nodeAddr
	if h, _, err := net.SplitHostPort(nodeAddr); err == nil {
		host = h
	}

	// Random serial for the leaf.
	serialBytes := make([]byte, 16)
	if _, err := rand.Read(serialBytes); err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate serial: %w", err)
	}
	serial := new(big.Int).SetBytes(serialBytes)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: host},
		NotBefore:    now.Add(-5 * time.Minute), // small clock-skew tolerance
		NotAfter:     now.Add(leafValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	if ip := net.ParseIP(host); ip != nil {
		template.IPAddresses = []net.IP{ip}
	} else {
		template.DNSNames = []string{host}
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, template, caCert, leafKey.Public(), caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create leaf certificate: %w", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{leafDER, caCert.Raw},
		PrivateKey:  leafKey,
	}, nil
}

// GeneratePassphrase returns a cryptographically random passphrase suitable
// for use with DeriveCAFromPassphrase. The result is 32 random bytes encoded
// as base64 RawURL (43 characters, no padding).
func GeneratePassphrase() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// deterministicReader is an io.Reader that always returns zeros.
// Used when creating the CA certificate so that x509.CreateCertificate
// produces deterministic output (Ed25519 signing is deterministic and
// doesn't consume randomness, but the function signature requires a reader).
type deterministicReader struct{}

func (deterministicReader) Read(b []byte) (int, error) {
	clear(b)
	return len(b), nil
}
