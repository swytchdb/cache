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

package effects

import (
	"bytes"
	"testing"
)

func TestEncryptor_RoundTrip(t *testing.T) {
	pub, priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	enc, err := NewEncryptor(pub, priv)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello, post-quantum world!")
	info := []byte("test-domain")

	sealed, err := enc.SealAndCompress(plaintext, info)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(sealed, plaintext) {
		t.Fatal("sealed should differ from plaintext")
	}

	recovered, err := enc.OpenAndDecompress(sealed, info)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(recovered, plaintext) {
		t.Fatalf("round-trip failed: got %q, want %q", recovered, plaintext)
	}
}

func TestEncryptor_DomainSeparation(t *testing.T) {
	pub, priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	enc, err := NewEncryptor(pub, priv)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("sensitive data")

	sealed, err := enc.SealAndCompress(plaintext, []byte("domain-a"))
	if err != nil {
		t.Fatal(err)
	}

	// Decrypting with wrong info should fail
	_, err = enc.OpenAndDecompress(sealed, []byte("domain-b"))
	if err == nil {
		t.Fatal("expected error when using wrong info for decryption")
	}
}

func TestEncryptor_LargePayload(t *testing.T) {
	pub, priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	enc, err := NewEncryptor(pub, priv)
	if err != nil {
		t.Fatal(err)
	}

	// 1MB payload — tests compression + encryption
	plaintext := make([]byte, 1024*1024)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	sealed, err := enc.SealAndCompress(plaintext, []byte("large"))
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := enc.OpenAndDecompress(sealed, []byte("large"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(recovered, plaintext) {
		t.Fatal("large payload round-trip failed")
	}
}

func TestEncryptor_EncryptOnlyNode(t *testing.T) {
	pub, _, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}

	enc, err := NewEncryptor(pub, nil)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("encrypt only")
	sealed, err := enc.SealAndCompress(plaintext, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) == 0 {
		t.Fatal("expected non-empty sealed data")
	}

	// Decryption should fail without private key
	_, err = enc.OpenAndDecompress(sealed, []byte("test"))
	if err == nil {
		t.Fatal("expected error when decrypting without private key")
	}
}

func TestGenerateKeyPair(t *testing.T) {
	pub, priv, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	if len(pub) == 0 {
		t.Fatal("public key should not be empty")
	}
	if len(priv) == 0 {
		t.Fatal("private key should not be empty")
	}
}
