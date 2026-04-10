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

package beacon

import (
	"context"
	"net"
	"testing"
)

func TestResolveJoinAddr_SRV(t *testing.T) {
	// Use a real resolver pointing at a name that won't have SRV records,
	// so it falls through to A/AAAA. Testing the SRV path requires DNS
	// infrastructure, so we test the fallback path here.
	addrs, err := ResolveJoinAddr(context.Background(), nil, "localhost", 7379)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) == 0 {
		t.Fatal("expected at least one address for localhost")
	}

	// Should have port 7379 (the default cluster port).
	for _, addr := range addrs {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("invalid address %q: %v", addr, err)
		}
		if port != "7379" {
			t.Fatalf("expected port 7379, got %s in %q", port, addr)
		}
	}
}

func TestResolveJoinAddr_FallbackToA(t *testing.T) {
	addrs, err := ResolveJoinAddr(context.Background(), nil, "localhost", 9999)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) == 0 {
		t.Fatal("expected at least one address")
	}

	// All addresses should use port 9999.
	for _, addr := range addrs {
		_, port, _ := net.SplitHostPort(addr)
		if port != "9999" {
			t.Fatalf("expected port 9999, got %s", port)
		}
	}
}

func TestResolveJoinAddr_Unresolvable(t *testing.T) {
	_, err := ResolveJoinAddr(context.Background(), nil, "this.name.does.not.exist.invalid.", 7379)
	if err == nil {
		t.Fatal("expected error for unresolvable name")
	}
}
