//go:build windows || plan9 || js

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

package redis

import "net"

// reusePortListenConfig returns a standard ListenConfig on platforms
// that don't support SO_REUSEPORT.
func reusePortListenConfig() net.ListenConfig {
	return net.ListenConfig{}
}

// supportsReusePort returns false on platforms without SO_REUSEPORT support.
func supportsReusePort() bool {
	return false
}
