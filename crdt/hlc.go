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

package crdt

import (
	"time"
)

// HLC is a thin wrapper around the OS monotonic clock.
// Exists solely for testability (injectable clock).
type HLC struct {
	clock func() time.Time
}

// NewHLC creates a new clock.
func NewHLC() *HLC {
	return &HLC{clock: time.Now}
}

// Now returns the current monotonic timestamp in nanoseconds.
func (h *HLC) Now() time.Time {
	return h.clock()
}

// SetClock sets a custom clock function (for testing).
func (h *HLC) SetClock(clock func() time.Time) {
	h.clock = clock
}
