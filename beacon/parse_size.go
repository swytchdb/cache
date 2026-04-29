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
	"fmt"
	"strings"
)

// ParseMemoryLimit parses either an absolute size ("256mb", "4gb") or
// a percentage-of-available-memory spec ("50%"). Exactly one of bytes
// / percent is non-zero on success. Percent is returned as a
// fraction in (0, 1] so the cloxcache EnforceMemoryTarget API can
// consume it directly.
func ParseMemoryLimit(s string) (bytes int64, percent float64, err error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if strings.HasSuffix(s, "%") {
		var n int64
		if _, scanErr := fmt.Sscanf(s[:len(s)-1], "%d", &n); scanErr != nil {
			return 0, 0, fmt.Errorf("invalid percent %q: %w", s, scanErr)
		}
		if n <= 0 || n > 100 {
			return 0, 0, fmt.Errorf("percent %d%% out of range (1-100)", n)
		}
		return 0, float64(n) / 100, nil
	}
	b, err := ParseSize(s)
	if err != nil {
		return 0, 0, err
	}
	return b, 0, nil
}

// ParseSize parses a size string like "256mb", "4gb", "1024" into
// bytes. Suffixes b, k/kb, m/mb, g/gb, t/tb are accepted
// (case-insensitive). For percentage specs, use ParseMemoryLimit.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	numEnd := len(s)
	for i, c := range s {
		if c < '0' || c > '9' {
			numEnd = i
			break
		}
	}
	if numEnd == 0 {
		return 0, fmt.Errorf("invalid size: %s", s)
	}

	var val int64
	if _, err := fmt.Sscanf(s[:numEnd], "%d", &val); err != nil {
		return 0, fmt.Errorf("invalid number: %s", s)
	}

	suffix := strings.TrimSpace(s[numEnd:])
	var multiplier int64
	switch suffix {
	case "", "b":
		multiplier = 1
	case "k", "kb":
		multiplier = 1024
	case "m", "mb":
		multiplier = 1024 * 1024
	case "g", "gb":
		multiplier = 1024 * 1024 * 1024
	case "t", "tb":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size suffix: %s", suffix)
	}

	return val * multiplier, nil
}
