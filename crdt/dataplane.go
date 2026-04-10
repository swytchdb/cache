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

// MatchGlobPattern matches a string against a Redis-style glob pattern.
// Supports: * (match any sequence), ? (match single char), [abc] (character class)
func MatchGlobPattern(s, pattern string) bool {
	// Iterative glob matching with O(n*m) worst case instead of exponential
	// Uses a single backtrack point for '*' wildcards
	si, pi := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				// Record star position and current string position
				starIdx = pi
				matchIdx = si
				pi++
				continue

			case '?':
				si++
				pi++
				continue

			case '[':
				// Character class
				classStart := pi + 1
				negate := false
				if classStart < len(pattern) && pattern[classStart] == '^' {
					negate = true
					classStart++
				}
				end := classStart
				for end < len(pattern) && pattern[end] != ']' {
					end++
				}
				if end >= len(pattern) {
					// Malformed pattern - treat as no match, try backtrack
					if starIdx >= 0 {
						matchIdx++
						si = matchIdx
						pi = starIdx + 1
						continue
					}
					return false
				}
				class := pattern[classStart:end]
				matched := matchCharClass(s[si], class)
				if negate {
					matched = !matched
				}
				if matched {
					si++
					pi = end + 1
					continue
				}
				// Fall through to backtrack

			case '\\':
				// Escape next character
				if pi+1 < len(pattern) && s[si] == pattern[pi+1] {
					si++
					pi += 2
					continue
				}
				// Fall through to backtrack

			default:
				if s[si] == pattern[pi] {
					si++
					pi++
					continue
				}
				// Fall through to backtrack
			}
		}

		// Backtrack: if we had a star, advance the match position
		if starIdx >= 0 {
			matchIdx++
			si = matchIdx
			pi = starIdx + 1
		} else {
			return false
		}
	}

	// Check remaining pattern is all stars
	for pi < len(pattern) {
		if pattern[pi] != '*' {
			return false
		}
		pi++
	}

	return true
}

func matchCharClass(b byte, class string) bool {
	i := 0
	for i < len(class) {
		if i+2 < len(class) && class[i+1] == '-' {
			if b >= class[i] && b <= class[i+2] {
				return true
			}
			i += 3
		} else {
			if b == class[i] {
				return true
			}
			i++
		}
	}
	return false
}
