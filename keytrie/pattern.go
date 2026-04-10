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

/*
 * This file is part of CloxCache.
 *
 * CloxCache is licensed under the MIT License.
 * See LICENSE file for details.
 */

package keytrie

// matchCharClass checks if a byte matches a character class like "abc" or "a-z".
func matchCharClass(b byte, class string) bool {
	i := 0
	for i < len(class) {
		if i+2 < len(class) && class[i+1] == '-' {
			// Range like a-z
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

// MatchGlob matches a string against a Redis-style glob pattern using O(n*m) algorithm.
// This avoids exponential backtracking on pathological patterns like "a*a*a*...*b".
func MatchGlob(s, pattern string) bool {
	return matchGlob(s, pattern)
}

func matchGlob(s, pattern string) bool {
	si, pi := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) {
			switch pattern[pi] {
			case '*':
				starIdx = pi
				matchIdx = si
				pi++
				continue

			case '?':
				si++
				pi++
				continue

			case '[':
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

			case '\\':
				if pi+1 < len(pattern) && s[si] == pattern[pi+1] {
					si++
					pi += 2
					continue
				}

			default:
				if s[si] == pattern[pi] {
					si++
					pi++
					continue
				}
			}
		}

		if starIdx >= 0 {
			matchIdx++
			si = matchIdx
			pi = starIdx + 1
		} else {
			return false
		}
	}

	for pi < len(pattern) {
		if pattern[pi] != '*' {
			return false
		}
		pi++
	}

	return true
}
