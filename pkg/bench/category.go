/*
 * Warp (C) 2019-2024 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package bench

import (
	"math/bits"
	"strings"
)

//go:generate stringer -type Category -trimprefix=Cat $GOFILE

// A Category allows requests to be separated into different categories.
type Category uint8

const (
	// CatCacheMiss means that caching was detected, but the object missed the cache.
	CatCacheMiss Category = iota

	// CatCacheHit means that caching was detected and the object was cached.
	CatCacheHit

	catLength
)

// Categories is a bitfield that represents potentially several categories.
type Categories uint64

func NewCategories(c ...Category) Categories {
	var cs Categories
	for _, cat := range c {
		cs |= 1 << cat
	}
	return cs
}

// Split returns the categories
func (c Categories) Split() []Category {
	if c == 0 {
		return nil
	}
	res := make([]Category, 0, bits.OnesCount64(uint64(c)))
	for i := Category(0); c != 0 && i < catLength; i++ {
		if c&1 == 1 {
			res = append(res, i)
		}
		c >>= 1
	}
	return res
}

func (c Categories) String() string {
	cs := c.Split()
	res := make([]string, len(cs))
	for i, c := range cs {
		res[i] = c.String()
	}
	return strings.Join(res, ",")
}
