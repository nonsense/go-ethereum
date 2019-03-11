// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package localstore

import (
	"context"

	"github.com/ethereum/go-ethereum/swarm/chunk"
)

// Hasser provides Has method to retrieve Chunks
// from database.
type Hasser struct {
	db *DB
}

// NewHasser returns a new Hasser on database.
func (db *DB) NewHasser() *Hasser {
	return &Hasser{
		db: db,
	}
}

// Has returns true if the chunk is stored in database.
func (h *Hasser) Has(addr chunk.Address) (bool, error) {
	return h.db.retrievalDataIndex.Has(addressToItem(addr))
}

// Has returns true if the chunk is stored in database.
func (db *DB) Has(_ context.Context, addr chunk.Address) (bool, error) {
	return db.retrievalDataIndex.Has(addressToItem(addr))
}
