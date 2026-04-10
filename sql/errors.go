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

package sql

import "errors"

var (
	// ErrNoSuchTable is returned from vtab / schema read paths when a
	// table name is not registered in the replicated table list.
	ErrNoSuchTable = errors.New("sql: no such table")

	// ErrTableExists is returned from DDL when CREATE TABLE targets a
	// name already in the registry.
	ErrTableExists = errors.New("sql: table already exists")

	// ErrNoSuchIndex is returned from index load/drop paths when the
	// index name is not registered.
	ErrNoSuchIndex = errors.New("sql: no such index")

	// ErrNoSuchView is returned from view load/drop paths when the
	// view name is not registered in the replicated view list.
	ErrNoSuchView = errors.New("sql: no such view")

	// ErrViewExists is returned from CREATE VIEW when the name is
	// already taken by another view. Table/view name collisions
	// surface as "sql: table already exists" via the parent pathway.
	ErrViewExists = errors.New("sql: view already exists")
)
