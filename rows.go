// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/sigmacomputing/odbc/api"
)

type Rows struct {
	os *ODBCStmt
}

func (r *Rows) Columns() []string {
	names := make([]string, len(r.os.Cols))
	for i := 0; i < len(names); i++ {
		names[i] = r.os.Cols[i].Name()
	}
	return names
}

func (r *Rows) Next(dest []driver.Value) error {
	ret := api.SQLFetch(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLFetch", r.os.h)
	}
	for i := range dest {
		v, err := r.os.Cols[i].Value(r.os.h, i)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

func (r *Rows) Close() error {
	return r.os.closeByRows()
}

func (r *Rows) HasNextResultSet() bool {
	return true
}

func (r *Rows) NextResultSet() error {
	ret := api.SQLMoreResults(r.os.h)
	if ret == api.SQL_NO_DATA {
		return io.EOF
	}
	if IsError(ret) {
		return NewError("SQLMoreResults", r.os.h)
	}

	err := r.os.BindColumns()
	if err != nil {
		return err
	}
	return nil
}

// ColumnTypeScanType should return the value type that can be used to scan
// types into.
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return r.os.Cols[index].ScanType()
}

// Nullable returns true if the column is nullable and false otherwise.
// If the column nullability is unknown, ok is false.
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.os.Cols[index].Nullable()
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	switch x := r.os.Cols[index].(type) {
	case *BindableColumn:
		return sqlTypeString(x.SQLType)
	case *NonBindableColumn:
		return sqlTypeString(x.SQLType)
	}
	return ""
}

func sqlTypeString(sqlt api.SQLSMALLINT) string {
	switch sqlt {
	case api.SQL_UNKNOWN_TYPE:
		return "ODBC_SQL_UNKNOWN_TYPE"
	case api.SQL_CHAR:
		return "ODBC_SQL_CHAR"
	case api.SQL_NUMERIC:
		return "ODBC_SQL_NUMERIC"
	case api.SQL_DECIMAL:
		return "ODBC_SQL_DECIMAL"
	case api.SQL_INTEGER:
		return "ODBC_SQL_INTEGER"
	case api.SQL_SMALLINT:
		return "ODBC_SQL_SMALLINT"
	case api.SQL_FLOAT:
		return "ODBC_SQL_FLOAT"
	case api.SQL_REAL:
		return "ODBC_SQL_REAL"
	case api.SQL_DOUBLE:
		return "ODBC_SQL_DOUBLE"
	case api.SQL_DATETIME:
		return "ODBC_SQL_DATETIME"
	case api.SQL_TIME:
		return "ODBC_SQL_TIME"
	case api.SQL_VARCHAR:
		return "ODBC_SQL_VARCHAR"
	case api.SQL_TYPE_DATE:
		return "ODBC_SQL_TYPE_DATE"
	case api.SQL_TYPE_TIME:
		return "ODBC_SQL_TYPE_TIME"
	case api.SQL_TYPE_TIMESTAMP:
		return "ODBC_SQL_TYPE_TIMESTAMP"
	case api.SQL_TIMESTAMP:
		return "ODBC_SQL_TIMESTAMP"
	case api.SQL_LONGVARCHAR:
		return "ODBC_SQL_LONGVARCHAR"
	case api.SQL_BINARY:
		return "ODBC_SQL_BINARY"
	case api.SQL_VARBINARY:
		return "ODBC_SQL_VARBINARY"
	case api.SQL_LONGVARBINARY:
		return "ODBC_SQL_LONGVARBINARY"
	case api.SQL_BIGINT:
		return "ODBC_SQL_BIGINT"
	case api.SQL_TINYINT:
		return "ODBC_SQL_TINYINT"
	case api.SQL_BIT:
		return "ODBC_SQL_BIT"
	case api.SQL_WCHAR:
		return "ODBC_SQL_WCHAR"
	case api.SQL_WVARCHAR:
		return "ODBC_SQL_WVARCHAR"
	case api.SQL_WLONGVARCHAR:
		return "ODBC_SQL_WLONGVARCHAR"
	case api.SQL_GUID:
		return "ODBC_SQL_GUID"
	case api.SQL_SIGNED_OFFSET:
		return "ODBC_SQL_SIGNED_OFFSET"
	case api.SQL_UNSIGNED_OFFSET:
		return "ODBC_SQL_UNSIGNED_OFFSET"
	}

	return ""
}
