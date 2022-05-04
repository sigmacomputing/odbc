// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/sigmacomputing/odbc/api"
)

type BufferLen api.SQLLEN

func (l *BufferLen) IsNull() bool {
	return *l == api.SQL_NULL_DATA
}

func (l *BufferLen) GetData(h api.SQLHSTMT, idx int, ctype api.SQLSMALLINT, buf []byte) api.SQLRETURN {
	return api.SQLGetData(h, api.SQLUSMALLINT(idx+1), ctype,
		api.SQLPOINTER(unsafe.Pointer(&buf[0])), api.SQLLEN(len(buf)),
		(*api.SQLLEN)(l))
}

func (l *BufferLen) Bind(h api.SQLHSTMT, idx int, ctype api.SQLSMALLINT, buf []byte) api.SQLRETURN {
	return api.SQLBindCol(h, api.SQLUSMALLINT(idx+1), ctype,
		api.SQLPOINTER(unsafe.Pointer(&buf[0])), api.SQLLEN(len(buf)),
		(*api.SQLLEN)(l))
}

// Column provides access to row columns.
type Column interface {
	Name() string
	Bind(h api.SQLHSTMT, idx int) (bool, error)
	Value(h api.SQLHSTMT, idx int) (driver.Value, error)
	Nullable() (bool, bool)
	ScanType() reflect.Type
}

func describeColumn(h api.SQLHSTMT, idx int, namebuf []uint16) (namelen int, sqltype api.SQLSMALLINT, size api.SQLULEN, nullable api.SQLSMALLINT, ret api.SQLRETURN) {
	var l, decimal api.SQLSMALLINT
	ret = api.SQLDescribeCol(h, api.SQLUSMALLINT(idx+1),
		(*api.SQLWCHAR)(unsafe.Pointer(&namebuf[0])),
		api.SQLSMALLINT(len(namebuf)), &l,
		&sqltype, &size, &decimal, &nullable)
	return int(l), sqltype, size, nullable, ret
}

// TODO(brainman): did not check for MS SQL timestamp

func NewColumn(h api.SQLHSTMT, idx int, loc *time.Location) (Column, error) {
	namebuf := make([]uint16, 150)
	namelen, sqltype, size, nullable, ret := describeColumn(h, idx, namebuf)
	if ret == api.SQL_SUCCESS_WITH_INFO && namelen > len(namebuf) {
		// try again with bigger buffer
		namebuf = make([]uint16, namelen)
		namelen, sqltype, size, nullable, ret = describeColumn(h, idx, namebuf)
	}
	if IsError(ret) {
		return nil, NewError("SQLDescribeCol", h)
	}
	if namelen > len(namebuf) {
		// still complaining about buffer size
		return nil, errors.New("Failed to allocate column name buffer")
	}
	b := &BaseColumn{
		name:     api.UTF16ToString(namebuf[:namelen]),
		loc:      loc,
		SQLType:  sqltype,
		nullable: nullable,
	}
	switch sqltype {
	case api.SQL_BIT:
		return NewBindableColumn(b, api.SQL_C_BIT, 1), nil
	case api.SQL_TINYINT, api.SQL_SMALLINT, api.SQL_INTEGER:
		return NewBindableColumn(b, api.SQL_C_LONG, 4), nil
	case api.SQL_BIGINT:
		return NewBindableColumn(b, api.SQL_C_SBIGINT, 8), nil
	case api.SQL_NUMERIC, api.SQL_DECIMAL, api.SQL_FLOAT, api.SQL_REAL, api.SQL_DOUBLE:
		return NewBindableColumn(b, api.SQL_C_DOUBLE, 8), nil
	case api.SQL_TYPE_TIMESTAMP:
		var v api.SQL_TIMESTAMP_STRUCT
		return NewBindableColumn(b, api.SQL_C_TYPE_TIMESTAMP, int(unsafe.Sizeof(v))), nil
	case api.SQL_TYPE_DATE:
		var v api.SQL_DATE_STRUCT
		return NewBindableColumn(b, api.SQL_C_DATE, int(unsafe.Sizeof(v))), nil
	case api.SQL_TYPE_TIME:
		var v api.SQL_TIME_STRUCT
		return NewBindableColumn(b, api.SQL_C_TIME, int(unsafe.Sizeof(v))), nil
	case api.SQL_SS_TIME2:
		var v api.SQL_SS_TIME2_STRUCT
		return NewBindableColumn(b, api.SQL_C_BINARY, int(unsafe.Sizeof(v))), nil
	case api.SQL_GUID:
		var v api.SQLGUID
		return NewBindableColumn(b, api.SQL_C_GUID, int(unsafe.Sizeof(v))), nil
	case api.SQL_CHAR:
		return NewVariableWidthColumn(b, api.SQL_C_CHAR, size)
	case api.SQL_WCHAR:
		return NewVariableWidthColumn(b, api.SQL_C_WCHAR, size)
	case api.SQL_BINARY:
		return NewVariableWidthColumn(b, api.SQL_C_BINARY, size)
	// (eric) SIG-19527 As a workaround for a Databricks ODBC driver bug, we treat
	// SQL_(W)VARCHAR as SQL_(W)LONGVARCHAR (and SQL_VARBINARY as SQL_LONGVARBINARY).
	// Specifically, the Simbaspark ODBC driver's SQLDescribeCol API always returns
	// 256 bytes for the size of a VARCHAR column, regardless of its actual size. As
	// such, we're effectively ignoring that value.
	case api.SQL_LONGVARCHAR, api.SQL_VARCHAR:
		return NewVariableWidthColumn(b, api.SQL_C_CHAR, 0)
	case api.SQL_WLONGVARCHAR, api.SQL_WVARCHAR, api.SQL_SS_XML:
		return NewVariableWidthColumn(b, api.SQL_C_WCHAR, 0)
	case api.SQL_LONGVARBINARY, api.SQL_VARBINARY:
		return NewVariableWidthColumn(b, api.SQL_C_BINARY, 0)
	default:
		return nil, fmt.Errorf("unsupported column type %d", sqltype)
	}
}

// BaseColumn implements common column functionality.
type BaseColumn struct {
	name     string
	loc      *time.Location
	SQLType  api.SQLSMALLINT
	CType    api.SQLSMALLINT
	nullable api.SQLSMALLINT
}

func (c *BaseColumn) Name() string {
	return c.name
}

func (c *BaseColumn) Value(buf []byte) (driver.Value, error) {
	var p unsafe.Pointer
	if len(buf) > 0 {
		p = unsafe.Pointer(&buf[0])
	}
	loc := time.UTC
	if drv.Loc != nil {
		loc = drv.Loc
	}
	if c.loc != nil {
		loc = c.loc
	}
	switch c.CType {
	case api.SQL_C_BIT:
		return buf[0] != 0, nil
	case api.SQL_C_LONG:
		return *((*int32)(p)), nil
	case api.SQL_C_SBIGINT:
		return *((*int64)(p)), nil
	case api.SQL_C_DOUBLE:
		return *((*float64)(p)), nil
	case api.SQL_C_CHAR:
		return buf, nil
	case api.SQL_C_WCHAR:
		if p == nil {
			return buf, nil
		}
		s := (*[1 << 28]uint16)(p)[: len(buf)/2 : len(buf)/2]
		return utf16toutf8(s), nil
	case api.SQL_C_TYPE_TIMESTAMP:
		t := (*api.SQL_TIMESTAMP_STRUCT)(p)
		r := time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
			int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction),
			loc)
		return r, nil
	case api.SQL_C_GUID:
		t := (*api.SQLGUID)(p)
		var p1, p2 string
		for _, d := range t.Data4[:2] {
			p1 += fmt.Sprintf("%02x", d)
		}
		for _, d := range t.Data4[2:] {
			p2 += fmt.Sprintf("%02x", d)
		}
		r := fmt.Sprintf("%08x-%04x-%04x-%s-%s",
			t.Data1, t.Data2, t.Data3, p1, p2)
		return r, nil
	case api.SQL_C_DATE:
		t := (*api.SQL_DATE_STRUCT)(p)
		r := time.Date(int(t.Year), time.Month(t.Month), int(t.Day),
			0, 0, 0, 0, loc)
		return r, nil
	case api.SQL_C_TIME:
		t := (*api.SQL_TIME_STRUCT)(p)
		r := time.Date(1, time.January, 1,
			int(t.Hour), int(t.Minute), int(t.Second), 0, loc)
		return r, nil
	case api.SQL_C_BINARY:
		if c.SQLType == api.SQL_SS_TIME2 {
			t := (*api.SQL_SS_TIME2_STRUCT)(p)
			r := time.Date(1, time.January, 1,
				int(t.Hour), int(t.Minute), int(t.Second), int(t.Fraction),
				loc)
			return r, nil
		}
		return buf, nil
	}
	return nil, fmt.Errorf("unsupported column ctype %d", c.CType)
}

// Nullable returns true if the column is nullable and false otherwise.
// If the column nullability is unknown, ok is false.
func (c *BaseColumn) Nullable() (bool, bool) {
	return c.nullable == 1, true
}

// Returns the type that can be used to scan types into. For example, the
// database column type "bigint" this should return "reflect.TypeOf(int64(0))".
func (c *BaseColumn) ScanType() reflect.Type {
	switch c.SQLType {
	// CHAR
	case api.SQL_CHAR:
		return reflect.TypeOf("")
	case api.SQL_WCHAR:
		return reflect.TypeOf("")
	case api.SQL_VARCHAR:
		return reflect.TypeOf("")
	case api.SQL_WVARCHAR:
		return reflect.TypeOf("")
	case api.SQL_LONGVARCHAR:
		return reflect.TypeOf("")
	case api.SQL_WLONGVARCHAR:
		return reflect.TypeOf("")
	// XML
	case api.SQL_SS_XML:
		return reflect.TypeOf("")
	// BINARY
	case api.SQL_BINARY:
		return reflect.TypeOf([]byte{})
	case api.SQL_VARBINARY:
		return reflect.TypeOf([]byte{})
	case api.SQL_LONGVARBINARY:
		return reflect.TypeOf([]byte{})
	// NUMERIC FIXED LENGTH
	case api.SQL_BIT:
		return reflect.TypeOf(true)
	case api.SQL_TINYINT:
		return reflect.TypeOf(int32(0))
	case api.SQL_SMALLINT:
		return reflect.TypeOf(int32(0))
	case api.SQL_INTEGER:
		return reflect.TypeOf(int32(0))
	case api.SQL_BIGINT:
		return reflect.TypeOf(int64(0))
	case api.SQL_NUMERIC:
		return reflect.TypeOf([]byte{})
	case api.SQL_DECIMAL:
		return reflect.TypeOf([]byte{})
	case -25: // not declared in sql.h nor in sqlext.h
		return reflect.TypeOf(int64(0))
	// NUMERIC NOT FIXED LENGTH
	case api.SQL_REAL:
		return reflect.TypeOf(float64(0))
	case api.SQL_FLOAT:
		return reflect.TypeOf(float64(0))
	case api.SQL_DOUBLE:
		return reflect.TypeOf(float64(0))
	// DATE / TIME
	case api.SQL_TYPE_DATE:
		return reflect.TypeOf(time.Time{})
	case api.SQL_TYPE_TIME:
		return reflect.TypeOf(time.Time{})
	case api.SQL_SS_TIME2:
		return reflect.TypeOf(time.Time{})
	case api.SQL_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	// GUID
	case api.SQL_GUID:
		// better to return an uuid?
		return reflect.TypeOf([]byte{})
	default:
		panic(fmt.Sprintf("not implemented ScanType() for type %v", c.CType))
	}
}

// BindableColumn allows access to columns that can have their buffers
// bound. Once bound at start, they are written to by odbc driver every
// time it fetches new row. This saves on syscall and, perhaps, some
// buffer copying. BindableColumn can be left unbound, then it behaves
// like NonBindableColumn when user reads data from it.
type BindableColumn struct {
	*BaseColumn
	IsBound         bool
	IsVariableWidth bool
	Size            int
	Len             BufferLen
	Buffer          []byte
}

// TODO(brainman): BindableColumn.Buffer is used by external code after external code returns - that needs to be avoided in the future

func NewBindableColumn(b *BaseColumn, ctype api.SQLSMALLINT, bufSize int) *BindableColumn {
	b.CType = ctype
	c := &BindableColumn{BaseColumn: b, Size: bufSize}
	l := 8 // always use small starting buffer
	if c.Size > l {
		l = c.Size
	}
	c.Buffer = make([]byte, l)
	return c
}

func NewVariableWidthColumn(b *BaseColumn, ctype api.SQLSMALLINT, colWidth api.SQLULEN) (Column, error) {
	if colWidth == 0 || colWidth > 1024 {
		b.CType = ctype
		return &NonBindableColumn{b}, nil
	}
	l := int(colWidth)
	switch ctype {
	case api.SQL_C_WCHAR:
		l += 1 // room for null-termination character
		l *= 2 // wchars take 2 bytes each
	case api.SQL_C_CHAR:
		l += 1 // room for null-termination character
	case api.SQL_C_BINARY:
		// nothing to do
	default:
		return nil, fmt.Errorf("do not know how wide column of ctype %d is", ctype)
	}
	c := NewBindableColumn(b, ctype, l)
	c.IsVariableWidth = true
	return c, nil
}

func (c *BindableColumn) Bind(h api.SQLHSTMT, idx int) (bool, error) {
	ret := c.Len.Bind(h, idx, c.CType, c.Buffer)
	if IsError(ret) {
		return false, NewError("SQLBindCol", h)
	}
	c.IsBound = true
	return true, nil
}

func (c *BindableColumn) Value(h api.SQLHSTMT, idx int) (driver.Value, error) {
	if !c.IsBound {
		ret := c.Len.GetData(h, idx, c.CType, c.Buffer)
		if IsError(ret) {
			return nil, NewError("SQLGetData", h)
		}
	}
	if c.Len.IsNull() {
		// is NULL
		return nil, nil
	}
	if !c.IsVariableWidth && int(c.Len) != c.Size {
		return nil, fmt.Errorf("wrong column #%d length %d returned, %d expected", idx, c.Len, c.Size)
	}
	return c.BaseColumn.Value(c.Buffer[:c.Len])
}

// NonBindableColumn provide access to columns, that can't be bound.
// These are of character or binary type, and, usually, there is no
// limit for their width.
type NonBindableColumn struct {
	*BaseColumn
}

func (c *NonBindableColumn) Bind(h api.SQLHSTMT, idx int) (bool, error) {
	return false, nil
}

func (c *NonBindableColumn) Value(h api.SQLHSTMT, idx int) (driver.Value, error) {
	var l BufferLen
	var total []byte
	b := make([]byte, 1024)
loop:
	for {
		ret := l.GetData(h, idx, c.CType, b)
		switch ret {
		case api.SQL_SUCCESS:
			if l.IsNull() {
				// is NULL
				return nil, nil
			}
			if int(l) > len(b) {
				return nil, fmt.Errorf("too much data returned: %d bytes returned, but buffer size is %d", l, cap(b))
			}
			total = append(total, b[:l]...)
			break loop
		case api.SQL_SUCCESS_WITH_INFO:
			err := NewError("SQLGetData", h).(*Error)
			if len(err.Diag) > 0 && err.Diag[0].State != "01004" {
				return nil, err
			}
			i := len(b)
			switch c.CType {
			case api.SQL_C_WCHAR:
				i -= 2 // remove wchar (2 bytes) null-termination character
			case api.SQL_C_CHAR:
				i-- // remove null-termination character
			}
			total = append(total, b[:i]...)
			if l != api.SQL_NO_TOTAL {
				// odbc gives us a hint about remaining data,
				// lets get it in one go.
				n := int(l) // total bytes for our data
				n -= i      // subtract already received
				n += 2      // room for biggest (wchar) null-terminator
				if len(b) < n {
					b = make([]byte, n)
				}
			}
		default:
			return nil, NewError("SQLGetData", h)
		}
	}
	return c.BaseColumn.Value(total)
}
