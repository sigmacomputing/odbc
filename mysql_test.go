// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package odbc

import (
	"database/sql"
	"flag"
	"fmt"
	"reflect"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

var (
	mysrv  = flag.String("mysrv", "server", "mysql server name")
	mydb   = flag.String("mydb", "dbname", "mysql database name")
	myuser = flag.String("myuser", "", "mysql user name")
	mypass = flag.String("mypass", "", "mysql password")
)

func mysqlConnect() (db *sql.DB, stmtCount int, err error) {
	// from https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-configuration-connection-parameters.html
	conn := fmt.Sprintf("driver=mysql;server=%s;database=%s;user=%s;password=%s;",
		*mysrv, *mydb, *myuser, *mypass)
	db, err = sql.Open("odbc", conn)
	if err != nil {
		return nil, 0, err
	}
	stats := db.Driver().(*Driver).Stats
	return db, stats.StmtCount, nil
}

func TestMYSQLTime(t *testing.T) {
	db, sc, err := mysqlConnect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB(t, db, sc, sc)

	db.Exec("drop table temp")
	exec(t, db, "create table temp(id int not null auto_increment primary key, time time)")
	now := time.Now()
	// SQL_TIME_STRUCT only supports hours, minutes and seconds
	now = time.Date(1, time.January, 1, now.Hour(), now.Minute(), now.Second(), 0, time.Local)
	_, err = db.Exec("insert into temp (time) values(?)", now)
	if err != nil {
		t.Fatal(err)
	}

	var ret time.Time
	if err := db.QueryRow("select time from temp where id = ?", 1).Scan(&ret); err != nil {
		t.Fatal(err)
	}
	if ret != now {
		t.Fatalf("unexpected return value: want=%v, is=%v", now, ret)
	}

	exec(t, db, "drop table temp")
}

func TestMYSQLNullability(t *testing.T) {
	db, sc, err := mysqlConnect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB(t, db, sc, sc)

	db.Exec("drop table temp")
	exec(t, db, `create table temp(name nvarchar(50), age int)`)

	rows, err := db.Query("select age from temp where name=?", "v")
	cTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	for _, colType := range cTypes {
		typ := *colType
		if nullable, ok := typ.Nullable(); nullable == false || ok == false {
			t.Fatalf("column did not contain nullability information")
		}

	}
	rows.Close()

	exec(t, db, "drop table temp")
}

func TestMYSQLScanType(t *testing.T) {
	db, sc, err := mysqlConnect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB(t, db, sc, sc)

	db.Exec("drop table temp")
	exec(t, db, `create table temp(name nvarchar(50), age int)`)

	rows, err := db.Query("select age from temp where name=?", "v")
	cTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	for _, colType := range cTypes {
		typ := *colType
		assert.Equal(t, reflect.TypeOf(int64(0)), typ.ScanType())
	}
	rows.Close()

	exec(t, db, "drop table temp")
}
