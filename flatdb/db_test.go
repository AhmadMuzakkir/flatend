package flatdb

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func getDB(t *testing.T, path string) (*sql.DB, func()) {
	os.RemoveAll(path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Error(err)
	}

	sqlStmt := `
	create table user (id integer unique, username text unique, password text);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		t.Error(err)
	}

	_, err = db.Exec("insert into user(id, username, password) values(?, ?, ?)", 1, "alice", "alice-password")
	if err != nil {
		t.Error(err)
	}

	return db, func() {
		os.RemoveAll(path)
	}
}

func TestSQL_Query(t *testing.T) {
	db, cleanup := getDB(t, "./flatendsqlite_1.db")
	defer cleanup()

	s := SQL{
		db: db,
	}

	ctx := context.Background()

	// Test matches with string arg.

	args := map[string]interface{}{"username": "alice"}
	rows, err := s.Query(ctx, "select * from user where username = :username", args)
	if err != nil {
		t.Error(err)
	}

	if !rows.Next() {
		t.Error("expected one row")
	}

	// Test matches with int arg.

	args = map[string]interface{}{"id": 1}
	rows, err = s.Query(ctx, "select * from user where id = :id", args)
	if err != nil {
		t.Error(err)
	}

	if !rows.Next() {
		t.Error("expected one row")
	}

	// Test no matches.

	args = map[string]interface{}{"username": "bob"}

	rows, err = s.Query(ctx, "select * from user where username = :username", args)
	if err != nil {
		t.Error(err)
	}

	if rows.Next() {
		t.Error("expected zero row")
	}
}

func TestSQL_Insert(t *testing.T) {
	db, cleanup := getDB(t, "./flatendsqlite_2.db")
	defer cleanup()

	s := SQL{
		db: db,
	}

	ctx := context.Background()

	args := map[string]interface{}{
		"id":       47,
		"username": "alice",
		"password": "5004",
	}

	res, err := s.Exec(ctx, "insert into user(id, username, password) values(:id, :username, :password)", args)
	if err != nil {
		t.Error(err)
	}

	if n, err := res.RowsAffected(); err != nil {
		t.Error(err)
	} else if n != 1 {
		t.Error("expected 1 row affected")
	}
}
