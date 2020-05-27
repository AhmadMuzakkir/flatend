package flatdb

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func getDB(t *testing.T) (*sql.DB, func()) {
	os.RemoveAll("./flatendsqlite.db")

	db, err := sql.Open("sqlite3", "flatendsqlite.db")
	if err != nil {
		t.Fatal(err)
	}

	sqlStmt := `
	create table user (id integer unique, username text unique, password text);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("insert into user(id, username, password) values(?, ?, ?)", "1", "alice", "alice-password")
	if err != nil {
		t.Fatal(err)
	}

	return db, func() {
		os.RemoveAll("./flatendsqlite.db")
	}
}

func TestSQL_Query(t *testing.T) {
	db, cleanup := getDB(t)
	defer cleanup()

	s := SQL{
		db: db,
	}

	// Test matches

	params := map[string]string{"username": "alice"}
	rows, err := s.Query("select * from user where username = :username", params)
	if err != nil {
		t.Fatal(err)
	}

	if !rows.Next() {
		t.Errorf("expected one row")
	}

	// Test no matches

	params["username"] = "bob"
	rows, err = s.Query("select * from user where username = :username", params)
	if err != nil {
		t.Fatal(err)
	}

	if rows.Next() {
		t.Errorf("expected zero row")
	}
}
