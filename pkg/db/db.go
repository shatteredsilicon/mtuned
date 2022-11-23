package db

import (
	"database/sql"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

const (
	invalidConnRetryTimes = 3
)

type executor interface {
	Select(interface{}, string, ...interface{}) error
	Get(interface{}, string, ...interface{}) error
	Exec(string, ...interface{}) (sql.Result, error)
	Ping() error
}

// Executor wrapped sql executor
type Executor struct {
	executor
}

// DB wrapped database structure
type DB struct {
	*Executor
}

// NewDB returns a wrapper of sqlx.DB
func NewDB() *DB {
	return &DB{
		Executor: &Executor{
			executor: db,
		},
	}
}

var db *sqlx.DB
var lastTooManyConnTime *time.Time

// Init setup db connection
func Init(cfg *mysql.Config) error {
	var (
		err error
	)

	db, err = sqlx.Connect("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}

	db.SetMaxIdleConns(0)
	return nil
}

// Select runs sqlx Select function,
func (e *Executor) Select(dest interface{}, query string, args ...interface{}) (err error) {
	err = e.executor.Select(dest, query, args...)
	checkDBError(err)
	return
}

// Get runs sqlx Get function
func (e *Executor) Get(dest interface{}, query string, args ...interface{}) (err error) {
	err = e.executor.Get(dest, query, args...)
	checkDBError(err)
	return
}

// Exec runs sqlx Exec function
func (e *Executor) Exec(query string, args ...interface{}) (result sql.Result, err error) {
	result, err = e.executor.Exec(query, args...)
	checkDBError(err)
	return
}

func checkDBError(err error) {
	if err == nil {
		return
	}

	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return
	}

	switch mysqlErr.Number {
	case 1040: // Too many connections error
		now := time.Now()
		lastTooManyConnTime = &now
	}
}

// LastTooManyConnTime returns the time that
// last 'Too many connection' error occurs
func LastTooManyConnTime() *time.Time {
	return lastTooManyConnTime
}
