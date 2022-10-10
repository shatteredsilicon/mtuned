package db

import (
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

// Init setup db connection
func Init(cfg *mysql.Config) error {
	var (
		err error
	)

	db, err = sqlx.Connect("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}

	return db.Ping()
}
