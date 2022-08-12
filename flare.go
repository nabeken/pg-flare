package flare

import (
	"database/sql"
	"fmt"
	"strings"
)

type TrafficGenerator struct {
	connConfig ConnConfig
}

func NewTrafficGenerator(connConfig ConnConfig) *TrafficGenerator {
	return &TrafficGenerator{connConfig: connConfig}
}

const flareDatabaseSchema = `
CREATE TABLE IF NOT EXISTS items (
    id   TEXT PRIMARY KEY
  , name TEXT NOT NULL
);
`

func CreateTestTable(suc SuperUserConfig, dbOwner string, dropDBBefore bool) error {
	db, err := suc.Open()
	if err != nil {
		return fmt.Errorf("opening the database connection with the super user: %w", err)
	}

	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging the database: %w", err)
	}

	if dropDBBefore {
		if _, err = db.Exec(`DROP DATABASE flare_test;`); err != nil {
			return fmt.Errorf("dropping a database: %w", err)
		}
	}

	if _, err = db.Exec(`CREATE DATABASE flare_test;`); err != nil {
		return fmt.Errorf("creating a database: %w", err)
	}

	if _, err := db.Exec(
		fmt.Sprintf(`ALTER DATABASE flare_test OWNER TO %s;`, quoteIdentifier(dbOwner)),
	); err != nil {
		return fmt.Errorf("updating the owner of the database: %w", err)
	}

	newSUC, err := suc.SwitchDatabase("flare_test")
	if err != nil {
		return fmt.Errorf("chaging to the new database: %w", err)
	}

	newDB, err := newSUC.Open()
	if err != nil {
		return fmt.Errorf("switching to the new database: %w", err)
	}

	if _, err := newDB.Exec(flareDatabaseSchema); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	return nil
}

func Open(connString string) (*sql.DB, error) {
	return sql.Open("pgx", connString)
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
