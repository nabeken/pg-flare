package flare

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
)

type TrafficGenerator struct {
	db *sql.DB
}

func NewTrafficGenerator(db *sql.DB) *TrafficGenerator {
	return &TrafficGenerator{db: db}
}

func (g *TrafficGenerator) Attack(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Stop writing new items...")
			return nil
		default:
		}

		if err := g.WriteNewItem(); err != nil {
			log.Printf("Failed to write a new item: %s", err)
		}
	}
}

func (g *TrafficGenerator) WriteNewItem() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning a new transaction: %w", err)
	}

	if _, err := tx.Exec(
		`INSERT into items values($1, $2);`,
		uuid.NewString(),
		uuid.NewString(),
	); err != nil {
		return fmt.Errorf("inserting a new item: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commiting the item: %w", err)
	}

	return nil
}

const flareDatabaseSchema = `
CREATE TABLE IF NOT EXISTS items (
    id   TEXT PRIMARY KEY
  , name TEXT NOT NULL
);
`

func CreateTestTable(suc SuperUserConfig, dbUser string, dropDBBefore bool) error {
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

	if _, err := newDB.Exec(
		fmt.Sprintf(`GRANT ALL ON items TO %s;`, quoteIdentifier(dbUser)),
	); err != nil {
		return fmt.Errorf("granting access to the dbuser: %w", err)
	}

	return nil
}

func Open(connString string) (*sql.DB, error) {
	return sql.Open("pgx", connString)
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func DumpRoles(suc SuperUserConfig) (string, error) {
	args, err := suc.ConnConfig.PSQLArgs()
	if err != nil {
		return "", fmt.Errorf("dump roles: %w", err)
	}
	args.Args = []string{"--roles-only"}

	return PGDumpAll(args)
}
