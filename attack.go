package flare

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const flareDatabaseSchema = `
CREATE TABLE IF NOT EXISTS items (
    id   TEXT PRIMARY KEY
  , name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id                TEXT PRIMARY KEY
  , last_keepalive_at TIMESTAMP WITH TIME ZONE NOT NULL
);
`

type TrafficGenerator struct {
	pool *pgxpool.Pool
	name string
}

func NewTrafficGenerator(pool *pgxpool.Pool, name string) *TrafficGenerator {
	return &TrafficGenerator{pool: pool, name: name}
}

func (g *TrafficGenerator) KeepAlive(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Stop sending heartbeat...")
			return nil
		default:
		}

		if err := g.SendHeartBeat(ctx); err != nil {
			log.Printf("Failed to write a new item: %s", err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (g *TrafficGenerator) Attack(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Stop writing new items...")
			return nil
		default:
		}

		if err := g.WriteNewItem(ctx); err != nil {
			log.Printf("Failed to write a new item: %s", err)
		}
	}
}

func (g *TrafficGenerator) SendHeartBeat(ctx context.Context) error {
	txctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := g.pool.Begin(txctx)
	if err != nil {
		return fmt.Errorf("beginning a new transaction: %w", err)
	}

	if _, err := tx.Exec(
		txctx, `
INSERT into sessions values ($1, $2)
ON CONFLICT (id)
DO
UPDATE SET last_keepalive_at = $2
;`,
		g.name,
		time.Now(),
	); err != nil {
		return fmt.Errorf("updating the session: %w", err)
	}

	if err := tx.Commit(txctx); err != nil {
		return fmt.Errorf("commiting the item: %w", err)
	}

	return nil
}

func (g *TrafficGenerator) WriteNewItem(ctx context.Context) error {
	txctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := g.pool.Begin(txctx)
	if err != nil {
		return fmt.Errorf("beginning a new transaction: %w", err)
	}

	if _, err := tx.Exec(
		txctx,
		`INSERT into items values($1, $2);`,
		uuid.NewString(),
		uuid.NewString(),
	); err != nil {
		return fmt.Errorf("inserting a new item: %w", err)
	}

	if err := tx.Commit(txctx); err != nil {
		return fmt.Errorf("commiting the item: %w", err)
	}

	return nil
}

func CreateTestTable(ctx context.Context, baseDSN, appUser string, dropDBBefore bool) error {
	const dbName = "flare_test"

	dsn, err := switchDatabase(baseDSN, "postgres")
	if err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if dropDBBefore {
		if _, err = conn.Exec(ctx, `DROP DATABASE flare_test;`); err != nil {
			return fmt.Errorf("dropping a database: %w", err)
		}
	}

	if _, err = conn.Exec(ctx, `CREATE DATABASE flare_test;`); err != nil {
		return fmt.Errorf("creating a database: %w", err)
	}

	dsn, err = switchDatabase(baseDSN, dbName)
	if err != nil {
		return err
	}

	newConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("chaging to the new database: %w", err)
	}

	if _, err := newConn.Exec(ctx, flareDatabaseSchema); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	if _, err := newConn.Exec(
		ctx,
		fmt.Sprintf(`GRANT ALL ON ALL TABLES In SCHEMA public TO %s;`, quoteIdentifier(appUser)),
	); err != nil {
		return fmt.Errorf("granting access to the app user: %w", err)
	}

	return nil
}

func switchDatabase(baseDSN, dbName string) (string, error) {
	dsn, err := url.Parse(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parsing the base DSN: %s", err)
	}

	dsn.Path = dbName

	return dsn.String(), nil
}
