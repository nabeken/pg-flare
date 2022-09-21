package flare

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
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

//func CreateTestTable(suc SuperUserConfig, dbUser string, dropDBBefore bool) error {
//	db, err := suc.Open()
//	if err != nil {
//		return fmt.Errorf("opening the database connection with the super user: %w", err)
//	}
//
//	defer db.Close()
//
//	if err := db.Ping(); err != nil {
//		return fmt.Errorf("pinging the database: %w", err)
//	}
//
//	if dropDBBefore {
//		if _, err = db.Exec(`DROP DATABASE flare_test;`); err != nil {
//			return fmt.Errorf("dropping a database: %w", err)
//		}
//	}
//
//	if _, err = db.Exec(`CREATE DATABASE flare_test;`); err != nil {
//		return fmt.Errorf("creating a database: %w", err)
//	}
//
//	newSUC, err := suc.SwitchDatabase("flare_test")
//	if err != nil {
//		return fmt.Errorf("chaging to the new database: %w", err)
//	}
//
//	newDB, err := newSUC.Open()
//	if err != nil {
//		return fmt.Errorf("switching to the new database: %w", err)
//	}
//
//	if _, err := newDB.Exec(flareDatabaseSchema); err != nil {
//		return fmt.Errorf("creating tables: %w", err)
//	}
//
//	if _, err := newDB.Exec(
//		fmt.Sprintf(`GRANT ALL ON items TO %s;`, quoteIdentifier(dbUser)),
//	); err != nil {
//		return fmt.Errorf("granting access to the dbuser: %w", err)
//	}
//
//	return nil
//}
//

func DumpRoles(connConfig ConnConfig) (string, error) {
	args := connConfig.PSQLArgs()
	args.Args = []string{"--roles-only"}

	return PGDumpAll(args)
}

func DumpSchema(connConfig ConnConfig, db string) (string, error) {
	args := connConfig.PSQLArgs()
	args.Args = []string{
		"--schema-only",
		"--create",
	}

	return PGDump(args, db)
}

//
//func CreatePublicationQuery(pubname string) string {
//	return fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES;`, quoteIdentifier(pubname))
//}
//
//func AlterTableReplicaIdentityFull(tbl string) string {
//	return fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL;`, quoteIdentifier(tbl))
//}
//
//func CreateSubscriptionQuery(subName, pubDSN, pubName string) string {
//	return fmt.Sprintf(`CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s;`, subName, pubDSN, pubName)
//}

type Config struct {
	Hosts Hosts `yaml:"hosts"`
}

type Hosts struct {
	Publisher  Host `yaml:"publisher"`
	Subscriber Host `yaml:"subscriber"`
}

type Host struct {
	Conn ConnConfig `yaml:"conn"`
}

type ConnConfig struct {
	User             string `yaml:"user" validate:"required"`
	Password         string `yaml:"password" validate:"required"`
	Host             string `yaml:"host" validate:"required"`
	Port             string `yaml:"port" validate:"required"`
	SystemIdentifier string `yaml:"system_identifier" validate:"required"`
}

func (c ConnConfig) DSNURI(dbName string) string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		c.User, c.Password,
		c.Host, c.Port,
		dbName,
	)
}

func (c ConnConfig) PSQLArgs() PSQLArgs {
	return PSQLArgs{
		User: c.User,
		Pass: c.Password,
		Host: c.Host,
		Port: c.Port,
	}
}

func ParseConfig(b []byte) (Config, error) {
	cfg := Config{}

	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}

	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

type PSQLArgs struct {
	User string
	Pass string
	Host string
	Port string
	Args []string
}

func (a PSQLArgs) BuildArgs() []string {
	var args []string

	if a.User != "" {
		args = append(args, []string{"-U", a.User}...)
	}
	if a.Host != "" {
		args = append(args, []string{"-h", a.Host}...)
	}
	if a.Port != "" {
		args = append(args, []string{"-p", a.Port}...)
	}

	return append(args, a.Args...)
}

func PSQL(args PSQLArgs, db string, r io.Reader) (string, string, error) {
	dumpArgs := []string{}
	dumpArgs = append(dumpArgs, args.BuildArgs()...)

	cmd := exec.Command("psql", append(dumpArgs, db)...)
	cmd.Env = []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("PGPASSWORD=%s", args.Pass),
	}

	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdin = r
	cmd.Stdout = &out
	cmd.Stderr = &errout

	if err := cmd.Run(); err != nil {
		return "", "", fmt.Errorf("psql: %w: %s", err, errout.String())
	}

	return out.String(), errout.String(), nil
}

func PGDump(args PSQLArgs, db string) (string, error) {
	dumpArgs := []string{}
	dumpArgs = append(dumpArgs, args.BuildArgs()...)

	cmd := exec.Command("pg_dump", append(dumpArgs, db)...)
	cmd.Env = []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("PGPASSWORD=%s", args.Pass),
	}

	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pg_dump: %w: %s", err, errout.String())
	}

	return out.String(), nil
}

func PGDumpAll(args PSQLArgs) (string, error) {
	dumpArgs := []string{}
	dumpArgs = append(dumpArgs, args.BuildArgs()...)

	cmd := exec.Command("pg_dumpall", dumpArgs...)
	cmd.Env = []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("PGPASSWORD=%s", args.Pass),
	}

	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pg_dumpall: %w: %s", err, errout.String())
	}

	return out.String(), nil
}

// Conn wraps *pgx.Conn to provider additional features on top of it.
type Conn struct {
	*pgx.Conn

	connConfig ConnConfig
	dbName     string
}

type SystemIdentifierError struct {
	Expected string
	Got      string
}

func (e SystemIdentifierError) Error() string {
	return fmt.Sprintf(
		"flare: system_identifier doesn't match! Got '%s', expected '%s'",
		e.Got, e.Expected,
	)
}

func (c *Conn) getSystemIdentifier(ctx context.Context) (string, error) {
	var identifier int64

	if err := c.Conn.QueryRow(
		ctx,
		"SELECT system_identifier FROM pg_control_system();",
	).Scan(&identifier); err != nil {
		return "", fmt.Errorf("querying the system identifier: %w", err)
	}

	return strconv.FormatInt(identifier, 10), nil
}

func (c *Conn) VerifySystemIdentifier(ctx context.Context) error {
	identifierStr, err := c.getSystemIdentifier(ctx)
	if err != nil {
		return err
	}

	if c.connConfig.SystemIdentifier != identifierStr {
		return SystemIdentifierError{
			Expected: c.connConfig.SystemIdentifier,
			Got:      identifierStr,
		}
	}

	return nil
}

func ConnectWithVerify(ctx context.Context, connConfig ConnConfig, dbName string) (*Conn, error) {
	fconn, err := Connect(ctx, connConfig, dbName)
	if err != nil {
		return nil, err
	}

	if err := fconn.VerifySystemIdentifier(ctx); err != nil {
		defer fconn.Close(ctx)

		return nil, fmt.Errorf("flare: verifying the identity: %w", err)
	}

	return fconn, nil
}

func Connect(ctx context.Context, connConfig ConnConfig, dbName string) (*Conn, error) {
	conn, err := pgx.Connect(ctx, connConfig.DSNURI(dbName))
	if err != nil {
		return nil, err
	}

	return &Conn{
		Conn:       conn,
		connConfig: connConfig,
		dbName:     dbName,
	}, nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
