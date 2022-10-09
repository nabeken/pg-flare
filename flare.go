package flare

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type TrafficGenerator struct {
	pool *pgxpool.Pool
}

func NewTrafficGenerator(pool *pgxpool.Pool) *TrafficGenerator {
	return &TrafficGenerator{pool: pool}
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

	tx, err := g.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning a new transaction: %w", err)
	}

	if _, err := tx.Exec(
		ctx,
		`INSERT into items values($1, $2);`,
		uuid.NewString(),
		uuid.NewString(),
	); err != nil {
		return fmt.Errorf("inserting a new item: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
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

func CreateTestTable(ctx context.Context, connConfig ConnConfig, dbUser string, dropDBBefore bool) error {
	conn, err := Connect(ctx, connConfig, "postgres")
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

	newConn, err := Connect(ctx, connConfig, "flare_test")
	if err != nil {
		return fmt.Errorf("chaging to the new database: %w", err)
	}

	if _, err := newConn.Exec(ctx, flareDatabaseSchema); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	if _, err := newConn.Exec(
		ctx,
		fmt.Sprintf(`GRANT ALL ON items TO %s;`, quoteIdentifier(dbUser)),
	); err != nil {
		return fmt.Errorf("granting access to the dbuser: %w", err)
	}

	return nil
}

func DumpRoles(connConfig ConnConfig, noPasswords bool) (string, error) {
	args := connConfig.PSQLArgs()
	args.Args = []string{"--roles-only"}

	if noPasswords {
		args.Args = append(args.Args, "--no-role-passwords")
	}

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

func CreatePublicationQuery(pubname string) string {
	return fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES;`, quoteIdentifier(pubname))
}

func AlterTableReplicaIdentityFull(tbl string) string {
	return fmt.Sprintf(`ALTER TABLE %s REPLICA IDENTITY FULL;`, quoteIdentifier(tbl))
}

func CreateSubscriptionQuery(subName, connInfo, pubName string) string {
	return fmt.Sprintf(
		`CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s;`,
		quoteIdentifier(subName),
		connInfo,
		quoteIdentifier(pubName),
	)
}

func RevokeConnectionQuery(dbName string) string {
	return fmt.Sprintf(
		`REVOKE CONNECT ON DATABASE %s FROM PUBLIC;`,
		quoteIdentifier(dbName),
	)
}

func GrantConnectionQuery(dbName string) string {
	return fmt.Sprintf(
		`GRANT CONNECT ON DATABASE %s TO PUBLIC;`,
		quoteIdentifier(dbName),
	)
}

func CreateExtensionQuery(ext string) string {
	return fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %s`, quoteIdentifier(ext))
}

func GrantCreateQuery(dbName, user string) string {
	return fmt.Sprintf(`GRANT CREATE ON DATABASE %s TO %s;`, quoteIdentifier(dbName), quoteIdentifier(user))
}

const KillConnectionQuery = `
	SELECT pg_terminate_backend(pid)
	FROM pg_stat_activity
	WHERE
		  pid <> pg_backend_pid()
	  AND usename <> 'postgres' -- skip replication slots
	  AND datname = $1
	;`

type Config struct {
	Hosts         Hosts                   `yaml:"hosts"`
	Publications  map[string]Publication  `yaml:"publications"`
	Subscriptions map[string]Subscription `yaml:"subscriptions"`
}

type Hosts struct {
	Publisher  Host `yaml:"publisher"`
	Subscriber Host `yaml:"subscriber"`
}

type Host struct {
	Conn ConnConfig `yaml:"conn"`
}

type Publication struct {
	PubName                   string   `yaml:"pubname"`
	ReplicaIdentityFullTables []string `yaml:"replica_identity_full_tables"`
}

type Subscription struct {
	DBName  string `yaml:"dbname"`
	PubName string `yaml:"pubname"`
}

type ConnConfig struct {
	User     string `yaml:"user" validate:"required"`
	Password string `yaml:"password" validate:"required"`

	DBOwner         string `yaml:"db_owner"`
	DBOwnerPassword string `yaml:"db_owner_password"`

	Host              string `yaml:"host" validate:"required"`
	HostViaSubscriber string `yaml:"host_via_subscriber"`

	Port              string `yaml:"port" validate:"required"`
	PortViaSubscriber string `yaml:"port_via_subscriber"`

	SystemIdentifier string `yaml:"system_identifier" validate:"required"`
}

func (c ConnConfig) DSNURI(dbName string) string {
	up := url.UserPassword(c.User, c.Password)

	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s",
		up.String(),
		c.Host, c.Port,
		dbName,
	)
}

func (c ConnConfig) DSNURIForSubscriber(dbName string) string {
	host := c.Host
	if shost := c.HostViaSubscriber; shost != "" {
		host = shost
	}

	port := c.Port
	if sport := c.PortViaSubscriber; sport != "" {
		port = sport
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		c.User, c.Password,
		host, port,
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

func ListInstalledExtensions(ctx context.Context, conn *Conn) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT extname FROM pg_extension order by extname;`)
	if err != nil {
		return nil, err
	}

	exts := []string{}

	for rows.Next() {
		var ext string
		if err := rows.Scan(&ext); err != nil {
			return nil, err
		}

		exts = append(exts, ext)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return exts, nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
