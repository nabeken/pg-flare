package flare

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/jackc/pgtype/zeronull"
	"github.com/jackc/pgx/v4"
)

func CreateFlareStatusTable(ctx context.Context, conn *Conn) error {
	const tableSchema = `
CREATE TABLE IF NOT EXISTS flare_replication_status (
   system_identifier  TEXT PRIMARY KEY
 , uuid               TEXT NOT NULL
 , created_at         TEXT NOT NULL
);
`

	if _, err := conn.Exec(ctx, tableSchema); err != nil {
		return fmt.Errorf("creating the status table: %w", err)
	}

	return nil
}

func WriteReplicationStatus(ctx context.Context, conn *Conn, sysID, uuid string) error {
	if _, err := conn.Exec(
		ctx,
		`INSERT INTO flare_replication_status VALUES ($1, $2, now());`,
		sysID, uuid,
	); err != nil {
		return fmt.Errorf("writing the replication status to the table: %w", err)
	}

	return nil
}

func DeleteReplicationStatus(ctx context.Context, conn *Conn, sysID string) error {
	if _, err := conn.Exec(
		ctx,
		`DELETE FROM flare_replication_status WHERE system_identifier = $1;`,
		sysID,
	); err != nil {
		return fmt.Errorf("deleting the replication status from the table: %w", err)
	}

	return nil
}

func ReadReplicationStatus(ctx context.Context, conn *Conn, sysID, uuid string) error {
	var exists bool
	err := conn.QueryRow(
		ctx,
		`SELECT TRUE
		 FROM flare_replication_status
		 WHERE system_identifier = $1 AND uuid = $2;`,
		sysID, uuid,
	).Scan(&exists)

	if err != nil {
		return fmt.Errorf("querying the replication status: %w", err)
	}

	return nil
}

func StripRoleOptionsForRDS(roles string) (string, error) {
	rr := strings.NewReplacer(
		" NOSUPERUSER", "",
		" NOREPLICATION", "",
		//" NOBYPASSRLS", "",
	)

	r := strings.NewReader(roles)
	scanner := bufio.NewScanner(r)

	var b strings.Builder

	for scanner.Scan() {
		t := scanner.Text()
		if strings.HasPrefix(t, "ALTER ROLE") {
			t = rr.Replace(t)
		}
		fmt.Fprintf(&b, "%s\n", t)
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scanning the roles: %w", err)
	}

	return b.String(), nil
}

func DumpRoles(ui UserInfo, noPasswords bool) (string, error) {
	args := ui.PSQLArgs()
	args.Args = []string{"--roles-only"}

	if noPasswords {
		args.Args = append(args.Args, "--no-role-passwords")
	}

	return PGDumpAll(args)
}

func DumpSchema(ui UserInfo, db string) (string, error) {
	args := ui.PSQLArgs()
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

func DropSubscriptionQuery(subName string) string {
	return fmt.Sprintf(
		`DROP SUBSCRIPTION %s;`,
		quoteIdentifier(subName),
	)
}

func DropPublicationQuery(pubName string) string {
	return fmt.Sprintf(
		`DROP PUBLICATION %s;`,
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

func GrantAllOnDatabaseQuery(dbName, role string) string {
	return fmt.Sprintf(
		`GRANT ALL ON DATABASE %s TO %s;`,
		quoteIdentifier(dbName),
		quoteIdentifier(role),
	)
}

func GrantAllOnAllTablesQuery(role string) string {
	return fmt.Sprintf(
		`GRANT ALL ON ALL TABLES IN SCHEMA public TO %s;`,
		quoteIdentifier(role),
	)
}

func CreateExtensionQuery(ext string) string {
	return fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %s`, quoteIdentifier(ext))
}

func GrantCreateQuery(dbName, user string) string {
	return fmt.Sprintf(`GRANT CREATE ON DATABASE %s TO %s;`, quoteIdentifier(dbName), quoteIdentifier(user))
}

func GrantConnectQuery(dbName, user string) string {
	return fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s;`, quoteIdentifier(dbName), quoteIdentifier(user))
}

const KillConnectionQuery = `
	SELECT pg_terminate_backend(pid)
	FROM pg_stat_activity
	WHERE
		  pid <> pg_backend_pid()
	  AND usename = $1 -- only kill the application session
	  AND datname = $2
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

type HostInfo struct {
	Host              string
	HostViaSubscriber string

	Port              string
	PortViaSubscriber string

	SystemIdentifier string
}

type UserInfo struct {
	User     string
	Password string

	hi HostInfo
}

func (ui UserInfo) WithHostInfo(hi HostInfo) UserInfo {
	ui.hi = hi
	return ui
}

func (ui UserInfo) DSNURI(dbName string) string {
	up := url.UserPassword(ui.User, ui.Password)

	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s",
		up.String(),
		ui.hi.Host, ui.hi.Port,
		dbName,
	)
}

func (ui UserInfo) DSNURIForSubscriber(dbName string) string {
	host := ui.hi.Host
	if shost := ui.hi.HostViaSubscriber; shost != "" {
		host = shost
	}

	port := ui.hi.Port
	if sport := ui.hi.PortViaSubscriber; sport != "" {
		port = sport
	}

	up := url.UserPassword(ui.User, ui.Password)

	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s",
		up.String(),
		host, port,
		dbName,
	)
}

func (ui UserInfo) PSQLArgs() PSQLArgs {
	return PSQLArgs{
		User: ui.User,
		Pass: ui.Password,
		Host: ui.hi.Host,
		Port: ui.hi.Port,
	}
}

type ConnConfig struct {
	SuperUser         string `yaml:"superuser" validate:"required"`
	SuperUserPassword string `yaml:"superuser_password" validate:"required"`

	DBOwner         string `yaml:"db_owner" validate:"required"`
	DBOwnerPassword string `yaml:"db_owner_password" validate:"required"`

	ReplicationUser         string `yaml:"repl_user"`
	ReplicationUserPassword string `yaml:"repl_user_password"`

	Host              string `yaml:"host" validate:"required"`
	HostViaSubscriber string `yaml:"host_via_subscriber"`

	Port              string `yaml:"port" validate:"required"`
	PortViaSubscriber string `yaml:"port_via_subscriber"`

	SystemIdentifier string `yaml:"system_identifier" validate:"required"`
}

func (c ConnConfig) GetHostInfo() HostInfo {
	return HostInfo{
		Host:              c.Host,
		HostViaSubscriber: c.HostViaSubscriber,

		Port:              c.Port,
		PortViaSubscriber: c.PortViaSubscriber,

		SystemIdentifier: c.SystemIdentifier,
	}
}

func (c ConnConfig) SuperUserInfo() UserInfo {
	return UserInfo{
		User:     c.SuperUser,
		Password: c.SuperUserPassword,

		hi: c.GetHostInfo(),
	}
}

func (c ConnConfig) DBOwnerInfo() UserInfo {
	return UserInfo{
		User:     c.DBOwner,
		Password: c.DBOwnerPassword,

		hi: c.GetHostInfo(),
	}
}

func (c ConnConfig) ReplicationUserInfo() UserInfo {
	return UserInfo{
		User:     c.ReplicationUser,
		Password: c.ReplicationUserPassword,

		hi: c.GetHostInfo(),
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

	userInfo UserInfo
	dbName   string
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

func (c *Conn) GetSystemIdentifier(ctx context.Context) (string, error) {
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
	identifierStr, err := c.GetSystemIdentifier(ctx)
	if err != nil {
		return err
	}

	if c.userInfo.hi.SystemIdentifier != identifierStr {
		return SystemIdentifierError{
			Expected: c.userInfo.hi.SystemIdentifier,
			Got:      identifierStr,
		}
	}

	return nil
}

func ConnectWithVerify(ctx context.Context, ui UserInfo, dbName string) (*Conn, error) {
	fconn, err := Connect(ctx, ui, dbName)
	if err != nil {
		return nil, err
	}

	if err := fconn.VerifySystemIdentifier(ctx); err != nil {
		defer fconn.Close(ctx)

		return nil, fmt.Errorf("flare: verifying the identity: %w", err)
	}

	return fconn, nil
}

func Connect(ctx context.Context, ui UserInfo, dbName string) (*Conn, error) {
	conn, err := pgx.Connect(ctx, ui.DSNURI(dbName))
	if err != nil {
		return nil, err
	}

	return &Conn{
		Conn: conn,

		userInfo: ui,
		dbName:   dbName,
	}, nil
}

func GetCurrentLSN(ctx context.Context, conn *Conn) (string, error) {
	var currentLSN string
	if err := conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&currentLSN); err != nil {
		return "", fmt.Errorf("scanning pg_current_wal_lsn: %w", err)
	}

	return currentLSN, nil
}

func CheckWhetherReplayLSNIsAdvanced(ctx context.Context, conn *Conn, currentLSN string) (string, bool, error) {
	var (
		replayLSN string
		advanced  bool
	)

	if err := conn.QueryRow(
		ctx,
		`SELECT replay_lsn::text, replay_lsn >= $1::pg_lsn FROM pg_stat_replication;`,
		currentLSN,
	).Scan(&replayLSN, &advanced); err != nil {
		return "", false, fmt.Errorf("scanning received_lsn: %w", err)
	}

	return replayLSN, advanced, nil
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

type DatabaseConn struct {
	DatabaseName    string
	PID             string
	UserName        zeronull.Text
	ApplicationName string
	ClientAddr      zeronull.Text
	BackendStart    time.Time

	WaitEvent     zeronull.Text
	WaitEventType zeronull.Text

	State zeronull.Text
}

func ListConnectionByDatabase(ctx context.Context, conn *Conn, dbName string) ([]DatabaseConn, error) {
	rows, err := conn.Query(ctx, `
		SELECT datname, pid::text, usename, application_name, client_addr::text, backend_start, wait_event, wait_event_type, state
		FROM pg_stat_activity
		WHERE datname = $1
		ORDER BY backend_start DESC
		;`, dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("querying the database conns: %w", err)
	}

	var dconns []DatabaseConn

	for rows.Next() {
		var dc DatabaseConn
		if err := rows.Scan(
			&dc.DatabaseName,
			&dc.PID,

			&dc.UserName,

			&dc.ApplicationName,
			&dc.ClientAddr,
			&dc.BackendStart,

			&dc.WaitEvent,
			&dc.WaitEventType,

			&dc.State,
		); err != nil {
			return nil, fmt.Errorf("scanning the database conn: %w", err)
		}

		dconns = append(dconns, dc)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning the database conns: %w", err)
	}

	return dconns, nil
}

type ReplicationSlot struct {
	SlotName          string
	Plugin            string
	SlotType          string
	Database          string
	Temporary         string
	Active            string
	ConfirmedFlushLSN zeronull.Text
}

type ReplicationStat struct {
	PID             string
	UserName        string
	ApplicationName zeronull.Text
	ClientAddr      zeronull.Text
	BackendStart    time.Time
	State           zeronull.Text

	SentLSN   zeronull.Text
	ReplayLSN zeronull.Text
}

func ListReplicationStatsBySubscription(ctx context.Context, conn *Conn, subName string) ([]ReplicationStat, error) {
	rows, err := conn.Query(ctx, `
SELECT
	  pid::text
	, usename
	, application_name
	, client_addr::text
	, backend_start
	, state::text
	, sent_lsn::text
	, replay_lsn::text
FROM pg_stat_replication
WHERE application_name = $1
ORDER BY pid
;
		`, subName,
	)
	if err != nil {
		return nil, fmt.Errorf("querying the replication stats: %w", err)
	}

	var stats []ReplicationStat

	for rows.Next() {
		var sl ReplicationStat
		if err := rows.Scan(
			&sl.PID,
			&sl.UserName,
			&sl.ApplicationName,
			&sl.ClientAddr,
			&sl.BackendStart,
			&sl.State,
			&sl.SentLSN,
			&sl.ReplayLSN,
		); err != nil {
			return nil, fmt.Errorf("scanning the stat: %w", err)
		}

		stats = append(stats, sl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning the stats: %w", err)
	}

	return stats, nil
}

func ListReplicationSlotsByDatabase(ctx context.Context, conn *Conn, dbName string) ([]ReplicationSlot, error) {
	rows, err := conn.Query(ctx, `
SELECT slot_name, plugin, slot_type, database, temporary::text, active::text, confirmed_flush_lsn::text
FROM pg_replication_slots
WHERE database = $1
ORDER BY slot_name
;
		`, dbName,
	)
	if err != nil {
		return nil, fmt.Errorf("querying the database conns: %w", err)
	}

	var slots []ReplicationSlot

	for rows.Next() {
		var sl ReplicationSlot
		if err := rows.Scan(
			&sl.SlotName,
			&sl.Plugin,
			&sl.SlotType,
			&sl.Database,
			&sl.Temporary,
			&sl.Active,
			&sl.ConfirmedFlushLSN,
		); err != nil {
			return nil, fmt.Errorf("scanning the slot: %w", err)
		}

		slots = append(slots, sl)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning the slots: %w", err)
	}

	return slots, nil
}

type SubscriptionStat struct {
	SubID       string
	SubName     string
	PID         zeronull.Text
	ReceivedLSN zeronull.Text

	LastMsgSendTime    zeronull.Timestamp
	LastMsgReceiptTime zeronull.Timestamp

	LatestEndLSN  zeronull.Text
	LatestEndTime zeronull.Timestamp
}

func ListSubscriptionStatByName(ctx context.Context, conn *Conn, subName string) ([]SubscriptionStat, error) {
	rows, err := conn.Query(ctx, `
SELECT subid::text, subname, pid::text, received_lsn::text, last_msg_send_time, last_msg_receipt_time, latest_end_lsn::text, latest_end_time
FROM pg_stat_subscription
WHERE subname = $1
ORDER BY subid
;
	`, subName)
	if err != nil {
		return nil, fmt.Errorf("querying the subscription conns: %w", err)
	}

	var stats []SubscriptionStat

	for rows.Next() {
		var stat SubscriptionStat
		if err := rows.Scan(
			&stat.SubID,
			&stat.SubName,
			&stat.PID,
			&stat.ReceivedLSN,
			&stat.LastMsgSendTime,
			&stat.LastMsgReceiptTime,
			&stat.LatestEndLSN,
			&stat.LatestEndTime,
		); err != nil {
			return nil, fmt.Errorf("scanning the subscritpion stat: %w", err)
		}

		stats = append(stats, stat)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning the stats: %w", err)
	}

	return stats, nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
