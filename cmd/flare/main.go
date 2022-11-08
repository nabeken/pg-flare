package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	flare "github.com/nabeken/pg-flare"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := realmain(); err != nil {
		log.Fatal(err)
	}
}

type globalFlags struct {
	configFile string
}

func realmain() error {
	gflags := &globalFlags{}

	rootCmd := &cobra.Command{
		Use:   "flare",
		Short: "flare is a command-line tool to help database migration with the logical replication",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErr("please specify a subcommand\n")
		},
	}

	rootCmd.PersistentFlags().StringVar(
		&gflags.configFile,
		"config",
		"./flare.yml",
		"the configuration file",
	)

	rootCmd.AddCommand(buildVerifyConnectivity(gflags))

	rootCmd.AddCommand(buildReplicateRolesCmd(gflags))
	rootCmd.AddCommand(buildReplicateSchemaCmd(gflags))

	rootCmd.AddCommand(buildCreatePublicationCmd(gflags))
	rootCmd.AddCommand(buildCreateSubscriptionCmd(gflags))

	rootCmd.AddCommand(buildCreateAttackDBCmd(gflags))
	rootCmd.AddCommand(buildAttackCmd(gflags))

	rootCmd.AddCommand(buildPauseWriteCmd(gflags))
	rootCmd.AddCommand(buildResumeWriteCmd(gflags))

	rootCmd.AddCommand(buildInstallExtensionsCmd(gflags))

	rootCmd.AddCommand(buildGrantCreateCmd(gflags))
	rootCmd.AddCommand(buildGrantReplicationCmd(gflags))

	rootCmd.AddCommand(buildMonitor(gflags))

	rootCmd.AddCommand(buildDropPublicationCmd(gflags))
	rootCmd.AddCommand(buildDropSubscriptionCmd(gflags))

	rootCmd.AddCommand(buildExecCmd(gflags))

	rootCmd.AddCommand(buildCreateReplicationStatusTableCmd(gflags))
	rootCmd.AddCommand(buildResetReplicationStatusCmd(gflags))

	return rootCmd.Execute()
}

func buildVerifyConnectivity(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify_connectivity",
		Short: "Verify connectivity for a given configuration",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			cmd.Printf("The system identifier for the publisher and the subscriber is OK!\n")

			cmd.Printf("Publisher: %s\n", cfg.Hosts.Publisher.Conn.SystemIdentifier)
			cmd.Printf("Subscriber: %s\n", cfg.Hosts.Subscriber.Conn.SystemIdentifier)

			return
		},
	}

	return cmd
}

func verifyConnection(ctx context.Context, cmd *cobra.Command, cfg flare.Config) error {
	pconn, err := flare.ConnectWithVerify(
		ctx,
		cfg.Hosts.Publisher.Conn.SuperUserInfo(),
		"postgres",
	)
	if err != nil {
		return fmt.Errorf("verifying the publisher: %w", err)
	}
	defer pconn.Close(ctx)

	sconn, err := flare.ConnectWithVerify(
		ctx,
		cfg.Hosts.Subscriber.Conn.SuperUserInfo(),
		"postgres",
	)
	if err != nil {
		return fmt.Errorf("verifying the subscriber: %w", err)
	}
	defer sconn.Close(ctx)

	return nil
}

func readConfigFileAndVerifyOrExit(ctx context.Context, cmd *cobra.Command, fn string) flare.Config {
	cfg := readConfigFileOrExit(cmd, fn)

	if err := verifyConnection(ctx, cmd, cfg); err != nil {
		log.Fatalf("Failed to verify the connection: %s\n", err)
	}

	return cfg
}

func readConfigFileOrExit(cmd *cobra.Command, fn string) flare.Config {
	cfg, err := parseConfigFile(fn)
	if err != nil {
		log.Fatalf("Failed to parse the configuration: %s\n", err)
	}

	return cfg
}

func parseConfigFile(fn string) (flare.Config, error) {
	b, err := os.ReadFile(fn)
	if err != nil {
		return flare.Config{}, fmt.Errorf("reading '%s': %w", fn, err)
	}

	return flare.ParseConfig(b)
}

func buildCreateSubscriptionCmd(gflags *globalFlags) *cobra.Command {
	var useReplUser bool

	cmd := &cobra.Command{
		Use:   "create_subscription [SUBNAME]",
		Short: "Create a subscription in the subscriber",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a subscription name in the config\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			subName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			subCfg, ok := cfg.Subscriptions[subName]
			if !ok {
				log.Fatalf("Subscription '%s' is not found in the config\n", subName)
			}

			pubConnForSub := cfg.Hosts.Publisher.Conn.SuperUserInfo()

			if useReplUser {
				pubConnForSub = cfg.Hosts.Publisher.Conn.ReplicationUserInfo()
			}

			subQuery := flare.CreateSubscriptionQuery(
				subName,
				pubConnForSub.DSNURIForSubscriber(subCfg.DBName),
				subCfg.PubName,
			)

			log.Print("Creating a subscription...")

			conn, err := flare.Connect(ctx, cfg.Hosts.Subscriber.Conn.SuperUserInfo(), subCfg.DBName)
			if err != nil {
				log.Fatalf("Failed to connect to the subscriber: %s\n", err)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			if _, err = conn.Exec(ctx, subQuery); err != nil {
				log.Fatal(err)
			}

			log.Print("The subscription has been created")
		},
	}

	cmd.Flags().BoolVar(
		&useReplUser,
		"use-repl-user",
		false,
		"Use the replication user to connect to the publisher",
	)

	return cmd
}

func buildCreatePublicationCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create_publication [DBNAME]",
		Short: "Create a publication in the given database in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			pubCfg, ok := cfg.Publications[dbName]
			if !ok {
				log.Fatalf("Database '%s' is not found in the config\n", dbName)
			}

			for _, tbl := range pubCfg.ReplicaIdentityFullTables {
				func() {
					dboconn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.DBOwnerInfo(), dbName)
					if err != nil {
						log.Fatalf("Failed to connect to the publisher: %s\n", err)
					}

					defer dboconn.Close(ctx)

					log.Printf("Setting REPLICA IDENTITY FULL for '%s'", tbl)

					if _, err = dboconn.Exec(ctx, flare.AlterTableReplicaIdentityFull(tbl)); err != nil {
						log.Fatalf("Failed to set the replica identity full: %s", err)
					}
				}()
			}

			log.Print("Creating a publication in the publisher...")

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			if _, err = conn.Exec(ctx, flare.CreatePublicationQuery(pubCfg.PubName)); err != nil {
				log.Fatalf("Failed to create a publication: %s", err)
			}

			log.Print("Publisher in the source has been created")
		},
	}

	return cmd
}

func buildReplicateSchemaCmd(gflags *globalFlags) *cobra.Command {
	var onlyDump bool
	var useDBOwner bool

	cmd := &cobra.Command{
		Use:   "replicate_schema [DBNAME]",
		Short: "Replicate schema",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			log.Printf("Reading the schema of '%s' from the publisher...", dbName)

			pubConnUserInfo := cfg.Hosts.Publisher.Conn.SuperUserInfo()
			if useDBOwner {
				pubConnUserInfo = cfg.Hosts.Publisher.Conn.DBOwnerInfo()
			}

			schema, err := flare.DumpSchema(pubConnUserInfo, dbName)
			if err != nil {
				log.Fatal(err)
			}

			if onlyDump {
				fmt.Print(schema)
				log.Print("no replication to the subscriber was made as per request in the flag")
				os.Exit(0)
			}

			log.Print("Copying the schema to the subscriber...")

			psqlArgs := cfg.Hosts.Subscriber.Conn.SuperUserInfo().PSQLArgs()
			result, resultErr, err := flare.PSQL(psqlArgs, "postgres", strings.NewReader(schema))
			if err != nil {
				log.Fatal(err)
			}

			fmt.Print(result)
			fmt.Print(resultErr)

			log.Print("Finished copying the schema to the subscriber")
		},
	}

	cmd.Flags().BoolVar(
		&onlyDump,
		"only-dump",
		false,
		"Only dump the schema instead of replicating to the subscriber",
	)

	cmd.Flags().BoolVar(
		&useDBOwner,
		"use-db-owner",
		false,
		"Use the db owner to dump the schema",
	)

	return cmd
}

func buildReplicateRolesCmd(gflags *globalFlags) *cobra.Command {
	var onlyDump bool
	var noPasswords bool
	var stripRoleOptionsForRDS bool

	cmd := &cobra.Command{
		Use:   "replicate_roles",
		Short: "Replicate roles from the publisher to the subscriber",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			log.Print("Reading the roles from the publisher...")

			roles, err := flare.DumpRoles(cfg.Hosts.Publisher.Conn.SuperUserInfo(), noPasswords)
			if err != nil {
				log.Fatal(err)
			}

			if stripRoleOptionsForRDS {
				roles, err = flare.StripRoleOptionsForRDS(roles)
				if err != nil {
					log.Fatalf("Failed to strip options: %s", err)
				}
			}

			if onlyDump {
				fmt.Print(roles)
				log.Print("no replication to the subscriber was made as per request in the flag")
				os.Exit(0)
			}

			log.Print("Copying the roles to the subscriber...")

			psqlArgs := cfg.Hosts.Subscriber.Conn.SuperUserInfo().PSQLArgs()
			result, resultErr, err := flare.PSQL(psqlArgs, "postgres", strings.NewReader(roles))
			if err != nil {
				log.Fatal(err)
			}

			fmt.Print(result)
			fmt.Print(resultErr)

			log.Print("Finished copying the roles to the subscriber")
		},
	}

	cmd.Flags().BoolVar(
		&onlyDump,
		"only-dump",
		false,
		"Only dump the roles instead of replicating to the subscriber",
	)

	cmd.Flags().BoolVar(
		&noPasswords,
		"no-passwords",
		false,
		"Do not dump the passwords",
	)

	cmd.Flags().BoolVar(
		&stripRoleOptionsForRDS,
		"strip-options-for-rds",
		false,
		"Strip role options for RDS",
	)

	return cmd
}

func buildAttackCmd(gflags *globalFlags) *cobra.Command {
	var dsn, name string

	cmd := &cobra.Command{
		Use:   "attack",
		Short: "Generate write traffic against `flare_test` table in the publisher for testing",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
			defer stop()

			pool, err := pgxpool.Connect(ctx, dsn)
			if err != nil {
				log.Fatalf("Failed to connect to flare_test database: %s\n", err)
			}

			gen := flare.NewTrafficGenerator(pool, name)

			eg, ctx := errgroup.WithContext(ctx)

			eg.Go(func() error {
				log.Print("Start sending heartbeat...")
				return gen.KeepAlive(ctx)
			})

			eg.Go(func() error {
				log.Print("Begin to attack the database...")
				return gen.Attack(ctx)
			})

			log.Print("Waiting for interrupt...")

			err = eg.Wait()

			log.Printf("Finished attacking the database: %s", err)
		},
	}

	cmd.Flags().StringVar(
		&dsn,
		"dsn",
		"postgres://postgres:postgres@127.0.0.1:5432/flare_test",
		"Data Source Name",
	)
	cmd.Flags().StringVar(
		&name,
		"name",
		"flare",
		"Worker's ID",
	)

	return cmd
}

func buildCreateAttackDBCmd(gflags *globalFlags) *cobra.Command {
	var dropDBBefore bool
	var baseDSN string
	var appUser string

	cmd := &cobra.Command{
		Use:   "create_attack_db",
		Short: "Create database for testing",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()

			log.Print("Creating the `flare_test` database...")

			if err := flare.CreateTestTable(
				ctx,
				baseDSN,
				appUser,
				dropDBBefore,
			); err != nil {
				log.Fatal(err)
			}
		},
	}

	cmd.Flags().StringVar(
		&appUser,
		"app-user",
		"app",
		"Application User",
	)

	cmd.Flags().StringVar(
		&baseDSN,
		"base-dsn",
		"postgres://postgres:postgres@127.0.0.1:5432",
		"Base DSN (without database name)",
	)

	cmd.Flags().BoolVar(
		&dropDBBefore,
		"drop-db-before",
		false,
		"Drop the database before creating it if exists",
	)

	return cmd
}

func buildPauseWriteCmd(gflags *globalFlags) *cobra.Command {
	var appUser string
	var allowedRepDuration string

	cmd := &cobra.Command{
		Use:   "pause_write [DBNAME] [SUBNAME]",
		Short: "Pause write traffic by revoking and killing access to a given databas in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				cmd.PrintErr("please specify a database and subscription name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			repDuration, err := time.ParseDuration(allowedRepDuration)
			if err != nil {
				log.Fatalf("Failed to parse allowedRepDuration: %s", err)
			}

			dbName := args[0]
			subName := args[1]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			// setup connections
			pdboconn := mustSetupConn(ctx, cfg.Hosts.Publisher.Conn.DBOwnerInfo(), dbName)
			defer pdboconn.Close(ctx)

			psuconn := mustSetupConn(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), "postgres")
			defer psuconn.Close(ctx)

			subdboconn := mustSetupConn(ctx, cfg.Hosts.Subscriber.Conn.DBOwnerInfo(), dbName)
			defer subdboconn.Close(ctx)

			log.Printf("Checking whether only one logical replication is working for '%s'...", dbName)

			// check the replication slots for the database
			// abort if there are multiple slots ongoing for the database, which indicates it's in the initial sync
			repSlots, err := flare.ListReplicationSlotsByDatabase(ctx, psuconn, dbName)
			if err != nil {
				log.Fatalf("Failed to list the replication slots for %s: %s", dbName, err)
			}

			if len(repSlots) > 1 {
				log.Fatalf("There are more than one replications are ongoing for '%s' where it should be only one in progress", dbName)
			}

			log.Printf("Confirmed there is only one replication is working for %s", dbName)

			// check whether the logical replication is working for 1 minute at least because if the logical replication has an issue, the process is being died repeadtly
			log.Printf("Checking whether the logical replication is working for subscription of '%s'...", subName)

			repStat := mustGetReplicationStatBySubscription(ctx, psuconn, subName)
			if string(repStat.ApplicationName) != subName {
				log.Fatalf("The replication doesn't sound for subscription of '%s'", subName)
			}

			repSince := time.Since(repStat.BackendStart)
			if repSince < repDuration {
				log.Fatalf("The replication doesn't seem to be stable because it just started %s ago. Please check error log.", repSince)
			}

			log.Printf("The logical replication is working for subscription of '%s' for %s", subName, repSince)

			log.Printf("Revoking the access against '%s' database from PUBLIC...", dbName)

			if _, err = pdboconn.Exec(ctx, flare.RevokeConnectionQuery(dbName)); err != nil {
				log.Fatal(err)
			}

			log.Printf("Database access against '%s' database has been revoked!", dbName)

			log.Printf("Killing the existing connections against '%s' database...", dbName)

			zeroConnTimes := 0

			// will retry until flare sees 3 times zero connections in a row
			for zeroConnTimes <= 3 {
				ret, err := psuconn.Exec(
					ctx,
					flare.KillConnectionQuery,
					appUser,
					dbName,
				)
				if err != nil {
					log.Fatalf("Failed to kill the connections: %", err)
				}

				if ret.RowsAffected() > 0 {
					log.Printf("%d connections got killed", ret.RowsAffected())

					// reset to zero to see whether there are still remaining connections again...
					zeroConnTimes = 0
				} else {
					zeroConnTimes++
				}

				time.Sleep(100 * time.Millisecond)
			}

			log.Printf("No connections against '%s' database are detected!", dbName)

			log.Printf("Checking the current replication stats again for the final confirmation...")
			repStat2 := mustGetReplicationStatBySubscription(ctx, psuconn, subName)
			if string(repStat2.ApplicationName) != subName {
				log.Fatalf("The replication doesn't sound for subscription of '%s'", subName)
			}

			log.Printf("Writing a probe record to %s...", dbName)
			repUUID := uuid.New().String()
			if err := flare.WriteReplicationStatus(
				ctx, pdboconn, cfg.Hosts.Publisher.Conn.SystemIdentifier, repUUID,
			); err != nil {
				log.Fatalf("Failed to write a probe record: %s", err)
			}

			for {
				log.Print("Checking whether the subscriber the latest write after the application traffic is suspended...")

				if err := flare.ReadReplicationStatus(
					ctx, subdboconn, cfg.Hosts.Publisher.Conn.SystemIdentifier, repUUID,
				); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						log.Print("The record hasn't arrived yet at the subscriber...")
						time.Sleep(100 * time.Millisecond)
						continue
					}

					log.Fatalf("Faild to read the replication status: %s", err)
				}

				log.Print("The record has arrived at the subscriber! It's time to switch!")
				break
			}
		},
	}

	cmd.Flags().StringVar(
		&appUser,
		"app-user",
		"postgres",
		"Specify an application to be paused",
	)
	cmd.Flags().StringVar(
		&allowedRepDuration,
		"allowed-rep-duration",
		"1m",
		"Specify how long we will wait to consider the replication is stable",
	)
	cmd.MarkFlagRequired("app-user")

	return cmd
}

func buildResumeWriteCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume_write",
		Short: "Resume write traffic by granting access to a given databas in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.DBOwnerInfo(), dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			log.Printf("Revoking the access against '%s' database...", dbName)

			if _, err = conn.Exec(ctx, flare.GrantConnectionQuery(dbName)); err != nil {
				log.Fatal(err)
			}

			log.Printf("Database access against '%s' database has been granted!!", dbName)
		},
	}

	return cmd
}

func buildInstallExtensionsCmd(gflags *globalFlags) *cobra.Command {
	var onlyShow bool
	var useDBOwner bool

	cmd := &cobra.Command{
		Use:   "install_extensions [DBNAME]",
		Short: "Install extensions in the publisher into the subscriber",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name in the config\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			pubConnUserInfo := cfg.Hosts.Publisher.Conn.SuperUserInfo()

			if useDBOwner {
				pubConnUserInfo = cfg.Hosts.Publisher.Conn.DBOwnerInfo()
			}

			pconn, err := flare.Connect(ctx, pubConnUserInfo, dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer pconn.Close(ctx)

			// list the installed extensions
			installedExts, err := flare.ListInstalledExtensions(ctx, pconn)
			if err != nil {
				log.Fatalf("Failed to list the installed extensions: %s\n", err)
			}

			subConnUserInfo := cfg.Hosts.Subscriber.Conn.SuperUserInfo()

			if useDBOwner {
				subConnUserInfo = cfg.Hosts.Subscriber.Conn.DBOwnerInfo()
			}

			sconn, err := flare.Connect(ctx, subConnUserInfo, dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the subscriber: %s\n", err)
			}

			defer sconn.Close(ctx)

			for _, ext := range installedExts {
				if onlyShow {
					log.Printf(
						"Extension '%s' is installed in the publisher's %s database. Do not install into the subscriber as per request.", ext, dbName,
					)
					continue
				}

				if _, err := sconn.Exec(ctx, flare.CreateExtensionQuery(ext)); err != nil {
					log.Fatalf(
						"Failed to install '%s' extension into the subscriber: %s\n", ext, err,
					)
				}

				log.Printf(
					"Extension '%s' has been installed into the subscriber's %s database", ext, dbName,
				)
			}
		},
	}

	cmd.Flags().BoolVar(
		&onlyShow,
		"only-show",
		false,
		"only show the installed extensions",
	)

	cmd.Flags().BoolVar(
		&useDBOwner,
		"use-db-owner",
		false,
		"Use the db owner to dump the schema",
	)

	return cmd
}

func buildGrantCreateCmd(gflags *globalFlags) *cobra.Command {
	var useDBOwner bool

	cmd := &cobra.Command{
		Use:   "grant_create [DBNAME]",
		Short: "Grant CREATE in the given database to super-user in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			superUser := cfg.Hosts.Publisher.Conn.SuperUserInfo().User

			log.Printf("Granting CREATE ON DATABASE '%s' to '%s' in the publisher...", dbName, superUser)

			pubConnUserInfo := cfg.Hosts.Publisher.Conn.SuperUserInfo()

			if useDBOwner {
				pubConnUserInfo = cfg.Hosts.Publisher.Conn.DBOwnerInfo()
			}

			conn, err := flare.Connect(ctx, pubConnUserInfo, dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer conn.Close(ctx)

			if _, err := conn.Exec(ctx, flare.GrantCreateQuery(dbName, superUser)); err != nil {
				log.Fatal(err)
			}
		},
	}

	cmd.Flags().BoolVar(
		&useDBOwner,
		"use-db-owner",
		false,
		"Use the db owner to grant",
	)

	return cmd
}

func buildGrantReplicationCmd(gflags *globalFlags) *cobra.Command {
	var useDBOwner bool

	cmd := &cobra.Command{
		Use:   "grant_replication [DBNAME]",
		Short: "Grant the replication user all privileges on a given database in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			replUser := cfg.Hosts.Publisher.Conn.ReplicationUserInfo().User

			log.Printf("Granting all privileges on '%s' to '%s' in the publisher...", dbName, replUser)

			pubConnUserInfo := cfg.Hosts.Publisher.Conn.SuperUserInfo()

			if useDBOwner {
				pubConnUserInfo = cfg.Hosts.Publisher.Conn.DBOwnerInfo()
			}

			conn, err := flare.Connect(ctx, pubConnUserInfo, dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer conn.Close(ctx)

			if _, err := conn.Exec(ctx, flare.GrantAllOnDatabaseQuery(dbName, replUser)); err != nil {
				log.Fatalf("Failed to grant on the database: %s", err)
			}

			if _, err := conn.Exec(ctx, flare.GrantAllOnAllTablesQuery(replUser)); err != nil {
				log.Fatalf("Failed to grant on all the tables: %s", err)
			}

			log.Printf("'%s' has been granted for '%s'!", replUser, dbName)
		},
	}

	cmd.Flags().BoolVar(
		&useDBOwner,
		"use-db-owner",
		false,
		"Use the db owner to grant",
	)

	return cmd
}
func sRenderSubscriptionStats(conn *flare.Conn, subName string) (string, error) {
	thdr := []string{
		"SubID", "Sub Name", "PID", "Received LSN", "Last Msg Send Time", "Last Msg Receipt Time", "Latest End LSN", "Latest End Time",
	}

	var row [][]string
	row = append(row, thdr)

	stats, err := flare.ListSubscriptionStatByName(context.Background(), conn, subName)
	if err != nil {
		return "", err
	}

	for _, stat := range stats {
		row = append(row, []string{
			stat.SubID,
			stat.SubName,

			string(stat.PID),

			string(stat.ReceivedLSN),

			time.Time(stat.LastMsgSendTime).String(),
			time.Time(stat.LastMsgReceiptTime).String(),

			string(stat.LatestEndLSN),
			time.Time(stat.LatestEndTime).String(),
		})
	}

	tbl, _ := pterm.DefaultTable.WithHasHeader().WithData(row).Srender()

	return tbl, nil
}

func sRenderReplicationStatsTable(conn *flare.Conn, subName string) (string, error) {
	thdr := []string{
		"PID", "User Name", "Application Name", "Client Addr", "Backend Start", "State", "Sent LSN", "Reply LSN",
	}

	var row [][]string
	row = append(row, thdr)

	stats, err := flare.ListReplicationStatsBySubscription(context.Background(), conn, subName)
	if err != nil {
		return "", err
	}

	for _, stat := range stats {
		row = append(row, []string{
			stat.PID,
			stat.UserName,
			string(stat.ApplicationName),
			string(stat.ClientAddr),
			stat.BackendStart.String(),
			string(stat.State),
			string(stat.SentLSN),
			string(stat.ReplayLSN),
		})
	}

	tbl, _ := pterm.DefaultTable.WithHasHeader().WithData(row).Srender()

	return tbl, nil
}

func sRenderReplicationSlotsTable(conn *flare.Conn, dbName string) (string, error) {
	thdr := []string{
		"Slot Name", "Plugin", "Slot Type", "Database", "Temporary", "Active", "Confirmed Flush LSN",
	}

	var row [][]string
	row = append(row, thdr)

	slots, err := flare.ListReplicationSlotsByDatabase(context.Background(), conn, dbName)
	if err != nil {
		return "", err
	}

	for _, slot := range slots {
		row = append(row, []string{
			slot.SlotName,
			slot.Plugin,
			slot.SlotType,
			slot.Database,
			slot.Temporary,
			slot.Active,
			string(slot.ConfirmedFlushLSN),
		})
	}

	tbl, _ := pterm.DefaultTable.WithHasHeader().WithData(row).Srender()

	return tbl, nil
}

func sRenderDatabaseConnsTable(conn *flare.Conn, dbName string) (string, error) {
	thdr := []string{
		"Database Name", "PID", "User Name", "ApplicationName", "Client Addr", "BackendStart", "WaitEvent", "WaitEventType", "State",
	}

	var row [][]string
	row = append(row, thdr)

	dconns, err := flare.ListConnectionByDatabase(context.Background(), conn, dbName)
	if err != nil {
		return "", err
	}

	for _, dconn := range dconns {
		row = append(row, []string{
			dconn.DatabaseName,
			dconn.PID,
			string(dconn.UserName),
			dconn.ApplicationName,
			string(dconn.ClientAddr),
			dconn.BackendStart.String(),
			string(dconn.WaitEvent),
			string(dconn.WaitEventType),
			string(dconn.State),
		})
	}

	tbl, _ := pterm.DefaultTable.WithHasHeader().WithData(row).Srender()

	return tbl, nil
}

func buildMonitor(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "monitor [DBNAME] [SUBNAME]",
		Short: "Monitor the replication for a given database",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 2 {
				cmd.PrintErr("please specify a database name and a subscription name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]
			subName := args[1]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			if _, ok := cfg.Publications[dbName]; !ok {
				log.Fatalf("Database '%s' is not found in the config\n", dbName)
			}
			if _, ok := cfg.Subscriptions[subName]; !ok {
				log.Fatalf("Subscription '%s' is not found in the config\n", subName)
			}

			pconn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s", err)
			}
			defer pconn.Close(ctx)

			sconn, err := flare.Connect(ctx, cfg.Hosts.Subscriber.Conn.SuperUserInfo(), dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the subscriber: %s", err)
			}
			defer sconn.Close(ctx)

			area, _ := pterm.DefaultArea.WithFullscreen().Start()

			for {
				content := fmt.Sprintf(
					"Time: %s\n\n", time.Now().Format("2006-01-02T15:04:05 -07:00:00"),
				)

				ptbl, err := sRenderDatabaseConnsTable(pconn, dbName)
				if err != nil {
					log.Fatalf("Failed to query the connections in the publisher: %s", err)
				}

				stbl, err := sRenderDatabaseConnsTable(sconn, dbName)
				if err != nil {
					log.Fatalf("Failed to query the connections in the subscriber: %s", err)
				}

				slots, err := sRenderReplicationSlotsTable(pconn, dbName)
				if err != nil {
					log.Fatalf("Failed to query the replication slots: %s", err)
				}

				repStats, err := sRenderReplicationStatsTable(pconn, subName)
				if err != nil {
					log.Fatalf("Failed to query the replication slots: %s", err)
				}

				stats, err := sRenderSubscriptionStats(sconn, subName)
				if err != nil {
					log.Fatalf("Failed to query the subscritpion stats: %s", err)
				}

				area.Update(
					fmt.Sprintf(
						"%s\nPublisher:\n%s\n\nSubscriber:\n%s\n\nReplication Slots:\n%s\n\nReplication Stats:\n%s\n\nSubscription Stats:\n%s",
						content, ptbl, stbl, slots, repStats, stats,
					),
				)

				time.Sleep(100 * time.Millisecond)
			}

			area.Stop()
		},
	}

	return cmd
}

func buildDropPublicationCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "drop_publication [DBNAME]",
		Short: "Drop a publication in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name in the config\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			pubCfg, ok := cfg.Publications[dbName]
			if !ok {
				log.Fatalf("Database '%s' is not found in the config\n", dbName)
			}

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			log.Print("Dropping a publication...")

			if _, err = conn.Exec(ctx, flare.DropPublicationQuery(pubCfg.PubName)); err != nil {
				log.Fatalf("Failed to drop the publication: %s", err)
			}

			log.Printf("The publication `%s` has been dropped", pubCfg.PubName)
		},
	}

	return cmd
}

func buildDropSubscriptionCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "drop_subscription [SUBNAME]",
		Short: "Drop a subscription in the subscriber",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a subscription name in the config\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			subName := args[0]

			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			subCfg, ok := cfg.Subscriptions[subName]
			if !ok {
				log.Fatalf("Subscription '%s' is not found in the config\n", subName)
			}

			conn, err := flare.Connect(ctx, cfg.Hosts.Subscriber.Conn.SuperUserInfo(), subCfg.DBName)
			if err != nil {
				log.Fatalf("Failed to connect to the subscriber: %s\n", err)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			log.Print("Dropping a subscription...")

			if _, err = conn.Exec(ctx, flare.DropSubscriptionQuery(subName)); err != nil {
				log.Fatalf("Failed to drop the subscritpion: %s", err)
			}

			log.Print("The subscription has been dropped")
		},
	}

	return cmd
}

func buildExecCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "exec",
		Short: "Execute a command with the connection configuration over envvars",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			if len(args) == 0 {
				cmd.PrintErr("please specify a command\n\n")
				os.Exit(1)
			}

			ecmdName := args[0]
			eargs := args[1:]

			ecmd := exec.Command(ecmdName, eargs...)
			ecmd.Env = append(os.Environ(),
				fmt.Sprintf("FLARE_CONNINFO_PUBLISHER_HOST=%s", cfg.Hosts.Publisher.Conn.Host),
				fmt.Sprintf("FLARE_CONNINFO_PUBLISHER_PORT=%s", cfg.Hosts.Publisher.Conn.Port),
				fmt.Sprintf("FLARE_CONNINFO_SUBSCRIBER_HOST=%s", cfg.Hosts.Subscriber.Conn.Host),
				fmt.Sprintf("FLARE_CONNINFO_SUBSCRIBER_PORT=%s", cfg.Hosts.Subscriber.Conn.Port),
			)

			ecmd.Stderr = os.Stderr
			ecmd.Stdout = os.Stdout

			if err := ecmd.Run(); err != nil {
				log.Fatalf("Failed to run the command: %s", err)
			}
		},
	}

	return cmd
}

func mustGetReplicationStatBySubscription(ctx context.Context, conn *flare.Conn, subName string) flare.ReplicationStat {
	stats, err := flare.ListReplicationStatsBySubscription(ctx, conn, subName)
	if err != nil {
		log.Fatalf("Failed to list subscription stats: %s", err)
	}

	if len(stats) > 1 {
		log.Fatal("There are multiple subscriptions... that sounds weird.")
	}

	if len(stats) == 0 {
		log.Fatalf("There is no ongoing replication for subscription of '%s'. Please check error log.", subName)
	}

	return stats[0]
}

func buildCreateReplicationStatusTableCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create_replication_status_table [DBNAME]",
		Short: "Create a table that manage a replication status",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			if len(args) == 0 {
				cmd.PrintErr("please specify a database\n\n")
				os.Exit(1)
			}

			dbName := args[0]

			dboconn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.DBOwnerInfo(), dbName)
			if err != nil {
				log.Fatalf("Failed to connect to the publisher: %s\n", err)
			}

			defer dboconn.Close(ctx)

			log.Print("Creating a table to maintain the replication status...")

			if err := flare.CreateFlareStatusTable(ctx, dboconn); err != nil {
				log.Fatalf("Failed to create flare_replication_status table in %s: %s", dbName, err)
			}

			log.Printf("flare_replication_status table has been created in '%s' database!", dbName)
		},
	}

	return cmd
}

func buildResetReplicationStatusCmd(gflags *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset_replication_status [DBNAME]",
		Short: "Reset the replication status",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			if len(args) == 0 {
				cmd.PrintErr("please specify a database\n\n")
				os.Exit(1)
			}

			dbName := args[0]

			pdboconn := mustSetupConn(ctx, cfg.Hosts.Publisher.Conn.DBOwnerInfo(), dbName)
			defer pdboconn.Close(ctx)

			sdboconn := mustSetupConn(ctx, cfg.Hosts.Subscriber.Conn.DBOwnerInfo(), dbName)
			defer sdboconn.Close(ctx)

			if err := flare.DeleteReplicationStatus(ctx, pdboconn, cfg.Hosts.Publisher.Conn.SystemIdentifier); err != nil {
				log.Fatalf("Failed to reset the replication status in the publisher for %s: %s", dbName, err)
			}

			if err := flare.DeleteReplicationStatus(ctx, sdboconn, cfg.Hosts.Publisher.Conn.SystemIdentifier); err != nil {
				log.Fatalf("Failed to reset the replication status in the subscriber for %s: %s", dbName, err)
			}

			log.Printf("The replication status has been reset for %s", dbName)
		},
	}

	return cmd
}

func mustSetupConn(ctx context.Context, ui flare.UserInfo, dbName string) *flare.Conn {
	conn, err := flare.Connect(ctx, ui, dbName)
	if err != nil {
		log.Fatalf("Failed to connect to the publisher: %s\n", err)
	}

	if err := conn.Ping(ctx); err != nil {
		log.Fatal("Failed to ping to the databaes: %s", err)
	}

	return conn
}
