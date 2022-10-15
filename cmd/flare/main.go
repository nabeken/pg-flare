package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	flare "github.com/nabeken/pg-flare"
	"github.com/spf13/cobra"
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
		cmd.PrintErrf("Failed to verify the connection: %s\n", err.Error())
		os.Exit(1)
	}

	return cfg
}

func readConfigFileOrExit(cmd *cobra.Command, fn string) flare.Config {
	cfg, err := parseConfigFile(fn)
	if err != nil {
		cmd.PrintErrf("Failed to parse the configuration: %s\n", err.Error())
		os.Exit(1)
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
				cmd.PrintErrf("Subscription '%s' is not found in the config\n", subName)
				os.Exit(1)
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
				cmd.PrintErrf("Failed to connect to the subscriber: %s\n", err.Error())
				os.Exit(1)
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
				cmd.PrintErrf("Database '%s' is not found in the config\n", dbName)
				os.Exit(1)
			}

			for _, tbl := range pubCfg.ReplicaIdentityFullTables {
				func() {
					dboconn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.DBOwnerInfo(), dbName)
					if err != nil {
						cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
						os.Exit(1)
					}

					defer dboconn.Close(ctx)

					log.Printf("Setting REPLICA IDENTITY FULL for '%s'", tbl)

					if _, err = dboconn.Exec(ctx, flare.AlterTableReplicaIdentityFull(tbl)); err != nil {
						log.Fatalf("Failed to set the replica identity full: %s", err.Error())
					}
				}()
			}

			log.Print("Creating a publication in the publisher...")

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), dbName)
			if err != nil {
				cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
				os.Exit(1)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			if _, err = conn.Exec(ctx, flare.CreatePublicationQuery(pubCfg.PubName)); err != nil {
				log.Fatal(err)
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

	return cmd
}

func buildAttackCmd(gflags *globalFlags) *cobra.Command {
	var dbUser, password string

	cmd := &cobra.Command{
		Use:   "attack",
		Short: "Generate write traffic against `flare_test` table in the publisher for testing",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			pubConnUserInfo := flare.UserInfo{
				User:     dbUser,
				Password: password,
			}.WithHostInfo(cfg.Hosts.Publisher.Conn.GetHostInfo())

			pool, err := pgxpool.Connect(ctx, pubConnUserInfo.DSNURI("flare_test"))
			if err != nil {
				cmd.PrintErrf("Failed to connect to flare_test database: %s\n", err.Error())
				os.Exit(1)
			}

			gen := flare.NewTrafficGenerator(pool)

			log.Print("Begin to attack the database...")

			if err := gen.Attack(ctx); err != nil {
				log.Println(err)
			}

			log.Print("Finished attacking the database...")
		},
	}

	cmd.Flags().StringVar(
		&dbUser,
		"dbuser",
		"app",
		"Data User (must not be a super user)",
	)
	cmd.Flags().StringVar(
		&password,
		"password",
		"app",
		"Data User Password",
	)

	return cmd
}

func buildCreateAttackDBCmd(gflags *globalFlags) *cobra.Command {
	var dropDBBefore bool
	var dbUser string

	cmd := &cobra.Command{
		Use:   "create_attack_db",
		Short: "Create database for testing",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			log.Print("Creating the `flare_test` database in the publisher for testing...")

			if err := flare.CreateTestTable(
				ctx,
				cfg.Hosts.Publisher.Conn.SuperUserInfo(),
				dbUser,
				dropDBBefore,
			); err != nil {
				log.Fatal(err)
			}
		},
	}

	cmd.Flags().StringVar(
		&dbUser,
		"dbuser",
		"app",
		"Database User (must not be a super user)",
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
	cmd := &cobra.Command{
		Use:   "pause_write",
		Short: "Pause write traffic by revoking and killing access to a given databas in the publisher",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.PrintErr("please specify a database name\n\n")
				cmd.Usage()
				os.Exit(1)
			}

			dbName := args[0]
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), dbName)
			if err != nil {
				cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
				os.Exit(1)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			log.Printf("Revoking the access against '%s' database...", dbName)

			if _, err = conn.Exec(ctx, flare.RevokeConnectionQuery(dbName)); err != nil {
				log.Fatal(err)
			}

			log.Printf("Database access against '%s' database has been revoked!", dbName)

			log.Printf("Killing the existing connections against '%s' database...", dbName)

			zeroConnTimes := 0

			// will retry until flare sees 3 times zero connections in a row
			for zeroConnTimes <= 3 {
				ret, err := conn.Exec(ctx, flare.KillConnectionQuery, dbName)
				if err != nil {
					log.Fatal(err)
				}

				if ret.RowsAffected() > 0 {
					// reset to zero to see whether there are still remaining connections again...
					zeroConnTimes = 0
				} else {
					zeroConnTimes++
				}

				time.Sleep(100 * time.Millisecond)
			}

			log.Printf("No connections against '%s' database are detected!", dbName)
		},
	}

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

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn.SuperUserInfo(), dbName)
			if err != nil {
				cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
				os.Exit(1)
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
				cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
				os.Exit(1)
			}

			defer pconn.Close(ctx)

			// list the installed extensions
			installedExts, err := flare.ListInstalledExtensions(ctx, pconn)
			if err != nil {
				cmd.PrintErrf("Failed to list the installed extensions: %s\n", err.Error())
				os.Exit(1)
			}

			subConnUserInfo := cfg.Hosts.Subscriber.Conn.SuperUserInfo()

			if useDBOwner {
				subConnUserInfo = cfg.Hosts.Subscriber.Conn.DBOwnerInfo()
			}

			sconn, err := flare.Connect(ctx, subConnUserInfo, dbName)
			if err != nil {
				cmd.PrintErrf("Failed to connect to the subscriber: %s\n", err.Error())
				os.Exit(1)
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
					cmd.PrintErrf(
						"Failed to install '%s' extension into the subscriber: %s\n", ext, err.Error(),
					)
					os.Exit(1)
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
	var superUser string
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

			log.Printf("Granting CREATE ON DATABASE '%s' to '%s' in the publisher...", dbName, superUser)

			pubConnUserInfo := cfg.Hosts.Publisher.Conn.SuperUserInfo()

			if useDBOwner {
				pubConnUserInfo = cfg.Hosts.Publisher.Conn.DBOwnerInfo()
			}

			conn, err := flare.Connect(ctx, pubConnUserInfo, dbName)
			if err != nil {
				cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
				os.Exit(1)
			}

			defer conn.Close(ctx)

			if _, err := conn.Exec(ctx, flare.GrantCreateQuery(dbName, superUser)); err != nil {
				log.Fatal(err)
			}
		},
	}

	cmd.Flags().StringVar(
		&superUser,
		"super-user",
		"postgres",
		"Specify the superuser to be granted",
	)

	cmd.Flags().BoolVar(
		&useDBOwner,
		"use-db-owner",
		false,
		"Use the db owner to grant",
	)

	return cmd
}
