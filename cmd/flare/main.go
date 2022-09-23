package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

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
		cfg.Hosts.Publisher.Conn,
		"postgres",
	)
	if err != nil {
		return fmt.Errorf("verifying the publisher: %w", err)
	}
	defer pconn.Close(ctx)

	sconn, err := flare.ConnectWithVerify(
		ctx,
		cfg.Hosts.Subscriber.Conn,
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

			subQuery := flare.CreateSubscriptionQuery(
				subName,
				cfg.Hosts.Publisher.Conn.DSNURIForSubscriber(subCfg.DBName),
				subCfg.PubName,
			)

			log.Print("Creating a subscription...")

			conn, err := flare.Connect(ctx, cfg.Hosts.Subscriber.Conn, subCfg.DBName)
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

			log.Print("Creating a publication in the publisher...")

			conn, err := flare.Connect(ctx, cfg.Hosts.Publisher.Conn, dbName)
			if err != nil {
				cmd.PrintErrf("Failed to connect to the publisher: %s\n", err.Error())
				os.Exit(1)
			}

			defer conn.Close(ctx)

			if err := conn.Ping(ctx); err != nil {
				log.Fatal(err)
			}

			for _, tbl := range pubCfg.ReplicaIdentityFullTables {
				log.Printf("Setting REPLICA IDENTITY FULL for '%s'", tbl)

				if _, err = conn.Exec(ctx, flare.AlterTableReplicaIdentityFull(tbl)); err != nil {
					log.Fatal(err)
				}
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

			schema, err := flare.DumpSchema(cfg.Hosts.Publisher.Conn, dbName)
			if err != nil {
				log.Fatal(err)
			}

			if onlyDump {
				fmt.Print(schema)
				log.Print("no replication to the subscriber was made as per request in the flag")
				os.Exit(0)
			}

			log.Print("Copying the schema to the subscriber...")

			psqlArgs := cfg.Hosts.Subscriber.Conn.PSQLArgs()
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

	return cmd
}

func buildReplicateRolesCmd(gflags *globalFlags) *cobra.Command {
	var onlyDump bool

	cmd := &cobra.Command{
		Use:   "replicate_roles",
		Short: "Replicate roles from the publisher to the subscriber",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.TODO()
			cfg := readConfigFileAndVerifyOrExit(ctx, cmd, gflags.configFile)

			log.Print("Reading the roles from the publisher...")

			roles, err := flare.DumpRoles(cfg.Hosts.Publisher.Conn)
			if err != nil {
				log.Fatal(err)
			}

			if onlyDump {
				fmt.Print(roles)
				log.Print("no replication to the subscriber was made as per request in the flag")
				os.Exit(0)
			}

			log.Print("Copying the roles to the subscriber...")

			psqlArgs := cfg.Hosts.Subscriber.Conn.PSQLArgs()
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

			cfg.Hosts.Publisher.Conn.User = dbUser
			cfg.Hosts.Publisher.Conn.Password = password

			pool, err := pgxpool.Connect(ctx, cfg.Hosts.Publisher.Conn.DSNURI("flare_test"))
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
				cfg.Hosts.Publisher.Conn,
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
