package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

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

	//rootCmd.AddCommand(buildAttackCmd())
	//rootCmd.AddCommand(buildAttackDBCmd())
	//rootCmd.AddCommand(buildCreatePublicationCmd())
	//rootCmd.AddCommand(buildCreateSubscriptionCmd())

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

//func buildCreateSubscriptionCmd() *cobra.Command {
//	var pubDSN, subDSN string
//
//	cmd := &cobra.Command{
//		Use:   "create_subscription [SUBNAME]",
//		Short: "Create a subscription in the given database in the DSN",
//		Run: func(cmd *cobra.Command, args []string) {
//			if len(args) != 1 {
//				cmd.PrintErr("please specify a subscription name\n\n")
//				cmd.Usage()
//				os.Exit(1)
//			}
//
//			subName := args[0]
//
//			pubConn := flare.MustNewConnConfig(pubDSN)
//			pubQuery := pubConn.MustQuery()
//			pubName := pubQuery.Get("x-publication")
//
//			pubConnInfo, err := pubConn.StdConnInfo()
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			log.Print(pubConnInfo)
//
//			subQuery := flare.CreateSubscriptionQuery(subName, pubConnInfo, pubName)
//
//			log.Print("Creating a subscription...")
//
//			suc := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(subDSN)}
//			db, err := suc.Open()
//
//			defer db.Close()
//
//			if err := db.Ping(); err != nil {
//				log.Fatal(err)
//			}
//
//			if _, err = db.Exec(subQuery); err != nil {
//				log.Fatal(err)
//			}
//
//			log.Print("The subscription has been created")
//		},
//	}
//
//	cmd.Flags().StringVar(
//		&subDSN,
//		"sub-super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/DBNAME",
//		"Subscriber Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("sub-super-user-dsn")
//
//	cmd.Flags().StringVar(
//		&pubDSN,
//		"pub-super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/DBNAME?x-publication=PUBNAME",
//		"Publisher Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("pub-super-user-dsn")
//
//	return cmd
//}
//
//func buildCreatePublicationCmd() *cobra.Command {
//	var dsn string
//	var replicaIdentityFullTables []string
//
//	cmd := &cobra.Command{
//		Use:   "create_publication [PUBNAME]",
//		Short: "Create a publication in the given database in the DSN",
//		Run: func(cmd *cobra.Command, args []string) {
//			if len(args) != 1 {
//				cmd.PrintErr("please specify a publication name\n\n")
//				cmd.Usage()
//				os.Exit(1)
//			}
//
//			pubName := args[0]
//			suc := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(dsn)}
//
//			log.Print("Creating a publisher in the source...")
//
//			db, err := suc.Open()
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			defer db.Close()
//
//			if err := db.Ping(); err != nil {
//				log.Fatal(err)
//			}
//
//			for _, tbl := range replicaIdentityFullTables {
//				log.Printf("Setting REPLICA IDENTITY FULL for '%s'", tbl)
//
//				if _, err = db.Exec(flare.AlterTableReplicaIdentityFull(tbl)); err != nil {
//					log.Fatal(err)
//				}
//			}
//
//			if _, err = db.Exec(flare.CreatePublicationQuery(pubName)); err != nil {
//				log.Fatal(err)
//			}
//
//			log.Print("Publisher in the source has been created")
//		},
//	}
//
//	cmd.Flags().StringArrayVar(
//		&replicaIdentityFullTables,
//		"replica-identity-full",
//		[]string{},
//		"Table to set REPLICA IDENTITY to FULL",
//	)
//
//	cmd.Flags().StringVar(
//		&dsn,
//		"super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/DBNAME",
//		"Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("super-user-dsn")
//
//	return cmd
//}
//

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

//
//func buildAttackCmd() *cobra.Command {
//	var dsn string
//
//	cmd := &cobra.Command{
//		Use:   "attack",
//		Short: "Generate write traffic against `flare_test` table for testing",
//		Run: func(cmd *cobra.Command, args []string) {
//			db, err := flare.Open(dsn)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			gen := flare.NewTrafficGenerator(db)
//
//			log.Print("Begin to attack the database...")
//
//			if err := gen.Attack(context.Background()); err != nil {
//				log.Println(err)
//			}
//
//			log.Print("Finished attacking the database...")
//		},
//	}
//
//	cmd.Flags().StringVar(
//		&dsn,
//		"dsn",
//		"postgres://app:app@localhost:5432/flare_test?sslmode=disable",
//		"Data Source Name (must not be a super user)",
//	)
//
//	return cmd
//}
//
//func buildAttackDBCmd() *cobra.Command {
//	var dsn, dbUser string
//	var dropDBBefore bool
//
//	cmd := &cobra.Command{
//		Use:   "create_attack_db",
//		Short: "Create database for testing",
//		Run: func(cmd *cobra.Command, args []string) {
//			if err := flare.CreateTestTable(
//				flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(dsn)},
//				dbUser,
//				dropDBBefore,
//			); err != nil {
//				log.Fatal(err)
//			}
//		},
//	}
//
//	cmd.Flags().StringVar(
//		&dsn,
//		"super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
//		"Super User Data Source Name",
//	)
//
//	cmd.Flags().StringVar(
//		&dbUser,
//		"dbuser",
//		"app",
//		"Database User (must not be a super user)",
//	)
//
//	cmd.Flags().BoolVar(
//		&dropDBBefore,
//		"drop-db-before",
//		false,
//		"Drop the database before creating it if exists",
//	)
//
//	return cmd
//}
