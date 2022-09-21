package main

import (
	"log"

	"github.com/spf13/cobra"
)

func main() {
	if err := realmain(); err != nil {
		log.Fatal(err)
	}
}

func realmain() error {
	rootCmd := &cobra.Command{
		Use:   "flare",
		Short: "flare is a command-line tool to help database migration with the logical replication",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErr("please specify a subcommand\n")
		},
	}

	//rootCmd.AddCommand(buildAttackCmd())
	//rootCmd.AddCommand(buildAttackDBCmd())
	//rootCmd.AddCommand(buildDumpRolesCmd())
	//rootCmd.AddCommand(buildReplicateRolesCmd())
	//rootCmd.AddCommand(buildReplicateSchemaCmd())
	//rootCmd.AddCommand(buildCreatePublicationCmd())
	//rootCmd.AddCommand(buildCreateSubscriptionCmd())

	return rootCmd.Execute()
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
//func buildReplicateSchemaCmd() *cobra.Command {
//	var srcDSN, dstDSN string
//
//	cmd := &cobra.Command{
//		Use:   "replicate_schema [DBNAME]",
//		Short: "Replicate schema",
//		Run: func(cmd *cobra.Command, args []string) {
//			if len(args) != 1 {
//				cmd.PrintErr("please specify a database name\n\n")
//				cmd.Usage()
//				os.Exit(1)
//			}
//
//			dbName := args[0]
//
//			srcSUC := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(srcDSN)}
//			dstSUC := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(dstDSN)}
//
//			log.Print("Reading the schema from the source...")
//
//			schema, err := flare.DumpSchema(srcSUC, dbName)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			log.Print("Copying the schema to the destination...")
//
//			psqlArgs := dstSUC.ConnConfig.MustPSQLArgs()
//			result, resultErr, err := flare.PSQL(psqlArgs, "postgres", strings.NewReader(schema))
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			fmt.Print(result)
//			fmt.Print(resultErr)
//
//			log.Print("Finished copying the schema to the destination")
//		},
//	}
//
//	cmd.Flags().StringVar(
//		&srcDSN,
//		"src-super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/SRC_DBNAME",
//		"Source Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("src-super-user-dsn")
//
//	cmd.Flags().StringVar(
//		&dstDSN,
//		"dst-super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/DST_DBNAME",
//		"Destination Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("dst-super-user-dsn")
//
//	return cmd
//}
//
//func buildReplicateRolesCmd() *cobra.Command {
//	var srcDSN, dstDSN string
//
//	cmd := &cobra.Command{
//		Use:   "replicate_roles",
//		Short: "Replicate roles",
//		Run: func(cmd *cobra.Command, args []string) {
//			srcSUC := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(srcDSN)}
//			dstSUC := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(dstDSN)}
//
//			log.Print("Reading the roles from the source...")
//
//			roles, err := flare.DumpRoles(srcSUC)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			log.Print("Copying the roles to the destination...")
//
//			psqlArgs := dstSUC.ConnConfig.MustPSQLArgs()
//			result, resultErr, err := flare.PSQL(psqlArgs, "postgres", strings.NewReader(roles))
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			fmt.Print(result)
//			fmt.Print(resultErr)
//
//			log.Print("Finished copying the roles to the destination")
//		},
//	}
//
//	cmd.Flags().StringVar(
//		&srcDSN,
//		"src-super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432",
//		"Source Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("src-super-user-dsn")
//
//	cmd.Flags().StringVar(
//		&dstDSN,
//		"dst-super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432",
//		"Destination Super User Data Source Name",
//	)
//	cmd.MarkFlagRequired("dst-super-user-dsn")
//
//	return cmd
//}
//
//func buildDumpRolesCmd() *cobra.Command {
//	var dsn string
//
//	cmd := &cobra.Command{
//		Use:   "dump_roles",
//		Short: "Dump roles",
//		Run: func(cmd *cobra.Command, args []string) {
//			suc := flare.SuperUserConfig{ConnConfig: flare.MustNewConnConfig(dsn)}
//
//			roles, err := flare.DumpRoles(suc)
//			if err != nil {
//				log.Fatal(err)
//			}
//
//			fmt.Print(roles)
//		},
//	}
//
//	cmd.Flags().StringVar(
//		&dsn,
//		"super-user-dsn",
//		"postgres://postgres:postgres@localhost:5432/",
//		"Super User Data Source Name",
//	)
//
//	return cmd
//}
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
