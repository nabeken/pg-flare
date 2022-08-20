package main

import (
	"context"
	"fmt"
	"log"

	flare "github.com/nabeken/pg-flare"
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

	rootCmd.AddCommand(buildAttackCmd())
	rootCmd.AddCommand(buildAttackDBCmd())
	rootCmd.AddCommand(buildDumpRolesCmd())

	return rootCmd.Execute()
}

func buildDumpRolesCmd() *cobra.Command {
	var dsn string

	cmd := &cobra.Command{
		Use:   "dump_roles",
		Short: "Dump roles",
		Run: func(cmd *cobra.Command, args []string) {
			suc := flare.SuperUserConfig{ConnConfig: flare.NewConnConfig(dsn)}

			roles, err := flare.DumpRoles(suc)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Print(roles)
		},
	}

	cmd.Flags().StringVar(
		&dsn,
		"super-user-dsn",
		"postgres://postgres:postgres@localhost:5432/",
		"Super User Data Source Name",
	)

	return cmd
}

func buildAttackCmd() *cobra.Command {
	var dsn string

	cmd := &cobra.Command{
		Use:   "attack",
		Short: "Generate write traffic against `flare_test` table for testing",
		Run: func(cmd *cobra.Command, args []string) {
			db, err := flare.Open(dsn)
			if err != nil {
				log.Fatal(err)
			}

			gen := flare.NewTrafficGenerator(db)

			log.Print("Begin to attack the database...")

			if err := gen.Attack(context.Background()); err != nil {
				log.Println(err)
			}

			log.Print("Finished attacking the database...")
		},
	}

	cmd.Flags().StringVar(
		&dsn,
		"dsn",
		"postgres://app:app@localhost:5432/flare_test?sslmode=disable",
		"Data Source Name (must not be a super user)",
	)

	return cmd
}

func buildAttackDBCmd() *cobra.Command {
	var dsn, dbUser string
	var dropDBBefore bool

	cmd := &cobra.Command{
		Use:   "create_attack_db",
		Short: "Create database for testing",
		Run: func(cmd *cobra.Command, args []string) {
			if err := flare.CreateTestTable(
				flare.SuperUserConfig{ConnConfig: flare.NewConnConfig(dsn)},
				dbUser,
				dropDBBefore,
			); err != nil {
				log.Fatal(err)
			}
		},
	}

	cmd.Flags().StringVar(
		&dsn,
		"super-user-dsn",
		"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
		"Super User Data Source Name",
	)

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
