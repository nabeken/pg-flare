package main

import (
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

	attackCmd := &cobra.Command{
		Use:   "attack",
		Short: "Generate write traffic for testing",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErr("please specify a subcommand\n")
		},
	}

	rootCmd.AddCommand(attackCmd)

	rootCmd.AddCommand(buildAttackDBCmd())

	return rootCmd.Execute()
}

func buildAttackDBCmd() *cobra.Command {
	var dsn, dbOwner string
	var dropDBBefore bool

	attackDBCmd := &cobra.Command{
		Use:   "create_attack_db",
		Short: "Create database for testing",
		Run: func(cmd *cobra.Command, args []string) {
			if err := flare.CreateTestTable(
				flare.SuperUserConfig{ConnConfig: flare.NewConnConfig(dsn)},
				dbOwner,
				dropDBBefore,
			); err != nil {
				log.Fatal(err)
			}
		},
	}

	attackDBCmd.Flags().StringVar(
		&dsn,
		"super-user-dsn",
		"postgres://postgres:postgres@localhost:5432/postgressslmode=disable",
		"Super User Data Source Name",
	)

	attackDBCmd.Flags().StringVar(
		&dbOwner,
		"dbowner",
		"postgres",
		"Database Owner",
	)

	attackDBCmd.Flags().BoolVar(
		&dropDBBefore,
		"drop-db-before",
		false,
		"Drop the database before creating it if exists",
	)

	return attackDBCmd
}
