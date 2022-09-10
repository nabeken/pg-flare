package flare

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPGDump(t *testing.T) {

	t.Run("Wrong Password", func(t *testing.T) {
		_, err := PGDump(
			PSQLArgs{
				User: "postgres",
				Host: "localhost",
				Port: "5430",
				Pass: "________",
			},
			"postgres",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "password authentication failed for user \"postgres\"")
	})

	t.Run("Correct Password", func(t *testing.T) {
		out, err := PGDump(
			PSQLArgs{
				User: "postgres",
				Host: "localhost",
				Port: "5430",
				Pass: "postgres",
			},
			"postgres",
		)
		require.NoError(t, err)
		require.Contains(t, out, "-- PostgreSQL database dump")
	})
}

func TestPGDumpAll(t *testing.T) {
	t.Run("Wrong Password", func(t *testing.T) {
		_, err := PGDumpAll(
			PSQLArgs{
				User: "postgres",
				Host: "localhost",
				Port: "5430",
				Pass: "________",
				Args: []string{"--roles-only"},
			},
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "password authentication failed for user \"postgres\"")
	})

	t.Run("Correct Password", func(t *testing.T) {
		out, err := PGDumpAll(
			PSQLArgs{
				User: "postgres",
				Host: "localhost",
				Port: "5430",
				Pass: "postgres",
				Args: []string{"--roles-only"},
			},
		)
		require.NoError(t, err)
		require.Contains(t, out, "-- PostgreSQL database cluster dump")
	})
}
