package flare

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPGDump(t *testing.T) {
	args := PSQLArgs{
		User: "postgres",
		Host: "localhost",
		Port: "5430",
	}

	t.Run("Wrong Password", func(t *testing.T) {
		_, err := PGDump(args, "postgres", "________")
		require.Error(t, err)
		require.Contains(t, err.Error(), "password authentication failed for user \"postgres\"")
	})

	t.Run("Correct Password", func(t *testing.T) {
		out, err := PGDump(args, "postgres", "postgres")
		require.NoError(t, err)
		require.Contains(t, out, "-- PostgreSQL database dump")
	})
}
