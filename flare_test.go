package flare

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	require := require.New(t)

	cfg := []byte(`
hosts:
  publisher:
    conn:
      user: postgres1
      password: 'password1'
      host: publisher
      port: '5430'
  subscriber:
    conn:
      user: postgres2
      password: 'password2'
      host: subscriber
      port: '5431'
      system_identifier: '67890'
`)

	_, err := ParseConfig(cfg)
	require.EqualError(
		err,
		"Key: 'Config.Hosts.Publisher.Conn.SystemIdentifier' Error:Field validation for 'SystemIdentifier' failed on the 'required' tag",
	)
}

func TestConfig(t *testing.T) {
	require := require.New(t)

	expected := Config{
		Hosts: Hosts{
			Publisher: Host{
				Conn: ConnConfig{
					User:             "postgres1",
					Password:         "password1",
					Host:             "publisher",
					Port:             "5430",
					SystemIdentifier: "12345",
				},
			},
			Subscriber: Host{
				Conn: ConnConfig{
					User:             "postgres2",
					Password:         "password2",
					Host:             "subscriber",
					Port:             "5431",
					SystemIdentifier: "67890",
				},
			},
		},
	}

	ymlConfig := mustReadTestData("example.yml")

	cfg, err := ParseConfig(ymlConfig)
	require.NoError(err)
	require.Equal(expected, cfg)
	require.Equal("postgres://postgres1:password1@publisher:5430/", cfg.Hosts.Publisher.Conn.DSNURI(""))
}

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
				Pass: "password1",
				Host: "localhost",
				Port: "5430",
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
				Pass: "password1",
				Host: "localhost",
				Port: "5430",
				Args: []string{"--roles-only"},
			},
		)
		require.NoError(t, err)
		require.Contains(t, out, "-- PostgreSQL database cluster dump")
	})
}

func TestConn(t *testing.T) {
	require := require.New(t)

	publisher := ConnConfig{
		User:             "postgres",
		Password:         "password1",
		Host:             "localhost",
		Port:             "5430",
		SystemIdentifier: "12345",
	}

	ctx := context.TODO()

	t.Run("Ping", func(t *testing.T) {
		conn, err := Connect(ctx, publisher, "postgres")
		require.NoError(err)

		defer conn.Close(ctx)

		require.NoError(conn.Ping(ctx))
	})

	t.Run("Verify/Error", func(t *testing.T) {
		conn, err := Connect(ctx, publisher, "postgres")
		require.NoError(err)

		defer conn.Close(ctx)

		verr := conn.VerifySystemIdentifier(ctx)
		require.ErrorAs(verr, &SystemIdentifierError{})
	})

	t.Run("Verify/Verified", func(t *testing.T) {
		conn1, err := Connect(ctx, publisher, "postgres")
		require.NoError(err)

		defer conn1.Close(ctx)

		correctIden, err := conn1.getSystemIdentifier(ctx)
		require.NoError(err)

		// create a new conn with correct identifier
		conn2, err := Connect(
			ctx,
			ConnConfig{
				User:             "postgres",
				Password:         "password1",
				Host:             "localhost",
				Port:             "5430",
				SystemIdentifier: correctIden,
			},
			"postgres",
		)
		require.NoError(err)

		defer conn2.Close(ctx)

		verr := conn2.VerifySystemIdentifier(ctx)
		require.NoError(verr)
	})
}

func mustReadTestData(fn string) []byte {
	b, err := os.ReadFile(filepath.Join("_testdata", fn))
	if err != nil {
		panic(err)
	}

	return b
}