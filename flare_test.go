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
      superuser: 'postgres1'
      superuser_password: 'password1'

      db_owner: 'postgres1'
      db_owner_password: 'password1'

      host: 'publisher'
      port: '5430'
  subscriber:
    conn:
      superuser: 'postgres2'
      superuser_password: 'password2'

      db_owner: 'postgres1'
      db_owner_password: 'password1'

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
					SuperUser:         "postgres1",
					SuperUserPassword: "password1",

					DBOwner:         "owner",
					DBOwnerPassword: "owner",

					ReplicationUser:         "repl",
					ReplicationUserPassword: "repl",

					Host:              "publisher",
					HostViaSubscriber: "publisher_sub",
					Port:              "5430",
					PortViaSubscriber: "5432",

					SystemIdentifier: "12345",
				},
			},
			Subscriber: Host{
				Conn: ConnConfig{
					SuperUser:         "postgres2",
					SuperUserPassword: "password2",

					DBOwner:         "owner",
					DBOwnerPassword: "owner",

					Host:             "subscriber",
					Port:             "5431",
					SystemIdentifier: "67890",
				},
			},
		},
		Publications: map[string]Publication{
			"pubtable1": {
				PubName: "publication1-name",
				ReplicaIdentityFullTables: []string{
					"full1", "full2",
				},
			},
			"pubtable2": {
				PubName: "publication2-name",
				ReplicaIdentityFullTables: []string{
					"full3", "full4",
				},
			},
		},
		Subscriptions: map[string]Subscription{
			"benchsub1": {
				DBName:  "pubtable1",
				PubName: "publication1-name",
			},
			"benchsub2": {
				DBName:  "pubtable2",
				PubName: "publication2-name",
			},
		},
	}

	ymlConfig := mustReadTestData("example.yml")

	cfg, err := ParseConfig(ymlConfig)
	require.NoError(err)
	require.Equal(expected, cfg)
	require.Equal("postgres://postgres1:password1@publisher:5430/", cfg.Hosts.Publisher.Conn.SuperUserInfo().DSNURI(""))
	require.Equal("postgres://postgres1:password1@publisher_sub:5432/", cfg.Hosts.Publisher.Conn.SuperUserInfo().DSNURIForSubscriber(""))
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
		SuperUser:         "postgres",
		SuperUserPassword: "password1",
		Host:              "localhost",
		Port:              "5430",
		SystemIdentifier:  "12345",
	}

	ctx := context.TODO()

	t.Run("Ping", func(t *testing.T) {
		conn, err := Connect(ctx, publisher.SuperUserInfo(), "postgres")
		require.NoError(err)

		defer conn.Close(ctx)

		require.NoError(conn.Ping(ctx))
	})

	t.Run("Verify/Error", func(t *testing.T) {
		conn, err := Connect(ctx, publisher.SuperUserInfo(), "postgres")
		require.NoError(err)

		defer conn.Close(ctx)

		verr := conn.VerifySystemIdentifier(ctx)
		require.ErrorAs(verr, &SystemIdentifierError{})
	})

	t.Run("Verify/Verified", func(t *testing.T) {
		conn1, err := Connect(ctx, publisher.SuperUserInfo(), "postgres")
		require.NoError(err)

		defer conn1.Close(ctx)

		correctIden, err := conn1.getSystemIdentifier(ctx)
		require.NoError(err)

		// create a new conn with correct identifier
		conn2, err := Connect(
			ctx,
			ConnConfig{
				SuperUser:         "postgres",
				SuperUserPassword: "password1",
				Host:              "localhost",
				Port:              "5430",
				SystemIdentifier:  correctIden,
			}.SuperUserInfo(),
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
