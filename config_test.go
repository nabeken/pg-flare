package flare

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnConfig(t *testing.T) {
	require := require.New(t)

	connConfig := MustNewConnConfig("pgx://localhost:5432/postgres?test=true")

	newConfig, err := connConfig.SwitchDatabase("new")
	require.NoError(err)
	require.Equal("pgx://localhost:5432/new?test=true", newConfig.connString)
}
