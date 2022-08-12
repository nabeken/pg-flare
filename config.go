package flare

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func NewConnConfig(dsn string) ConnConfig {
	return ConnConfig{connString: dsn}
}

type ConnConfig struct {
	connString string
}

func (c ConnConfig) SwitchDatabase(newDB string) (ConnConfig, error) {
	u, err := url.Parse(c.connString)
	if err != nil {
		return ConnConfig{}, fmt.Errorf("parsing connString: %w", err)
	}

	u.Path = newDB

	return ConnConfig{
		connString: u.String(),
	}, nil
}

type SuperUserConfig struct {
	ConnConfig ConnConfig
}

func (c SuperUserConfig) Open() (*sql.DB, error) {
	return Open(c.ConnConfig.connString)
}

func (c SuperUserConfig) SwitchDatabase(newDB string) (SuperUserConfig, error) {
	newConfig, err := c.ConnConfig.SwitchDatabase(newDB)
	if err != nil {
		return SuperUserConfig{}, err
	}

	return SuperUserConfig{
		ConnConfig: newConfig,
	}, nil
}

type PublisherConfig struct {
	ConnConfig ConnConfig
}

type SubscriberConfig struct {
	ConnConfig ConnConfig
}
