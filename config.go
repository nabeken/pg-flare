package flare

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func NewConnConfig(dsn string) ConnConfig {
	return ConnConfig{connString: dsn}
}

type ConnConfig struct {
	connString string
}

func (c ConnConfig) MustPSQLArgs() (PSQLArgs) {
	args, err := c.PSQLArgs()
	if err != nil {
		panic(err)
	}

	return args
}

func (c ConnConfig) PSQLArgs() (PSQLArgs, error) {
	u, err := url.Parse(c.connString)
	if err != nil {
		return PSQLArgs{}, fmt.Errorf("parsing connString: %w", err)
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return PSQLArgs{}, fmt.Errorf("parsing host: %w", err)
	}

	pass, _ := u.User.Password()

	return PSQLArgs{
		User: u.User.Username(),
		Pass: pass,
		Host: host,
		Port: port,
	}, nil
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
