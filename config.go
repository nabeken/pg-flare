package flare

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func MustNewConnConfig(dsn string) ConnConfig {
	c, err := NewConnConfig(dsn)
	if err != nil {
		panic(err)
	}

	return c
}

func NewConnConfig(dsn string) (ConnConfig, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return ConnConfig{}, fmt.Errorf("parsing dsn: %w", err)
	}

	return ConnConfig{
		connString: dsn,
		u:          u,
	}, nil
}

type ConnConfig struct {
	connString string
	u          *url.URL
}

func (c ConnConfig) StdConnInfo() (string, error) {
	u, err := url.Parse(c.connString)
	if err != nil {
		return "", fmt.Errorf("parsing connString: %w", err)
	}

	v, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("parsing the query: %w", err)
	}

	// remove non-standard parameters
	v.Del("x-publication")

	u.RawQuery = v.Encode()

	return u.String(), nil
}

func (c ConnConfig) MustQuery() url.Values {
	q, err := c.Query()
	if err != nil {
		panic(err)
	}

	return q
}

func (c ConnConfig) Query() (url.Values, error) {
	return url.ParseQuery(c.u.RawQuery)
}

func (c ConnConfig) MustPSQLArgs() PSQLArgs {
	args, err := c.PSQLArgs()
	if err != nil {
		panic(err)
	}

	return args
}

func (c ConnConfig) PSQLArgs() (PSQLArgs, error) {
	host, port, err := net.SplitHostPort(c.u.Host)
	if err != nil {
		return PSQLArgs{}, fmt.Errorf("parsing host: %w", err)
	}

	pass, _ := c.u.User.Password()

	return PSQLArgs{
		User: c.u.User.Username(),
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

	return NewConnConfig(u.String())
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
