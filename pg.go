package flare

import (
	"bytes"
	"fmt"
	"os/exec"
)

type PSQLArgs struct {
	User string
	Host string
	Port string
}

func (a PSQLArgs) BuildArgs() []string {
	var args []string

	if a.User != "" {
		args = append(args, []string{"-U", a.User}...)
	}
	if a.Host != "" {
		args = append(args, []string{"-h", a.Host}...)
	}
	if a.Port != "" {
		args = append(args, []string{"-p", a.Port}...)
	}

	return args
}

func PGDump(args PSQLArgs, db, password string) (string, error) {
	cmd := exec.Command("pg_dump", append(args.BuildArgs(), db)...)
	cmd.Env = []string{
		fmt.Sprintf("PGPASSWORD=%s", password),
	}

	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pg_dump: %w: %s", err, errout.String())
	}

	return out.String(), nil
}
