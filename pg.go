package flare

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

type PSQLArgs struct {
	User string
	Host string
	Port string
	Pass string
	Args []string
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

	return append(args, a.Args...)
}

func PGDump(args PSQLArgs, db, password string) (string, error) {
	dumpArgs := []string{}
	dumpArgs = append(dumpArgs, args.BuildArgs()...)

	cmd := exec.Command("pg_dump", append(dumpArgs, db)...)
	cmd.Env = []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
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

func PGDumpAll(args PSQLArgs) (string, error) {
	dumpArgs := []string{}
	dumpArgs = append(dumpArgs, args.BuildArgs()...)

	cmd := exec.Command("pg_dumpall", dumpArgs...)
	cmd.Env = []string{
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("PGPASSWORD=%s", args.Pass),
	}

	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pg_dumpall: %w: %s", err, errout.String())
	}

	return out.String(), nil
}
