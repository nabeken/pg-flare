package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	flare "github.com/nabeken/pg-flare"
	"github.com/stretchr/testify/assert"
)

func TestCLI(t *testing.T) {
	assert := assert.New(t)

	tmpdir := t.TempDir()
	flareCLI := filepath.Join(tmpdir, "flare")
	flareCfg := filepath.Join(tmpdir, "flare.yml")

	testYAML := mustGenerateTestYAML()

	if err := os.WriteFile(flareCfg, testYAML, 0666); err != nil {
		t.Fatal(err)
	}

	if err := exec.Command("go", "build", "-o", flareCLI, ".").Run(); err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer

	tcmd := exec.Command(flareCLI, "--config", flareCfg, "exec", "--", "sh", "-c", "env | sort | grep FLARE_")
	tcmd.Stdout = &b
	tcmd.Stderr = &b

	assert.NoError(tcmd.Run())
	assert.Equal(`FLARE_CONNINFO_PUBLISHER_HOST=127.0.0.1
FLARE_CONNINFO_PUBLISHER_PORT=5430
FLARE_CONNINFO_SUBSCRIBER_HOST=127.0.1
FLARE_CONNINFO_SUBSCRIBER_PORT=5431
`, b.String())

}

func mustGenerateTestYAML() []byte {
	publisher := flare.ConnConfig{
		SuperUser:         "postgres",
		SuperUserPassword: "password1",
		Host:              "127.0.0.1",
		Port:              "5430",
		SystemIdentifier:  "",
	}

	subscriber := flare.ConnConfig{
		SuperUser:         "postgres",
		SuperUserPassword: "password2",
		Host:              "127.0.0.1",
		Port:              "5431",
		SystemIdentifier:  "",
	}

	ctx := context.TODO()

	pconn, err := flare.Connect(ctx, publisher.SuperUserInfo(), "postgres")
	if err != nil {
		panic(err)
	}

	defer pconn.Close(ctx)

	psysID, err := pconn.GetSystemIdentifier(ctx)
	if err != nil {
		panic(err)
	}

	sconn, err := flare.Connect(ctx, subscriber.SuperUserInfo(), "postgres")
	if err != nil {
		panic(err)
	}

	defer sconn.Close(ctx)

	ssysID, err := sconn.GetSystemIdentifier(ctx)
	if err != nil {
		panic(err)
	}

	tmpl := mustReadTestData("test.tpl.yml")
	tmpl = bytes.Replace(tmpl, []byte("@@PUBLISHER_SYSTEM_ID@@"), []byte(psysID), 1)
	tmpl = bytes.Replace(tmpl, []byte("@@SUBSCRIBER_SYSTEM_ID@@"), []byte(ssysID), 1)

	return tmpl
}

func mustReadTestData(fn string) []byte {
	b, err := os.ReadFile(filepath.Join("_testdata", fn))
	if err != nil {
		panic(err)
	}

	return b
}
