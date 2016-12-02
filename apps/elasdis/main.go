package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/funkygao/go-helix"
	"github.com/funkygao/gocli"
)

func main() {
	app := os.Args[0]
	c := cli.NewCLI(app, helix.Ver)
	c.Args = os.Args[1:]
	c.Commands = commands
	c.HelpFunc = func(m map[string]cli.CommandFactory) string {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("Elastic redis\n\n"))
		buf.WriteString(cli.BasicHelpFunc(app)(m))
		return buf.String()
	}

	exitCode, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	} else if c.IsVersion() {
		// -v | -version | --version
		os.Exit(0)
	}

	os.Exit(exitCode)
}
