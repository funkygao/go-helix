package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/funkygao/go-helix/ver"
	"github.com/funkygao/gocli"
)

func main() {
	app := os.Args[0]
	for _, arg := range os.Args[1:] {
		if arg == "--generate-bash-completion" {
			for name, _ := range commands {
				fmt.Println(name)
			}
			return
		}
	}
	c := cli.NewCLI(app, ver.Ver)
	c.Args = os.Args[1:]
	c.Commands = commands
	c.HelpFunc = func(m map[string]cli.CommandFactory) string {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("Distributed replicated file system under Helix\n\n"))
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
