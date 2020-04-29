package main

import (
	"github.com/danielgtaylor/openapi-cli-generator/cli"
)

func main() {
	cli.Init(&cli.Config{
		AppName:   "waspctl",
		EnvPrefix: "WASPCTL",
		Version:   "1.0.0",
	})

    waspOpenapiRegister(false)

	cli.Root.Execute()
}
