package main

import (
	"github.com/mdesmet/go-dbt/pkg/dbt"
)

func main() {
	rootCmd := dbt.RootCommand()
	rootCmd.Execute()
}
