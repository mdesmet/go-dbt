package dbt

import (
	"fmt"

	"github.com/spf13/cobra"
)

func compileTask(cmd *cobra.Command, _ []string) {
	fmt.Println(cmd.Flags().GetString("model"))
	fmt.Println("----------------")
	graph := createGraph()
	for _, node := range graph.Models {
		fmt.Println(node.UniqueId)
		fmt.Println(node.CompiledSql)
		fmt.Println("----------------")
	}
}
