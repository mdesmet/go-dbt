package dbt

import "github.com/spf13/cobra"

func RootCommand() *cobra.Command {
	currentVersion := "0.0.1"

	cmd := cobra.Command{
		Use:     "fast-dbt",
		Short:   "fast-dbt CLI",
		Version: currentVersion,
		Long: `
TODO.
Explore the available commands by running 'fast-dbt --help'`,
	}

	cmd.SetVersionTemplate("fast-dbt v{{.Version}}\n")

	run := cobra.Command{
		Use:   "run",
		Short: "Run a model",
		Long:  `TODO`,
		Run:   runTask,
	}

	run.Flags().StringP("model", "m", "", "Specify the models to be run")

	cmd.AddCommand(&run)

	compile := cobra.Command{
		Use:   "compile",
		Short: "Compile a model",
		Long:  `TODO`,
		Run:   compileTask,
	}

	compile.Flags().StringP("model", "m", "", "Specify the models to be compiled")

	cmd.AddCommand(&compile)

	watch := cobra.Command{
		Use:   "watch",
		Short: "Compile all models automatically",
		Long:  `TODO`,
		Run:   watchTask,
	}

	cmd.AddCommand(&watch)

	return &cmd
}
