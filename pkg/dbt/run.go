package dbt

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/flosch/pongo2/v4"
	"github.com/mdesmet/go-dbt/pkg/dag"
	"github.com/mdesmet/go-dbt/pkg/database"
	"github.com/spf13/cobra"
)

type taskResult struct {
	modelId string
	ok      TaskStatus
	desc    string
}

type TaskStatus int

const (
	Ok TaskStatus = iota
	Error
	Skipped
)

func runTask(cmd *cobra.Command, _ []string) {
	dag := dag.CreateDag()
	graph := createGraph()
	populateDag(graph, dag)

	// select from dag
	selection, _ := cmd.Flags().GetString("model")
	dag, _ = dag.ApplySelection(selection)

	runSelection(graph, dag)
}

func populateDag(graph *Graph, dag *dag.Dag) {
	for _, model := range graph.Models {
		dag.AddVertex(model.Name)
		for name := range model.Children {
			dag.AddEdge(model.Name, name)
		}
	}

	if !dag.Valid() {
		// should probably do this in a better way
		log.Fatal("Dag contains cycles, can't continue")
	}
}

func runSelection(graph *Graph, dag *dag.Dag) {
	connection := graph.GetActiveConnection()

	db := database.Connect(connection)
	defer db.Close()

	numWorkers := connection.Threads
	numJobs := dag.Len()
	results := make(chan taskResult, numJobs)
	jobs := make(chan *Model, numJobs)

	for w := 1; w <= numWorkers; w++ {
		ctx := context.TODO()
		conn, err := db.Conn(ctx)
		if err != nil {
			log.Fatal(err)
		}
		go worker(ctx, conn, *graph, w, jobs, results)
	}

	// get all nodes without any ancestors and order based on number of descendants
	addedModels := make(map[string]bool)
	addModelsToQueue(addedModels, dag, jobs, graph)

	for a := 1; a <= numJobs; a++ {
		result := <-results
		fmt.Println("Received result", result)
		dag.RemoveVertex(result.modelId)
		addModelsToQueue(addedModels, dag, jobs, graph)
		if result.ok == Error {
			// skip all descendants if a model errored
			for descendant := range dag.Descendants(result.modelId) {
				dag.RemoveVertex(descendant)
				results <- createTaskResult(descendant, Ok, fmt.Sprintf("Skipped Model %s", descendant))
			}
			fmt.Println(result.desc)
		}
		if dag.Empty() {
			close(jobs)
		}
	}
}

func addModelsToQueue(addedModels map[string]bool, dag *dag.Dag, jobs chan<- *Model, graph *Graph) {
	for _, vertex := range dag.VerticesWithoutAncestors() {
		if _, seen := addedModels[vertex]; !seen {
			fmt.Println("Adding vertex", vertex)
			jobs <- graph.Models[vertex]
			addedModels[vertex] = true
		}
	}
}

func worker(ctx context.Context, conn *sql.Conn, g Graph, workerId int, jobs <-chan *Model, results chan<- taskResult) {
	for model := range jobs {
		runModel(ctx, conn, g, workerId, model, results)
	}
}

func runModel(ctx context.Context, conn *sql.Conn, g Graph, workerId int, model *Model, results chan<- taskResult) {
	fmt.Println("worker", workerId, "started  job", model.UniqueId)
	compiledSQl, err := compileWithContext(model, pongo2.Context{
		"ref":    g.ref,
		"source": g.source,
		"config": model.Config,
	})
	if err != nil {
		log.Fatal("An error occurred while compiling model")
	}
	model.CompiledSql = compiledSQl
	fmt.Println("Going to execute SQL", model.CompiledSql)
	_, err = conn.ExecContext(ctx, model.CompiledSql)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
	fmt.Println("worker", workerId, "finished job", model.UniqueId)
	results <- createTaskResult(model.Name, Ok, fmt.Sprintf("Model %s has run", model.Name))
}

func createTaskResult(modelId string, status TaskStatus, desc string) taskResult {
	return taskResult{
		modelId: modelId,
		ok:      status,
		desc:    desc,
	}
}
