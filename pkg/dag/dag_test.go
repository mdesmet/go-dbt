package dag

import (
	"reflect"
	"testing"
)

func createDag(vertices []string) *Dag {
	dag := CreateDag()

	for _, string := range vertices {
		dag.AddVertex(string)
	}
	return dag
}

func TestDagCreation(t *testing.T) {
	vertices := []string{"1", "2"}
	dag := createDag(vertices)

	if !verticesEquals(dag.vertices, vertices) {
		t.Error()
	}
}

func TestDagValidateReturnsTrueForValidDag(t *testing.T) {
	vertices := []string{"1", "2"}
	dag := createDag(vertices)
	dag.AddEdge("1", "2")
	if !dag.Valid() {
		t.Error()
	}
}

func TestDagValidateReturnsFalseForInValidDagWithDirectCycle(t *testing.T) {
	vertices := []string{"1", "2"}
	dag := createDag(vertices)
	dag.AddEdge("1", "2")
	dag.AddEdge("2", "1")
	if dag.Valid() {
		t.Error()
	}
}

func TestDagValidateReturnsFalseForInValidDagWithIndirectCycle(t *testing.T) {
	vertices := []string{"1", "2", "3"}
	dag := createDag(vertices)
	dag.AddEdge("1", "2")
	dag.AddEdge("2", "3")
	dag.AddEdge("3", "1")
	if dag.Valid() {
		t.Error()
	}
}

func TestAddEdge(t *testing.T) {
	vertices := []string{"1", "2"}
	dag := createDag(vertices)

	e := dag.AddEdge("1", "2")
	if e != nil {
		t.Error(e)
	}
	descendants := dag.Descendants("1")
	if !reflect.DeepEqual([]string{"2"}, descendants) {
		t.Error()
	}

	ancestors := dag.Ancestors("2")
	if !reflect.DeepEqual([]string{"1"}, ancestors) {
		t.Error()
	}
}

func verticesEquals(current map[string]bool, expected []string) bool {
	vertices := make(map[string]bool)
	for _, vertex := range expected {
		vertices[vertex] = true
	}
	return reflect.DeepEqual(current, vertices)
}
