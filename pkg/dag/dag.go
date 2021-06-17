package dag

import "strings"

/*
	Select all nodes without any parents and add to queue
	Start processing the queue
	Once a node is finished, remove it from the Dag and add all nodes without parents to the queue
	Order the queue based on the int (for now always the number of descendants/parents)
	Execute until no nodes left
*/

type EdgeWalkFunc func(v string) map[string]int

type Dag struct {
	vertices  map[string]bool
	upEdges   map[string]map[string]int // the direct ancestors
	downEdges map[string]map[string]int // the direct descendants
}

func CreateDag() *Dag {
	dag := Dag{
		vertices:  make(map[string]bool),
		upEdges:   make(map[string]map[string]int),
		downEdges: make(map[string]map[string]int),
	}
	return &dag
}

func (dag *Dag) Len() int {
	return len(dag.vertices)
}

func (dag *Dag) Valid() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	for vertex := range dag.vertices {
		if dag.checkForCycles(vertex, visited, recStack) {
			return false
		}
	}
	return true
}

func (dag *Dag) checkForCycles(vertex string, visited map[string]bool, recStack map[string]bool) bool {
	recStack[vertex] = true
	visited[vertex] = true
	for vertex := range dag.downEdges[vertex] {
		if _, seen := visited[vertex]; !seen {
			if dag.checkForCycles(vertex, visited, recStack) {
				return true
			}
		}
		if _, seen := recStack[vertex]; seen {
			return true
		}
	}
	delete(recStack, vertex)
	return false
}

func (dag *Dag) AddVertex(vertex string) {
	dag.vertices[vertex] = true
}

func (dag *Dag) RemoveVertex(vertex string) {
	delete(dag.vertices, vertex)
	delete(dag.upEdges, vertex)
	for upEdge := range dag.upEdges {
		delete(dag.upEdges[upEdge], vertex)
	}
	delete(dag.downEdges, vertex)
	for downEdge := range dag.downEdges {
		delete(dag.downEdges[downEdge], vertex)
	}
}

func (dag *Dag) AddEdge(source string, target string) error {
	if _, created := dag.upEdges[target]; !created {
		dag.upEdges[target] = make(map[string]int)
	}
	dag.upEdges[target][source] = 1

	if _, created := dag.downEdges[source]; !created {
		dag.downEdges[source] = make(map[string]int)
	}
	dag.downEdges[source][target] = 1

	return nil
}

func (dag *Dag) VerticesWithoutAncestors() []string {
	vertices := make([]string, 0)
	for vertex := range dag.vertices {
		if len(dag.Ancestors(vertex)) == 0 {
			vertices = append(vertices, vertex)
		}
	}
	return vertices
}

func (dag *Dag) Descendants(v string) map[string]bool {
	seenEdges := make(map[string]bool)
	descendants := make(map[string]bool)
	return dag.recursivelyFindEdges(descendants, seenEdges, dag.walkDown(v), dag.walkDown)
}

func (dag *Dag) Ancestors(v string) map[string]bool {
	seenEdges := make(map[string]bool)
	ancestors := make(map[string]bool)
	return dag.recursivelyFindEdges(ancestors, seenEdges, dag.walkUp(v), dag.walkUp)
}

func (dag *Dag) Empty() bool {
	return len(dag.vertices) == 0
}

func (dag *Dag) Copy() *Dag {
	// TODO: maybe rewrite with public api
	newDag := CreateDag()
	for k, v := range dag.vertices {
		newDag.vertices[k] = v
	}
	for k, v := range dag.downEdges {
		newDag.downEdges[k] = make(map[string]int)
		for vertex, weight := range v {
			newDag.downEdges[k][vertex] = weight
		}
	}
	for k, v := range dag.upEdges {
		newDag.upEdges[k] = make(map[string]int)
		for vertex, weight := range v {
			newDag.upEdges[k][vertex] = weight
		}
	}
	return newDag
}

func (dag *Dag) ApplySelection(selection string) (*Dag, error) {
	if selection == "" {
		return dag, nil
	}

	selections := strings.Split(selection, " ")
	newDag := CreateDag()

	// TODO check if selections are valid

	for _, model := range selections {
		newDag.AddVertex(model)

		hasPlusPrefix := strings.HasPrefix(model, "+")
		hasPlusSuffix := strings.HasSuffix(model, "+")

		if !hasPlusPrefix && !hasPlusSuffix {
			continue
		}

		selectionDag := dag.Copy()

		if hasPlusPrefix {
			ancestors := selectionDag.Ancestors(model)
			for vertex := range selectionDag.vertices {
				if _, seen := ancestors[vertex]; !seen {
					selectionDag.RemoveVertex(vertex)
				}
			}
		}

		if hasPlusSuffix {
			descendants := selectionDag.Descendants(model)
			for vertex := range selectionDag.vertices {
				if _, seen := descendants[vertex]; !seen {
					selectionDag.RemoveVertex(vertex)
				}
			}
		}

		newDag.Union(selectionDag)
	}
	return newDag, nil
}

func (dag *Dag) Union(otherDag *Dag) {
	// TODO: maybe rewrite with public api
	for k, v := range otherDag.vertices {
		dag.vertices[k] = v
	}
	for k, v := range otherDag.downEdges {
		if _, seen := dag.downEdges[k]; !seen {
			dag.downEdges[k] = make(map[string]int)
		}
		for vertex, weight := range v {
			dag.downEdges[k][vertex] = weight
		}
	}
	for k, v := range otherDag.upEdges {
		if _, seen := dag.upEdges[k]; !seen {
			dag.upEdges[k] = make(map[string]int)
		}
		for vertex, weight := range v {
			dag.upEdges[k][vertex] = weight
		}
	}
}

func (dag *Dag) recursivelyFindEdges(descendantsOrAncestors map[string]bool, seenEdges map[string]bool, edges map[string]int, edgeWalker EdgeWalkFunc) map[string]bool {
	for vertex := range edges {
		if _, seen := seenEdges[vertex]; !seen {
			descendantsOrAncestors[vertex] = true
			seenEdges[vertex] = true
			descendantsOrAncestors = dag.recursivelyFindEdges(descendantsOrAncestors, seenEdges, edgeWalker(vertex), edgeWalker)
		}
	}
	return descendantsOrAncestors
}

func (dag *Dag) walkDown(v string) map[string]int {
	return dag.downEdges[v]
}

func (dag *Dag) walkUp(v string) map[string]int {
	return dag.upEdges[v]
}
