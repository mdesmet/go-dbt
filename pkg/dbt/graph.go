package dbt

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	"github.com/flosch/pongo2/v4"
	"github.com/mdesmet/go-dbt/pkg/config"
)

type ModelConfig struct {
	Materialization string
	Alias           string
}

func createModelConfig(materialization string, alias string) *ModelConfig {
	modelConfig := ModelConfig{
		Materialization: materialization,
		Alias:           alias,
	}
	return &modelConfig
}

type Model struct {
	UniqueId    string
	Name        string
	DirEntry    fs.DirEntry
	RawSql      string
	CompiledSql string
	Children    map[string]bool
	Parents     map[string]bool
	Config      *ModelConfig
}

type Relation struct {
	database string
	schema   string
	object   string
}

type Source struct {
	Database string
	Schema   string
	Object   string
}

func (r Relation) String() string {
	return fmt.Sprintf("%s.%s.%s", r.database, r.schema, r.object)
}

func (model Model) fqn() Relation {
	return Relation{
		database: "test",
		schema:   "test",
		object:   model.Name,
	}
}

func (source Source) fqn() Relation {
	return Relation{
		database: source.Database,
		schema:   source.Schema,
		object:   source.Object,
	}
}

type Graph struct {
	Models        map[string]*Model
	ProjectConfig config.Config
	Profiles      config.Profiles
	Sources       map[string]map[string]Source
}

func createGraph() *Graph {
	graph := Graph{
		Models:  make(map[string]*Model),
		Sources: make(map[string]map[string]Source),
	}

	graph.parseProjectConfig()
	graph.parseProfiles()
	graph.discoverResources()
	graph.parseModels()
	return &graph
}

func (g *Graph) parseProjectConfig() {
	g.ProjectConfig = config.ReadConfig()
}

func (g *Graph) parseProfiles() {
	g.Profiles = config.ReadProfiles()
}

func (g *Graph) GetActiveConnection() *config.Connection {
	profile := g.Profiles[g.ProjectConfig.Profile]
	target := profile.Target
	connection := profile.Outputs[target]
	return &connection
}

func (g *Graph) discoverResources() {
	err := filepath.WalkDir("./models",
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			fileName := d.Name()

			if strings.HasSuffix(fileName, ".yml") {
				sourcesConfig := config.ReadSources(path)

				for _, sources := range sourcesConfig.Sources {
					for _, source := range sources.Tables {
						if _, seen := g.Sources[sources.Name]; !seen {
							g.Sources[sources.Name] = make(map[string]Source)
						}
						g.Sources[sources.Name][source] = Source{
							Database: sources.Database,
							Schema:   sources.Schema,
							Object:   source,
						}
					}
				}
			}

			if strings.HasSuffix(fileName, ".sql") {
				// TODO: parse project config
				name := fileName[0 : len(fileName)-4]
				key := fmt.Sprintf("model.%s.%s", g.ProjectConfig.Name, name)
				content, err := ioutil.ReadFile(path)
				if err != nil {
					log.Fatal(err)
				}

				if _, seen := g.Models[name]; seen {
					log.Fatal("Duplicate name detected", name)
				}

				g.Models[name] = &Model{
					Name:     name,
					DirEntry: d,
					UniqueId: key,
					RawSql:   string(content),
					Children: make(map[string]bool),
					Parents:  make(map[string]bool),
					Config:   createModelConfig("view", ""),
				}
			}

			return nil
		})
	if err != nil {
		log.Println(err)
	}
}

func (g *Graph) parseModels() {
	for name, model := range g.Models {
		_, err := compileWithContext(model, pongo2.Context{
			"ref":    g.registerRef(name),
			"source": g.source,
			"config": model.Config,
		})
		if err != nil {
			log.Fatal("Could not parse template")
		}
	}
}

func compileWithContext(model *Model, pongoContext pongo2.Context) (string, error) {
	tpl, err := pongo2.FromString(model.RawSql)
	if err != nil {
		return "", err
	}

	compiledSql, err := tpl.Execute(pongoContext)
	if err != nil {
		return "", err
	}
	return compiledSql, nil
}

func (g *Graph) registerRef(name string) func(string) Relation {
	return func(ref string) Relation {
		if _, seen := g.Models[ref]; seen {
			g.Models[name].Parents[ref] = true
			g.Models[ref].Children[name] = true
		} else {
			log.Fatal("Target ref doesn't exist")
		}

		return g.Models[ref].fqn()
	}
}

func (g *Graph) ref(ref string) Relation {
	if _, seen := g.Models[ref]; !seen {
		log.Fatal("Target ref doesn't exist")
	}
	return g.Models[ref].fqn()
}

func (g *Graph) source(namespace string, object string) Relation {
	if _, seen := g.Sources[namespace]; !seen {
		// source doesn't exist! bail out
		log.Fatal("Source namespace doesn't exist")
	}
	if _, seen := g.Sources[namespace][object]; !seen {
		// source doesn't exist! bail out
		log.Fatal("Source table doesn't exist")
	}
	return g.Sources[namespace][object].fqn()
}
