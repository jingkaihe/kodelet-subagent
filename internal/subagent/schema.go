package subagent

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/google/jsonschema-go/jsonschema"
)

// schemas.json holds the advertised tool schemas. Each output schema is
// self-contained because Kodelet's code-mode declaration generator does not
// resolve local JSON Schema references.
//
//go:embed schemas.json
var schemaDocument []byte

type toolSchema struct {
	input    map[string]any
	output   map[string]any
	resolved *jsonschema.Resolved
}

var toolSchemas = mustLoadToolSchemas()

func mustLoadToolSchemas() map[string]toolSchema {
	var document struct {
		Inputs           map[string]map[string]any `json:"inputs"`
		AgentOutput      map[string]any            `json:"agentOutput"`
		SteerAgentOutput map[string]any            `json:"steerAgentOutput"`
	}
	if err := json.Unmarshal(schemaDocument, &document); err != nil {
		panic(fmt.Sprintf("subagent: invalid schemas.json: %v", err))
	}
	outputs := map[string]map[string]any{
		"spawn_agent":    document.AgentOutput,
		"wait_agent":     document.AgentOutput,
		"followup_agent": document.AgentOutput,
		"cancel_agent":   document.AgentOutput,
		"steer_agent":    document.SteerAgentOutput,
		"list_agents": {
			"properties": map[string]any{
				"agents": map[string]any{"items": document.AgentOutput, "title": "Agents", "type": "array"},
			},
			"required": []any{"agents"},
			"title":    "ListAgentsOutput",
			"type":     "object",
		},
	}
	schemas := make(map[string]toolSchema, len(outputs))
	for name, output := range outputs {
		input := document.Inputs[name]
		data, err := json.Marshal(input)
		if err != nil {
			panic(err)
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(data, &schema); err != nil {
			panic(fmt.Sprintf("subagent: invalid %s input schema: %v", name, err))
		}
		resolved, err := schema.Resolve(nil)
		if err != nil {
			panic(fmt.Sprintf("subagent: invalid %s input schema: %v", name, err))
		}
		schemas[name] = toolSchema{input: input, output: output, resolved: resolved}
	}
	return schemas
}

// clone returns copies safe to hand to the SDK.
func (s toolSchema) clone() (input, output map[string]any) {
	return cloneSchema(s.input), cloneSchema(s.output)
}

func cloneSchema(schema map[string]any) map[string]any {
	data, err := json.Marshal(schema)
	if err != nil {
		panic(err)
	}
	clone := map[string]any{}
	if err := json.Unmarshal(data, &clone); err != nil {
		panic(err)
	}
	return clone
}

// decodeInput validates raw against the tool's input schema and decodes it
// over defaults, so omitted optional fields keep their default values.
func decodeInput[In any](name string, raw json.RawMessage, defaults In) (In, error) {
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage("{}")
	}
	var instance any
	if err := json.Unmarshal(raw, &instance); err != nil {
		return defaults, err
	}
	if err := toolSchemas[name].resolved.Validate(instance); err != nil {
		return defaults, err
	}
	input := defaults
	if err := json.Unmarshal(raw, &input); err != nil {
		return defaults, err
	}
	return input, nil
}
