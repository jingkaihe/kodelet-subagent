package types

import (
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"
)

// AgentNameMaxLength bounds canonical agent names.
const AgentNameMaxLength = 48

// AgentNamePattern matches one to three lowercase kebab-case words.
var AgentNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+){0,2}$`)

// ValidateAgentName returns name if it is a canonical agent name.
func ValidateAgentName(name string) (string, error) {
	if name == "" {
		return "", errors.New("name is required")
	}
	if utf8.RuneCountInString(name) > AgentNameMaxLength {
		return "", fmt.Errorf("name must be at most %d characters", AgentNameMaxLength)
	}
	if !AgentNamePattern.MatchString(name) {
		return "", errors.New("name must contain one to three lowercase words separated by single " +
			"hyphens, start with a letter, and contain only letters and digits")
	}
	return name, nil
}
