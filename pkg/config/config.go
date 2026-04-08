package config

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strings" // Added for strings.Contains

	"gopkg.in/yaml.v3"
)

// ReplayConfig defines the structure of our YAML configuration.
type ReplayConfig struct {
	DefaultS3Targets    []string          `yaml:"default_s3_targets"`
// 1 to 1 host mapping
//	HostMapping         map[string]string `yaml:"host_mapping"`
// 1 to many host mapping
	HostMapping map[string][]string `yaml:"host_mapping"`
	StateTrackingConfig `yaml:"state_tracking"` // Nested struct
}

// StateTrackingConfig defines configuration for the state machine.
type StateTrackingConfig struct {
	Enabled                bool     `yaml:"enabled"`
	RetentionWindowSeconds int      `yaml:"retention_window_seconds"`
	ProducerOperations     []string `yaml:"producer_operations"`
}

// compiledHostMapping stores direct and regex-compiled mappings.
type compiledHostMapping struct {
	//Direct   map[string]string
	Direct map[string][]string
	Wildcard []struct {
		Pattern *regexp.Regexp
		Targets []string
		//Target  string
	}
}

// LoadConfig loads the replay configuration from a YAML file.
func LoadConfig(filePath string) (*ReplayConfig, error) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	var cfg ReplayConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file %s: %w", filePath, err)
	}

	// Default state tracking settings if not specified
	if cfg.StateTrackingConfig.RetentionWindowSeconds == 0 {
		cfg.StateTrackingConfig.RetentionWindowSeconds = 600 // Default 10 minutes
	}
	if len(cfg.StateTrackingConfig.ProducerOperations) == 0 {
		cfg.StateTrackingConfig.ProducerOperations = []string{"PutObject", "CreateBucket"} // Common S3 producers
	}


	return &cfg, nil
}

// NewCompiledHostMapping processes the raw host_mapping from config
// to separate direct matches from wildcard/regex patterns and compile regexes.
//func NewCompiledHostMapping(rawMappings map[string]string) (*compiledHostMapping, error) {
func NewCompiledHostMapping(rawMappings map[string][]string) (*compiledHostMapping, error) {
	cm := &compiledHostMapping{
		//Direct:   make(map[string]string),
		Direct:    make(map[string][]string),
		Wildcard: []struct {
			Pattern *regexp.Regexp
			Targets	[]string
			//Target  string
		}{},
	}

//	for original, target := range rawMappings {
	for original, targets := range rawMappings {
		// Heuristic: If it contains common regex metacharacters, treat as regex.
		// Otherwise, treat as a direct string match.
		if containsRegexMetachar(original) {
			// Prepend and append ^$ to ensure full string match
			regexPattern := "^" + original + "$"
			
			re, err := regexp.Compile(regexPattern)
			if err != nil {
				return nil, fmt.Errorf("invalid regex pattern '%s' in host_mapping: %w", original, err)
			}
			cm.Wildcard = append(cm.Wildcard, struct {
				Pattern *regexp.Regexp
				Targets	[]string
			}{Pattern: re, Targets: targets})
			//cm.Wildcard = append(cm.Wildcard, struct {
				//Pattern *regexp.Regexp
				//Target  string
			//}{Pattern: re, Target: target})
		} else {
			//cm.Direct[original] = target
			cm.Direct[original] = targets
		}
	}
	return cm, nil
}

// Resolve is a convenience wrapper so callers (e.g. cli/replay.go) can
// keep the old cfg.Resolve(host) code path. It recompiles the mapping
// once per call; that’s fine for replay workloads.
func (cfg *ReplayConfig) Resolve(originalHost string) (string, error) {
    mapper, err := NewCompiledHostMapping(cfg.HostMapping)
    if err != nil {
        return "", err
    }
    rr := 0 // single-shot call – round-robin index local
    return mapper.ResolveTarget(originalHost, cfg.DefaultS3Targets, &rr), nil
}

// ResolveTarget resolves the new target URL based on original host,
// using direct mapping, then regex mapping, then default targets (round-robin).
//func (cm *compiledHostMapping) ResolveTarget(originalHost string, defaultTargets []string, roundRobinIdx *int) string {
func (cm *compiledHostMapping) ResolveTarget(originalHost string, defaultTargets []string, roundRobinIdx *int) string {
	// 1. Check explicit direct mappings
	//if target, ok := cm.Direct[originalHost]; ok {
	//	return target
	//}
        if targets, ok := cm.Direct[originalHost]; ok && len(targets) > 0 {
	       i := *roundRobinIdx % len(targets)
	       target := targets[i]
	       *roundRobinIdx = (*roundRobinIdx + 1) % len(targets)
	       return target
     }


	// 2. Check regex mappings
	// Iterate through wildcard patterns in the order they were defined in YAML.
	// The first match wins. This is important if you have overlapping patterns.
	for _, mapping := range cm.Wildcard {
		if mapping.Pattern.MatchString(originalHost) {
		//	return mapping.Target
		i := *roundRobinIdx % len(mapping.Targets)
		target := mapping.Targets[i]
		*roundRobinIdx = (*roundRobinIdx + 1) % len(mapping.Targets)
		return target
		}
	}

	// 3. Fallback to default_s3_targets (round-robin)
	if len(defaultTargets) > 0 {
		idx := *roundRobinIdx % len(defaultTargets)
		target := defaultTargets[idx]
		*roundRobinIdx = (*roundRobinIdx + 1) % len(defaultTargets) // Update for next call
		return target
	}

	return "" // No target found
}

// containsRegexMetachar checks if a string contains common regex metacharacters.
// This is a heuristic to determine if a string should be treated as a regex.
func containsRegexMetachar(s string) bool {
	metachars := []string{".", "*", "+", "?", "|", "(", ")", "[", "]", "{", "}", "^", "$", "\\"}
	for _, char := range metachars {
		if strings.Contains(s, char) {
			return true
		}
	}
	return false
}
