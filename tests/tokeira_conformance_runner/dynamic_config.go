package runner //nolint:revive // the package's existing name

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
)

const (
	DynamicConfigTestPrefix       = "TOKEIRA_CONFIG_TEST_UNHONOURED"
	DynamicConfigHarnessPrefix    = "TOKEIRA_CONFIG_HARNESS_UNHONOURED"
	DynamicConfigTestLogFormat    = DynamicConfigTestPrefix + " test=%q key=%q value=%q answer=%q"
	DynamicConfigHarnessLogFormat = DynamicConfigHarnessPrefix + " key=%q value=%q answer=%q"
)

// DynamicConfigReport attributes labels by their explicit test name, since Go can
// attach setup and sub-test output to a different test's event. Repeats share one
// entry per test and key, retaining every distinct requested value and answer.
type DynamicConfigReport struct {
	tests           map[string]map[string]map[string]struct{}
	harness         map[string]map[string]struct{}
	unreadableLines int
}

func (r *DynamicConfigReport) readLine(line string) error {
	var test, key, value, answer string
	testIndex := strings.Index(line, DynamicConfigTestPrefix)
	harnessIndex := strings.Index(line, DynamicConfigHarnessPrefix)
	if testIndex >= 0 && (harnessIndex < 0 || testIndex < harnessIndex) {
		if _, err := fmt.Sscanf(line[testIndex:], DynamicConfigTestLogFormat, &test, &key, &value, &answer); err != nil {
			r.unreadableLines++
			return fmt.Errorf("parse test dynamic-config label: %w", err)
		}
		if r.tests == nil {
			r.tests = make(map[string]map[string]map[string]struct{})
		}
		if r.tests[test] == nil {
			r.tests[test] = make(map[string]map[string]struct{})
		}
		addConfigAnswer(r.tests[test], key, value, answer)
	} else if harnessIndex >= 0 {
		if _, err := fmt.Sscanf(line[harnessIndex:], DynamicConfigHarnessLogFormat, &key, &value, &answer); err != nil {
			r.unreadableLines++
			return fmt.Errorf("parse harness dynamic-config label: %w", err)
		}
		if r.harness == nil {
			r.harness = make(map[string]map[string]struct{})
		}
		addConfigAnswer(r.harness, key, value, answer)
	}
	return nil
}

func addConfigAnswer(settings map[string]map[string]struct{}, key, value, answer string) {
	if settings[key] == nil {
		settings[key] = make(map[string]struct{})
	}
	settings[key][fmt.Sprintf("value=%q answer=%q", value, answer)] = struct{}{}
}

func (r *DynamicConfigReport) ReadVerbose(input io.Reader) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var firstErr error
	for scanner.Scan() {
		if err := r.readLine(scanner.Text()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return errors.Join(firstErr, scanner.Err())
}

func (r *DynamicConfigReport) ReadJSON(input io.Reader) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var pending string
	var firstErr error
	for scanner.Scan() {
		var event struct {
			Action string
			Output string
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			r.unreadableLines++
			if firstErr == nil {
				firstErr = fmt.Errorf("read go test JSON: %w", err)
			}
			continue
		}
		if event.Action != "output" {
			continue
		}
		// test2json can split a long log line across output events.
		pending += event.Output
		for {
			line, rest, found := strings.Cut(pending, "\n")
			if !found {
				break
			}
			if err := r.readLine(line); err != nil && firstErr == nil {
				firstErr = err
			}
			pending = rest
		}
	}
	if err := r.readLine(pending); err != nil && firstErr == nil {
		firstErr = err
	}
	return errors.Join(firstErr, scanner.Err())
}

func (r *DynamicConfigReport) Summary() string {
	var out strings.Builder
	fmt.Fprintf(&out, "----- tests that ran without a setting they asked for: %d -----\n", len(r.tests))
	for _, test := range slices.Sorted(maps.Keys(r.tests)) {
		for _, key := range slices.Sorted(maps.Keys(r.tests[test])) {
			fmt.Fprintf(&out, "  %s key=%q %s\n", test, key, configAnswers(r.tests[test][key]))
		}
	}
	fmt.Fprintf(&out, "----- harness settings tokeirad did not honour: %d -----\n", len(r.harness))
	for _, key := range slices.Sorted(maps.Keys(r.harness)) {
		fmt.Fprintf(&out, "  key=%q %s\n", key, configAnswers(r.harness[key]))
	}
	if r.unreadableLines > 0 {
		fmt.Fprintf(&out, "----- dynamic-config listing incomplete: %d unreadable lines -----\n", r.unreadableLines)
	}
	return out.String()
}

func configAnswers(answers map[string]struct{}) string {
	return strings.Join(slices.Sorted(maps.Keys(answers)), "; ")
}
