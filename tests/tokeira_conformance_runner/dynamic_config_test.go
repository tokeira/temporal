package runner

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicConfigReportStreams(t *testing.T) {
	lines := []string{
		"=== RUN   TestSuite/TestOther\n",
		`    bridge.go:12: TOKEIRA_CONFIG_TEST_UNHONOURED test="TestSuite/TestLeaf/child" key="key.b" value="4" answer="http 503"` + "\n",
		`    setup.go:10: TOKEIRA_CONFIG_TEST_UNHONOURED test="TestSuite/TestLeaf" key="key.a" value="1" answer="connect status unimplemented: unknown key"` + "\n",
		`    bridge.go:12: TOKEIRA_CONFIG_TEST_UNHONOURED test="TestSuite/TestLeaf" key="key.b" value="2" answer="no control listener"` + "\n",
		`    setup.go:10: TOKEIRA_CONFIG_HARNESS_UNHONOURED key="key.a" value="3000" answer="unimplemented"` + "\n",
		"--- PASS: TestSuite/TestLeaf (0.01s)\n",
		"--- FAIL: TestSuite/TestLeaf/child (0.01s)\n",
	}
	want := `----- tests that ran without a setting they asked for: 2 -----
  TestSuite/TestLeaf key="key.a" value="1" answer="connect status unimplemented: unknown key"
  TestSuite/TestLeaf key="key.b" value="2" answer="no control listener"
  TestSuite/TestLeaf/child key="key.b" value="4" answer="http 503"
----- harness settings tokeirad did not honour: 1 -----
  key="key.a" value="3000" answer="unimplemented"
`
	for _, format := range []string{"verbose", "json"} {
		t.Run(format, func(t *testing.T) {
			var input strings.Builder
			encoder := json.NewEncoder(&input)
			for range 3 {
				for _, line := range lines {
					if format == "verbose" {
						input.WriteString(line)
					} else {
						// The event's test can differ from the name in the label.
						require.NoError(t, encoder.Encode(map[string]string{"Action": "output", "Test": "TestSuite/TestOther", "Output": line}))
					}
				}
			}
			var report DynamicConfigReport
			if format == "verbose" {
				require.NoError(t, report.ReadVerbose(strings.NewReader(input.String())))
			} else {
				require.NoError(t, report.ReadJSON(strings.NewReader(input.String())))
			}
			require.Equal(t, want, report.Summary())
		})
	}
}

func TestDynamicConfigReportValues(t *testing.T) {
	var report DynamicConfigReport
	for _, value := range []string{"1", "2", "1"} {
		line := fmt.Sprintf(DynamicConfigTestLogFormat, "TestSuite/TestLeaf", "key", value, "refused\n\"quoted\"\\answer")
		require.NotContains(t, line, "\n", "a label occupies one physical line")
		require.NoError(t, report.ReadVerbose(strings.NewReader(line)))
	}
	require.Equal(t, `----- tests that ran without a setting they asked for: 1 -----
  TestSuite/TestLeaf key="key" value="1" answer="refused\n\"quoted\"\\answer"; value="2" answer="refused\n\"quoted\"\\answer"
----- harness settings tokeirad did not honour: 0 -----
`, report.Summary())
	line := fmt.Sprintf(DynamicConfigHarnessLogFormat, "key", DynamicConfigTestPrefix, "refused")
	for range 2 {
		require.NoError(t, report.ReadVerbose(strings.NewReader(line)))
	}
	require.Contains(t, report.Summary(), "----- harness settings tokeirad did not honour: 1 -----\n")
	require.Contains(t, report.Summary(), `key="key" value="TOKEIRA_CONFIG_TEST_UNHONOURED" answer="refused"`)
}

func TestDynamicConfigReportJSONFragments(t *testing.T) {
	line := fmt.Sprintf(DynamicConfigTestLogFormat, "TestSuite/TestLeaf", "key", "1", "refused")
	var input strings.Builder
	encoder := json.NewEncoder(&input)
	for _, fragment := range []string{line[:25], line[25:]} {
		require.NoError(t, encoder.Encode(map[string]string{"Action": "output", "Output": fragment}))
	}
	require.NoError(t, encoder.Encode(map[string]string{"Action": "pass", "Test": "TestSuite/TestLeaf"}))
	var report DynamicConfigReport
	require.NoError(t, report.ReadJSON(strings.NewReader(input.String())))
	require.Contains(t, report.Summary(), `TestSuite/TestLeaf key="key" value="1" answer="refused"`)
}

func TestDynamicConfigReportErrors(t *testing.T) {
	var report DynamicConfigReport
	require.ErrorContains(t, report.ReadJSON(strings.NewReader("{")), "read go test JSON")
	require.ErrorContains(t, report.ReadVerbose(strings.NewReader(DynamicConfigTestPrefix)), "parse test dynamic-config label")
	require.ErrorContains(t, report.ReadVerbose(strings.NewReader(DynamicConfigHarnessPrefix)), "parse harness dynamic-config label")
	require.ErrorContains(t, report.ReadVerbose(configErrorReader{}), "read failed")
	require.ErrorContains(t, report.ReadJSON(configErrorReader{}), "read failed")
	require.Equal(t, "----- tests that ran without a setting they asked for: 0 -----\n----- harness settings tokeirad did not honour: 0 -----\n----- dynamic-config listing incomplete: 3 unreadable lines -----\n", report.Summary())
}

func TestDynamicConfigReportContinuesAfterUnreadableLines(t *testing.T) {
	for _, format := range []string{"verbose", "json"} {
		t.Run(format, func(t *testing.T) {
			lines := []string{
				fmt.Sprintf(DynamicConfigTestLogFormat, "TestBefore", "key", "1", "refused"),
				DynamicConfigTestPrefix,
				fmt.Sprintf(DynamicConfigTestLogFormat, "TestAfter", "key", "2", "refused"),
				DynamicConfigHarnessPrefix,
				fmt.Sprintf(DynamicConfigHarnessLogFormat, "key", "3", "refused"),
			}
			var input strings.Builder
			encoder := json.NewEncoder(&input)
			for _, line := range lines {
				if format == "verbose" {
					input.WriteString(line + "\n")
				} else {
					require.NoError(t, encoder.Encode(map[string]string{"Action": "output", "Output": line + "\n"}))
					if line == DynamicConfigTestPrefix {
						input.WriteString("not JSON\n")
					}
				}
			}
			var report DynamicConfigReport
			unreadable := 2
			if format == "verbose" {
				require.ErrorContains(t, report.ReadVerbose(strings.NewReader(input.String())), "parse test dynamic-config label")
			} else {
				unreadable++
				require.ErrorContains(t, report.ReadJSON(strings.NewReader(input.String())), "parse test dynamic-config label")
			}
			require.Equal(t, `----- tests that ran without a setting they asked for: 2 -----
  TestAfter key="key" value="2" answer="refused"
  TestBefore key="key" value="1" answer="refused"
----- harness settings tokeirad did not honour: 1 -----
  key="key" value="3" answer="refused"
`+fmt.Sprintf("----- dynamic-config listing incomplete: %d unreadable lines -----\n", unreadable), report.Summary())
		})
	}
}

type configErrorReader struct{}

func (configErrorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }
