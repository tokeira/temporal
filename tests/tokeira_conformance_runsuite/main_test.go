package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSuiteOutputLabelledOutcomes(t *testing.T) {
	for _, result := range []string{"PASS", "FAIL"} {
		t.Run(result, func(t *testing.T) {
			output := `    setup.go:1: TOKEIRA_CONFIG_TEST_UNHONOURED test="TestSuite/TestLeaf" key="key" value="4" answer="unimplemented"
    setup.go:2: TOKEIRA_CONFIG_HARNESS_UNHONOURED key="key" value="1" answer="unimplemented"
    --- ` + result + ": TestSuite/TestLeaf (0.01s)\n"
			outcome, err := parseSuiteOutput([]byte(strings.Repeat(output, 3)))
			require.NoError(t, err)
			require.Zero(t, outcome.skip)
			if result == "PASS" {
				require.Equal(t, 3, outcome.pass)
				require.True(t, outcome.ok())
			} else {
				require.Equal(t, 3, outcome.fail)
				require.False(t, outcome.ok())
			}
			require.Equal(t, `----- tests that ran without a setting they asked for: 1 -----
  TestSuite/TestLeaf key="key" value="4" answer="unimplemented"
----- harness settings tokeirad did not honour: 1 -----
  key="key" value="1" answer="unimplemented"
`, outcome.dynamicConfig.Summary())
		})
	}
}

func TestParseSuiteOutputUnreadableLabels(t *testing.T) {
	output := `    setup.go:1: TOKEIRA_CONFIG_TEST_UNHONOURED
    setup.go:2: TOKEIRA_CONFIG_TEST_UNHONOURED test="TestSuite/TestLeaf" key="key" value="4" answer="unimplemented"
    setup.go:3: TOKEIRA_CONFIG_HARNESS_UNHONOURED
    setup.go:4: TOKEIRA_CONFIG_HARNESS_UNHONOURED key="key" value="1" answer="unimplemented"
    --- PASS: TestSuite/TestLeaf (0.01s)
`
	outcome, err := parseSuiteOutput([]byte(output))
	require.ErrorContains(t, err, "parse test dynamic-config label")
	require.Equal(t, 1, outcome.pass)
	require.True(t, outcome.ok())
	require.Equal(t, `----- tests that ran without a setting they asked for: 1 -----
  TestSuite/TestLeaf key="key" value="4" answer="unimplemented"
----- harness settings tokeirad did not honour: 1 -----
  key="key" value="1" answer="unimplemented"
----- dynamic-config listing incomplete: 2 unreadable lines -----
`, outcome.dynamicConfig.Summary())
}
