package util_test

import (
	"mtuned/pkg/util"
	"testing"
)

func TestParseBool(t *testing.T) {
	trueValue, falseValue := true, false
	strs := []string{"nil", "", "on", "ON", "1", "off", "OFF", "false", "true"}
	expects := []*bool{nil, nil, &trueValue, &trueValue, &trueValue, &falseValue, &falseValue, &falseValue, &trueValue}

	for i := range strs {
		got := util.ParseBool(strs[i])
		if !((expects[i] == nil && got == nil) || (expects[i] != nil && got != nil && *expects[i] == *got)) {
			t.Errorf("parsing %s, want: %v, got: %#v", strs[i], expects[i], got)
		}
	}
}
