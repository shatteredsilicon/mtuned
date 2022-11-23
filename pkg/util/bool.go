package util

import "strings"

// ParseBool tries to parse the str and returns
// its value, returns nil if it's not a bool
func ParseBool(str string) *bool {
	trueValue, falseValue := true, false
	str = strings.ToLower(strings.TrimSpace(str))
	if SliceContains([]string{"1", "on", "true"}, str) {
		return &trueValue
	}
	if SliceContains([]string{"0", "off", "false"}, str) {
		return &falseValue
	}
	return nil
}
