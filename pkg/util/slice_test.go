package util_test

import (
	"testing"

	"mtuned/pkg/util"
)

func TestSliceContains(t *testing.T) {
	s1, s2, s3 := []int{0, 1, -222, 8734}, []interface{}{}, -9
	e1, e2, e3 := "test", 1, 888

	if util.SliceContains(s1, e1) {
		t.Errorf("if %v contains %v: want false; got true", s1, e1)
	}

	if !util.SliceContains(s1, e2) {
		t.Errorf("if %v contains %v: want true; got false", s1, e2)
	}

	if util.SliceContains(s2, e3) {
		t.Errorf("if %v contains %v: want false; got true", s2, e3)
	}

	if util.SliceContains(s3, e3) {
		t.Errorf("if %v contains %v: want false; got true", s3, e3)
	}
}
