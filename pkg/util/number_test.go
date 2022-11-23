package util_test

import (
	"mtuned/pkg/util"
	"testing"
)

func TestNextUint64Multiple(t *testing.T) {
	numbers := [][2]uint64{
		{756, 1024},
		{356, 64},
		{0, 0},
		{512, 512},
	}
	expects := []uint64{1024, 384, 0, 512}

	for i := range numbers {
		got := util.NextUint64Multiple(numbers[i][0], numbers[i][1])
		if expects[i] != got {
			t.Errorf("next multiple of %d that greater than or equal to %d: want: %d, got: %d", numbers[i][1], numbers[i][0], expects[i], got)
		}
	}
}

func TestLastUint64Multiple(t *testing.T) {
	numbers := [][2]uint64{
		{756, 1024},
		{356, 64},
		{0, 0},
		{512, 512},
		{32, 64},
	}
	expects := []uint64{0, 320, 0, 512, 32}

	for i := range numbers {
		got := util.LastUint64Multiple(numbers[i][0], numbers[i][1])
		if expects[i] != got {
			t.Errorf("last multiple of %d that smaller than or equal to %d: want %d, got %d", numbers[i][1], numbers[i][0], expects[i], got)
		}
	}
}

func TestNextPowerOfTwo(t *testing.T) {
	numbers := []uint64{0, 1, 15, 64, 1 << 63, util.MaxUint64}
	expects := []uint64{1, 2, 16, 128, 0, 0}

	for i := range numbers {
		got := util.NextPowerOfTwo(numbers[i])
		if expects[i] != got {
			t.Errorf("next power of 2 that greater than %d: want %d, got %d", numbers[i], expects[i], got)
		}
	}
}
