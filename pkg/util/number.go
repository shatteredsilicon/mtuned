package util

import "fmt"

const (
	// MaxUint64 Max uint64 value
	MaxUint64 = ^uint64(0)
)

const (
	_ = iota
	// KB kilobyte
	KB = 1 << (10 * iota)
	// MB megabyte
	MB
	// GB gigabyte
	GB
)

// NextUint64Multiple returns next multiple of 'base'
// that greater than or equal to 'current'
func NextUint64Multiple(current, base uint64) uint64 {
	if base == 0 {
		return 0
	}

	if current%base == 0 {
		return current
	}

	return (current - current%base) + base
}

// LastUint64Multiple returns last multiple of 'base'
// that smaller than or equal to 'current'
func LastUint64Multiple(current, base uint64) uint64 {
	if base == 0 || current == 0 {
		return 0
	}

	if base%current == 0 {
		return current
	}

	return current - current%base
}

// NextPowerOfTwo returns next power of 2
// that greater than 'current', if next power
// of 2 exceeds uint64, it returns 0
func NextPowerOfTwo(current uint64) uint64 {
	for i := 0; i < 64; i++ {
		power := uint64(1) << i
		if power > current {
			return power
		}
	}

	return 0
}

// Uint64ToSizeString round down number to nearest
// GB/MB/KB/B and returns the readable string
func Uint64ToSizeString(number uint64) string {
	if number >= GB {
		return fmt.Sprintf("%dG", number/GB)
	} else if number >= MB {
		return fmt.Sprintf("%dM", number/MB)
	} else if number >= KB {
		return fmt.Sprintf("%dK", number/KB)
	} else {
		return fmt.Sprintf("%dB", number)
	}
}
