package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceAlloc(t *testing.T) {
	sa := &sliceAlloc{}
	sa.Make(100)
	assert.Equal(t, 1, sa.allocs)
	sa.Make(1024)
	assert.Equal(t, 2, sa.allocs)
	for i := 0; i < 100; i++ {
		sa.Make(100)
	}
	assert.Equal(t, 3, sa.allocs)
}
