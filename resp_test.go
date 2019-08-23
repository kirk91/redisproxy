package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewError(t *testing.T) {
	v := newError("unknown error")
	assert.Equal(t, Error, v.Type)
	assert.NotEmpty(t, v.Text)
}

func TestNewSimpleString(t *testing.T) {
	v := newSimpleString("ping")
	assert.Equal(t, SimpleString, v.Type)
	assert.NotEmpty(t, v.Text)
}

func TestNewBulkString(t *testing.T) {
	v := newBulkString("get")
	assert.Equal(t, BulkString, v.Type)
	assert.NotEmpty(t, v.Text)
}

func TestNewNullBulkString(t *testing.T) {
	v := newNullBulkString()
	assert.Equal(t, BulkString, v.Type)
	assert.Nil(t, v.Text)
}

func TestNewInteger(t *testing.T) {
	v := newInteger(10)
	assert.Equal(t, Integer, v.Type)
	assert.Equal(t, int64(10), v.Int)
}

func TestNewArray(t *testing.T) {
	v := newArray([]RespValue{
		*newBulkString("get"),
		*newBulkString("a"),
	})
	assert.Equal(t, Array, v.Type)
	assert.Equal(t, 2, len(v.Array))
}
