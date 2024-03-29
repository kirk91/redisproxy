package proxy

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tmap = make(map[int64][]byte)

func init() {
	var n = len(itoaOffset)*2 + 100000
	for i := -n; i <= n; i++ {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
	for i := math.MinInt64; i != 0; i = int(float64(i) / 1.1) {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
	for i := math.MaxInt64; i != 0; i = int(float64(i) / 1.1) {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
}

func TestItoa(t *testing.T) {
	for i, b := range tmap {
		assert.Equal(t, itoa(i), string(b))
	}
	for i := int64(minItoa); i <= maxItoa; i++ {
		assert.Equal(t, itoa(i), strconv.Itoa(int(i)))
	}
}

func TestEncodeSimpleString(t *testing.T) {
	v := newSimpleString("OK")
	testEncodeAndCheck(t, v, []byte("+OK\r\n"))
}

func TestEncodeError(t *testing.T) {
	v := newError("Error")
	testEncodeAndCheck(t, v, []byte("-Error\r\n"))
}

func TestEncodeInteger(t *testing.T) {
	for _, i := range []int{-1, 0, 1024 * 1024} {
		v := newInteger(int64(i))
		testEncodeAndCheck(t, v, []byte(fmt.Sprintf(":%d\r\n", i)))
	}
}

func TestEncodeBulkString(t *testing.T) {
	v := &RespValue{Type: BulkString}
	testEncodeAndCheck(t, v, []byte("$-1\r\n"))
	v.Text = []byte{}
	testEncodeAndCheck(t, v, []byte("$0\r\n\r\n"))
	v.Text = []byte("helloworld!!")
	testEncodeAndCheck(t, v, []byte("$12\r\nhelloworld!!\r\n"))
}

func TestEncodeArray(t *testing.T) {
	v := newArray(nil)
	testEncodeAndCheck(t, v, []byte("*-1\r\n"))
	v.Array = []RespValue{}
	testEncodeAndCheck(t, v, []byte("*0\r\n"))
	v.Array = append(v.Array, *newInteger(0))
	testEncodeAndCheck(t, v, []byte("*1\r\n:0\r\n"))
	v.Array = append(v.Array, *newNullBulkString())
	testEncodeAndCheck(t, v, []byte("*2\r\n:0\r\n$-1\r\n"))
	v.Array = append(v.Array, *newBulkString("test"))
	testEncodeAndCheck(t, v, []byte("*3\r\n:0\r\n$-1\r\n$4\r\ntest\r\n"))
}

func encode(v *RespValue) []byte {
	var b bytes.Buffer
	enc := newEncoder(&b, 8192)
	enc.Encode(v)
	enc.Flush()
	return b.Bytes()
}

func testEncodeAndCheck(t *testing.T, v *RespValue, expect []byte) {
	t.Helper()
	b := encode(v)
	assert.Equal(t, expect, b)
}

func newBenchmarkEncoder(n int) *encoder {
	return newEncoder(ioutil.Discard, 1024*128)
}

func benchmarkEncode(b *testing.B, n int) {
	v := &RespValue{
		Type: BulkString,
		Text: make([]byte, n),
	}
	e := newBenchmarkEncoder(n)
	for i := 0; i < b.N; i++ {
		assert.Nil(b, e.encode(v))
	}
	assert.Nil(b, e.Flush())
}

func BenchmarkEncode(b *testing.B) {
	tests := []struct {
		name     string
		dataSize int
	}{
		{"16B", 16},
		{"64B", 64},
		{"512B", 512},
		{"1KB", 1024},
		{"2KB", 1024 * 2},
		{"4KB", 1024 * 4},
		{"16KB", 1024 * 16},
		{"32KB", 1024 * 32},
		{"128KB", 1024 * 128},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			benchmarkEncode(b, test.dataSize)
		})
	}
}

func TestBtoi64(t *testing.T) {
	assert := assert.New(t)
	for i, b := range tmap {
		v, err := btoi64(b)
		assert.Nil(err)
		assert.Equal(v, i)
	}
}

func TestDecodeInvalidData(t *testing.T) {
	test := []string{
		"*hello\r\n",
		"*-100\r\n",
		"*3\r\nhi",
		"*3\r\nhi\r\n",
		"*4\r\n$1",
		"*4\r\n$1\r",
		"*4\r\n$1\n",
		"*2\r\n$3\r\nget\r\n$what?\r\nx\r\n",
		"*4\r\n$3\r\nget\r\n$1\r\nx\r\n",
		"*2\r\n$3\r\nget\r\n$1\r\nx",
		"*2\r\n$3\r\nget\r\n$1\r\nx\r",
		"*2\r\n$3\r\nget\r\n$100\r\nx\r\n",
		"$6\r\nfoobar\r",
		"$0\rn\r\n",
		"$-1\n",
		"*0",
		"*2n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*-\r\n",
		"+OK\n",
		"-Error message\r",
	}
	for i, s := range test {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			r := bytes.NewReader([]byte(s))
			dec := newDecoder(r, 8192)
			_, err := dec.Decode()
			assert.NotEqual(t, nil, err)
		})
	}
}

func TestDecodeBulkString(t *testing.T) {
	assert := assert.New(t)
	test := "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"
	dec := newDecoder(bytes.NewReader([]byte(test)), 8192)
	v, err := dec.Decode()
	assert.Nil(err)
	assert.Equal(2, len(v.Array))
	s1 := v.Array[0]
	assert.Equal(true, bytes.Equal(s1.Text, []byte("LLEN")))
	s2 := v.Array[1]
	assert.Equal(true, bytes.Equal(s2.Text, []byte("mylist")))
}

func TestDecoder(t *testing.T) {
	test := []string{
		"$6\r\nfoobar\r\n",
		"$0\r\n\r\n",
		"$-1\r\n",
		"*0\r\n",
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*3\r\n:1\r\n:2\r\n:3\r\n",
		"*-1\r\n",
		"+OK\r\n",
		"-Error message\r\n",
		"*2\r\n$1\r\n0\r\n*0\r\n",
		"*3\r\n$4\r\nEVAL\r\n$31\r\nreturn {1,2,{3,'Hello World!'}}\r\n$1\r\n0\r\n",
	}
	for i, s := range test {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			dec := newDecoder(bytes.NewReader([]byte(s)), 8192)
			_, err := dec.Decode()
			assert.Nil(t, err)
		})
	}
}

type loopReader struct {
	buf []byte
	pos int
}

func (b *loopReader) Read(p []byte) (int, error) {
	if b.pos == len(b.buf) {
		b.pos = 0
	}
	n := copy(p, b.buf[b.pos:])
	b.pos += n
	return n, nil
}

func newBenchmarkDecoder(n int) (*decoder, error) {
	v := newArray([]RespValue{
		*newBulkString(string(make([]byte, n))),
	})
	var buf bytes.Buffer
	enc := newEncoder(&buf, 8192)
	enc.Encode(v)
	enc.Flush()
	p := buf.Bytes()

	var b bytes.Buffer
	for i := 0; i < 128 && b.Len() < 1024*1024; i++ {
		_, err := b.Write(p)
		if err != nil {
			return nil, err
		}
	}
	return newDecoder(&loopReader{buf: b.Bytes()}, 1024*128), nil
}

func benchmarkDecode(b *testing.B, n int) {
	d, err := newBenchmarkDecoder(n)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		v, err := d.Decode()
		assert.Nil(b, err)
		assert.Equal(b, true, len(v.Array) == 1 && len(v.Array[0].Text) == n)
	}
}

func BenchmarkDecode(b *testing.B) {
	tests := []struct {
		name     string
		dataSize int
	}{
		{"16B", 16},
		{"64B", 64},
		{"512B", 512},
		{"1KB", 1024},
		{"2KB", 1024 * 2},
		{"4KB", 1024 * 4},
		{"16KB", 1024 * 16},
		{"32KB", 1024 * 32},
		{"128KB", 1024 * 128},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			benchmarkDecode(b, test.dataSize)
		})
	}
}
