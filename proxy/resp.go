package proxy

type RespType byte

const (
	SimpleString RespType = '+'
	Error        RespType = '-'
	Integer      RespType = ':'
	BulkString   RespType = '$'
	Array        RespType = '*'
)

type RespValue struct {
	Type RespType

	Int   int64
	Text  []byte
	Array []RespValue
}

var (
	invalidRequest = "invalid rawRequest"
)

func newError(s string) *RespValue {
	return &RespValue{
		Type: Error,
		Text: []byte(s),
	}
}

func newSimpleString(s string) *RespValue {
	return &RespValue{
		Type: SimpleString,
		Text: []byte(s),
	}
}

func newBulkString(s string) *RespValue {
	return &RespValue{
		Type: BulkString,
		Text: []byte(s),
	}
}

func newNullBulkString() *RespValue {
	return &RespValue{Type: BulkString}
}

func newInteger(i int64) *RespValue {
	return &RespValue{
		Type: Integer,
		Int:  i,
	}
}

func newArray(array []RespValue) *RespValue {
	return &RespValue{
		Type:  Array,
		Array: array,
	}
}
