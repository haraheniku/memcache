package memcache

import (
	"testing"
)

func BenchmarkSet(b *testing.B) {
	mc := New("127.0.0.1:11211")
	if err := mc.Set(&Item{
		Key:   "hoge",
		Value: []byte("hoge"),
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc.Get("hoge")
	}
}
