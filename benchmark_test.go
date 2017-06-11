package memcache

import (
	"testing"
)

func BenchmarkSet(b *testing.B) {
	conn, err := Connect("tcp", "127.0.0.1:11211")
	if err != nil {
		b.Fatal(err)
	}
	if err := conn.Set(&Item{
		Key:   "hoge",
		Value: []byte("hoge"),
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Get("hoge")
	}
}
