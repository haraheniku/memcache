package memcache

import (
	"testing"
)

func BenchmarkGet(b *testing.B) {
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

func BenchmarkParallelGet(b *testing.B) {
	mc := New("127.0.0.1:11211")
	if err := mc.Set(&Item{
		Key:   "hoge",
		Value: []byte("hoge"),
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(b *testing.PB) {
		for b.Next() {
			mc.Get("hoge")
		}
	})
}
