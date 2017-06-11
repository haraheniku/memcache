package memcache

import (
	"reflect"
	"testing"
)

func TestMemache(t *testing.T) {
	conn, err := Connect("tcp", "127.0.0.1:11211")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Set(&Item{
		Key:   "hoge",
		Value: []byte("hoge"),
	}); err != nil {
		t.Fatalf("got error on set; %v", err)
	}

	item, err := conn.Get("hoge")
	if err != nil {
		t.Fatalf("got error on get; %v", err)
	}
	if item.Key != "hoge" {
		t.Errorf("expected %q but got %q", "hoge", item.Key)
	}
	if expected := []byte("hoge"); !reflect.DeepEqual(expected, item.Value) {
		t.Errorf("expected %#v but got %#v", expected, item.Value)
	}
}
