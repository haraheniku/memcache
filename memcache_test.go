package memcache

import (
	"reflect"
	"testing"
)

func TestMemache(t *testing.T) {
	cli := New("127.0.0.1:11211")
	if err := cli.Set(&Item{
		Key:   "hoge",
		Value: []byte("hoge"),
	}); err != nil {
		t.Fatalf("got error on set; %v", err)
	}

	item, err := cli.Get("hoge")
	if err != nil {
		t.Fatalf("got error on get; %v", err)
	}
	if item.Key != "hoge" {
		t.Errorf("expected %q but got %q", "hoge", item.Key)
	}
	if expected := []byte("hoge"); !reflect.DeepEqual(expected, item.Value) {
		t.Errorf("expected %#v but got %#v", expected, item.Value)
	}

	items, err := cli.GetMulti([]string{"hoge", "niku", "fuga"})
	if err != nil {
		t.Errorf("got error on get_mutli; %v", err)
	}
	for _, x := range []struct {
		key   string
		value []byte
	}{
		{"hoge", []byte("hoge")},
	} {
		item := items[x.key]
		if item == nil {
			t.Errorf("got nil; %v", x)
		}
		if item.Key != x.key || !reflect.DeepEqual(x.value, item.Value) {
			t.Errorf("expected %v but got %s, %v", x, item.Key, item.Value)
		}
	}
}
