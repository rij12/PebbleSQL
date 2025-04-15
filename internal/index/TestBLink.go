package blinktree

import (
	"fmt"
	"sync"
	"testing"
)

func TestBlinkTreeOperations(t *testing.T) {
	tree := NewBlinkTree()

	t.Log("\n--- Inserting keys ---")
	entries := []struct {
		key   int
		value string
	}{
		{10, "ten"}, {20, "twenty"}, {30, "thirty"}, {40, "forty"}, {50, "fifty"},
		{60, "sixty"}, {70, "seventy"}, {80, "eighty"}, {90, "ninety"}, {100, "hundred"},
	}
	for _, e := range entries {
		tree.Insert(e.key, []byte(e.value))
	}

	t.Log("\n--- Tree structure after insertions ---")
	tree.Print()

	t.Log("\n--- Searching keys ---")
	for _, e := range entries {
		val, found := tree.Search(e.key)
		if !found {
			t.Errorf("Expected to find key %d, but did not.", e.key)
			continue
		}
		if string(val) != e.value {
			t.Errorf("Key %d: expected value '%s', got '%s'", e.key, e.value, val)
		}
	}

	t.Log("\n--- Searching for a missing key ---")
	if _, found := tree.Search(999); found {
		t.Errorf("Did not expect to find key 999, but it was found.")
	}

	t.Log("\n--- Deleting a key ---")
	tree.Delete(30)
	if _, found := tree.Search(30); found {
		t.Errorf("Expected key 30 to be deleted, but it was found.")
	}
}

func TestBlinkTreeConcurrentInsert(t *testing.T) {
	tree := NewBlinkTree()
	wg := sync.WaitGroup{}
	total := 100

	t.Log("\n--- Concurrent Insertions ---")
	for i := 1; i <= total; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			tree.Insert(k, []byte(fmt.Sprintf("val%d", k)))
		}(i)
	}
	wg.Wait()

	// Verify all inserted keys
	for i := 1; i <= total; i++ {
		val, found := tree.Search(i)
		if !found || string(val) != fmt.Sprintf("val%d", i) {
			t.Errorf("Key %d: expected 'val%d', got '%s', found=%v", i, i, val, found)
		}
	}
}

func ExampleBlinkTree_InsertAndSearch() {
	tree := NewBlinkTree()
	for i := 10; i <= 50; i += 10 {
		tree.Insert(i, []byte(fmt.Sprintf("val%d", i)))
	}
	val, found := tree.Search(30)
	if found {
		fmt.Println("Found:", string(val))
	} else {
		fmt.Println("Not found")
	}
	// Output:
	// Found: val30
}
