package drpc

import (
	"encoding/json"
	"testing"
)

func TestPayloadToJsonWithNestedMapInterface(t *testing.T) {
	payload := NewPayload()
	payload.Param("xxxl", map[interface{}]interface{}{
		"name": "tom",
		"meta": map[interface{}]interface{}{
			"age": int64(18),
		},
		"list": []interface{}{
			map[interface{}]interface{}{"k": "v"},
			"ok",
		},
	})

	result := payload.ToJson()
	if result == "" {
		t.Fatalf("ToJson() should not be empty")
	}

	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(result), &decoded); err != nil {
		t.Fatalf("ToJson() returned invalid json: %v, raw: %s", err, result)
	}

	xxxl, ok := decoded["xxxl"].(map[string]interface{})
	if !ok {
		t.Fatalf("decoded[xxxl] should be map[string]interface{}, got: %#v", decoded["xxxl"])
	}

	meta, ok := xxxl["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("decoded[xxxl][meta] should be map[string]interface{}, got: %#v", xxxl["meta"])
	}
	if got := meta["age"]; got != float64(18) {
		t.Fatalf("decoded age mismatch, want 18 got %#v", got)
	}
}
