package log_greptime

import (
	"strings"
	"testing"

	. "github.com/infrago/base"
)

func TestEncodeFieldsFallsBackForUnmarshalableValues(t *testing.T) {
	got := encodeFields(Map{"bad": func() {}})
	if got == "{}" {
		t.Fatal("expected fields fallback to preserve error context")
	}
	if !strings.Contains(got, `"_error"`) {
		t.Fatalf("expected error marker in fallback payload, got %s", got)
	}
	if !strings.Contains(got, `"bad"`) {
		t.Fatalf("expected field key to be preserved, got %s", got)
	}
}
