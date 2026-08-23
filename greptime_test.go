package log_greptime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	. "github.com/infrago/base"
	blog "github.com/infrago/log"
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

func TestEncodeFieldsUsesPrimitiveFastPath(t *testing.T) {
	got := encodeFields(Map{"b": true, "a": 1})
	if got != `{"a":1,"b":true}` {
		t.Fatalf("expected stable primitive JSON, got %s", got)
	}
}

func TestEncodeFieldsAcceptsPreencodedJSON(t *testing.T) {
	got := encodeFields(Map{"_json": `{"x":1}`})
	if got != `{"x":1}` {
		t.Fatalf("expected preencoded JSON to pass through, got %s", got)
	}
	if !json.Valid([]byte(got)) {
		t.Fatalf("expected valid JSON, got %s", got)
	}
}

func TestUniqueTimeLockedPreservesOrder(t *testing.T) {
	conn := &greptimeConnection{}
	base := time.Unix(1773000000, 10)
	conn.tsMutex.Lock()
	got := []time.Time{
		conn.uniqueTimeLocked(base),
		conn.uniqueTimeLocked(base),
		conn.uniqueTimeLocked(base.Add(-time.Second)),
	}
	conn.tsMutex.Unlock()

	if len(got) != 3 {
		t.Fatalf("expected 3 timestamps, got %d", len(got))
	}
	if !got[0].Equal(base) {
		t.Fatalf("expected first timestamp to be unchanged, got %v", got[0])
	}
	if !got[1].After(got[0]) || !got[2].After(got[1]) {
		t.Fatalf("expected timestamps to be strictly increasing, got %#v", got)
	}
}

func TestNewTableUsesCachedSchema(t *testing.T) {
	conn := &greptimeConnection{setting: greptimeSetting{Table: "logs"}, schema: newLogSchema()}
	tbl, err := conn.newTable()
	if err != nil {
		t.Fatal(err)
	}
	if err := tbl.AddRow("project", "role", "profile", "node", "module", "INFO", int64(blog.LevelInfo), "body", "request-id", "trace-id", "{}", time.Now()); err != nil {
		t.Fatal(err)
	}
}
