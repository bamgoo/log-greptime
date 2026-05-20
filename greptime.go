package log_greptime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gpb "github.com/GreptimeTeam/greptime-proto/go/greptime/v1"
	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
	blog "github.com/infrago/log"
)

type (
	greptimeDriver struct{}

	greptimeConnection struct {
		mutex    sync.RWMutex
		instance *blog.Instance
		client   *greptime.Client
		setting  greptimeSetting
		levels   map[blog.Level]string
		schema   []*gpb.ColumnSchema
		tsMutex  sync.Mutex
		lastTsNs int64
	}

	greptimeSetting struct {
		Host     string
		Port     int
		Username string
		Password string
		Database string
		Table    string
		Timeout  time.Duration
		Insecure bool
	}
)

func init() {
	infra.Register("greptime", &greptimeDriver{})
}

func (d *greptimeDriver) Connect(inst *blog.Instance) (blog.Connection, error) {
	setting := greptimeSetting{
		Host:     "127.0.0.1",
		Port:     4001,
		Database: "public",
		Table:    "logs",
		Timeout:  5 * time.Second,
		Insecure: true,
	}

	if inst != nil {
		if v, ok := getString(inst.Setting, "host"); ok && v != "" {
			setting.Host = v
		}
		if v, ok := getString(inst.Setting, "server"); ok && v != "" {
			setting.Host = v
		}
		if v, ok := getInt(inst.Setting, "port"); ok && v > 0 {
			setting.Port = v
		}
		if v, ok := getString(inst.Setting, "username"); ok {
			setting.Username = v
		}
		if v, ok := getString(inst.Setting, "user"); ok && setting.Username == "" {
			setting.Username = v
		}
		if v, ok := getString(inst.Setting, "password"); ok {
			setting.Password = v
		}
		if v, ok := getString(inst.Setting, "pass"); ok && setting.Password == "" {
			setting.Password = v
		}
		if v, ok := getString(inst.Setting, "database"); ok && v != "" {
			setting.Database = v
		}
		if v, ok := getString(inst.Setting, "db"); ok && v != "" {
			setting.Database = v
		}
		if v, ok := getString(inst.Setting, "table"); ok && v != "" {
			setting.Table = v
		}
		if v, ok := getDuration(inst.Setting, "timeout"); ok && v > 0 {
			setting.Timeout = v
		}
		if v, ok := getBool(inst.Setting, "insecure"); ok {
			setting.Insecure = v
		}
		if v, ok := getBool(inst.Setting, "tls"); ok {
			setting.Insecure = !v
		}
	}

	return &greptimeConnection{
		instance: inst,
		setting:  setting,
		levels:   blog.Levels(),
		schema:   newLogSchema(),
	}, nil
}

func (c *greptimeConnection) Open() error {
	cfg := greptime.NewConfig(c.setting.Host).
		WithPort(c.setting.Port).
		WithDatabase(c.setting.Database).
		WithAuth(c.setting.Username, c.setting.Password).
		WithInsecure(c.setting.Insecure)

	client, err := greptime.NewClient(cfg)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *greptimeConnection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// greptimedb-ingester-go v0.4.x has no client Close method.
	// We only use non-stream writes, so releasing the reference is enough.
	c.client = nil
	return nil
}

func (c *greptimeConnection) Write(logs ...blog.Log) error {
	c.mutex.RLock()
	client := c.client
	inst := c.instance
	c.mutex.RUnlock()

	if client == nil || inst == nil || len(logs) == 0 {
		return nil
	}
	tbl, err := c.newTable()
	if err != nil {
		return err
	}
	var writeErr error
	c.tsMutex.Lock()
	for _, entry := range logs {
		level := c.levels[entry.Level]
		if level == "" {
			level = "UNKNOWN"
		}

		fields := encodeFields(entry.Fields)
		ts := c.uniqueTimeLocked(entry.Time)
		if err = tbl.AddRow(
			entry.Project,
			entry.Role,
			entry.Profile,
			entry.Node,
			level,
			int64(entry.Level),
			entry.Body,
			fields,
			ts,
		); err != nil {
			writeErr = err
			break
		}
	}
	c.tsMutex.Unlock()
	if writeErr != nil {
		return writeErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()

	_, err = client.Write(ctx, tbl)
	return err
}

func (c *greptimeConnection) newTable() (*table.Table, error) {
	tbl, err := table.New(c.setting.Table)
	if err != nil {
		return nil, err
	}
	if len(c.schema) == 0 {
		c.schema = newLogSchema()
	}
	_ = tbl.WithSanitate(false)
	tbl.WithColumnsSchema(c.schema)
	return tbl, nil
}

func newLogSchema() []*gpb.ColumnSchema {
	return []*gpb.ColumnSchema{
		{ColumnName: "project", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "role", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "profile", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "node", SemanticType: gpb.SemanticType_TAG, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "level", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "level_code", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_INT64},
		{ColumnName: "body", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "fields", SemanticType: gpb.SemanticType_FIELD, Datatype: gpb.ColumnDataType_STRING},
		{ColumnName: "time", SemanticType: gpb.SemanticType_TIMESTAMP, Datatype: gpb.ColumnDataType_TIMESTAMP_NANOSECOND},
	}
}

func getString(m Map, key string) (string, bool) {
	if m == nil {
		return "", false
	}
	val, ok := m[key]
	if !ok {
		return "", false
	}
	v, ok := val.(string)
	return v, ok
}

func getInt(m Map, key string) (int, bool) {
	if m == nil {
		return 0, false
	}
	val, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

func getDuration(m Map, key string) (time.Duration, bool) {
	if m == nil {
		return 0, false
	}
	val, ok := m[key]
	if !ok {
		return 0, false
	}
	switch v := val.(type) {
	case time.Duration:
		return v, true
	case int:
		return time.Second * time.Duration(v), true
	case int64:
		return time.Second * time.Duration(v), true
	case float64:
		return time.Second * time.Duration(v), true
	case string:
		d, err := time.ParseDuration(v)
		if err == nil {
			return d, true
		}
	}
	return 0, false
}

func getBool(m Map, key string) (bool, bool) {
	if m == nil {
		return false, false
	}
	val, ok := m[key]
	if !ok {
		return false, false
	}
	v, ok := val.(bool)
	return v, ok
}

func (c *greptimeConnection) uniqueTimeLocked(t time.Time) time.Time {
	ns := t.UnixNano()
	if ns <= c.lastTsNs {
		ns = c.lastTsNs + 1
	}
	c.lastTsNs = ns
	return time.Unix(0, ns)
}

const emptyFieldsJSON = "{}"

func encodeFields(m Map) string {
	if len(m) == 0 {
		return emptyFieldsJSON
	}
	if encoded, ok := preencodedJSON(m); ok {
		return encoded
	}
	if encoded, ok := encodePrimitiveFields(m); ok {
		return encoded
	}
	bts, err := json.Marshal(m)
	if err != nil {
		fallback := Map{
			"_error": err.Error(),
		}
		for key, value := range m {
			if _, e := json.Marshal(value); e == nil {
				fallback[key] = value
			} else {
				fallback[key] = fmt.Sprint(value)
			}
		}
		bts, err = json.Marshal(fallback)
		if err != nil {
			return `{"_error":"failed to encode fields"}`
		}
	}
	return string(bts)
}

func preencodedJSON(m Map) (string, bool) {
	if len(m) != 1 {
		return "", false
	}
	for key, value := range m {
		if key != "_json" && key != "__json" {
			return "", false
		}
		encoded, ok := value.(string)
		if !ok || !json.Valid([]byte(encoded)) {
			return "", false
		}
		return encoded, true
	}
	return "", false
}

func encodePrimitiveFields(m Map) (string, bool) {
	keys := make([]string, 0, len(m))
	for key, value := range m {
		if !isPrimitiveJSONValue(value) {
			return "", false
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.Grow(len(m) * 16)
	b.WriteByte('{')
	for idx, key := range keys {
		if idx > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(key))
		b.WriteByte(':')
		writePrimitiveJSONValue(&b, m[key])
	}
	b.WriteByte('}')
	return b.String(), true
}

func isPrimitiveJSONValue(value any) bool {
	switch value.(type) {
	case nil, string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	default:
		return false
	}
}

func writePrimitiveJSONValue(b *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		b.WriteString("null")
	case string:
		b.WriteString(strconv.Quote(v))
	case bool:
		b.WriteString(strconv.FormatBool(v))
	case int:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int8:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		b.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		b.WriteString(strconv.FormatInt(v, 10))
	case uint:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint8:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint16:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint32:
		b.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint64:
		b.WriteString(strconv.FormatUint(v, 10))
	case float32:
		b.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
	case float64:
		b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
	}
}
