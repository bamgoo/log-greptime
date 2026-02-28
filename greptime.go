package log_greptime

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"

	"github.com/bamgoo/bamgoo"
	. "github.com/bamgoo/base"
	blog "github.com/bamgoo/log"
)

type (
	greptimeDriver struct{}

	greptimeConnection struct {
		instance *blog.Instance
		client   *greptime.Client
		setting  greptimeSetting
		levels   map[blog.Level]string
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
	bamgoo.Register("greptime", &greptimeDriver{})
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
	// greptimedb-ingester-go v0.4.x has no client Close method.
	// We only use non-stream writes, so releasing the reference is enough.
	c.client = nil
	return nil
}

func (c *greptimeConnection) Write(logs ...blog.Log) error {
	if c.client == nil || c.instance == nil || len(logs) == 0 {
		return nil
	}
	tbl, err := c.newTable()
	if err != nil {
		return err
	}

	for _, entry := range logs {
		level := c.levels[entry.Level]
		if level == "" {
			level = "UNKNOWN"
		}

		ts := c.uniqueTime(entry.Time)
		fields := encodeFields(entry.Fields)
		if err := tbl.AddRow(
			entry.Project,
			entry.Profile,
			entry.Node,
			level,
			int64(entry.Level),
			entry.Body,
			fields,
			ts,
		); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()

	_, err = c.client.Write(ctx, tbl)
	return err
}

func (c *greptimeConnection) newTable() (*table.Table, error) {
	tbl, err := table.New(c.setting.Table)
	if err != nil {
		return nil, err
	}
	_ = tbl.WithSanitate(false)
	if err := tbl.AddTagColumn("project", types.STRING); err != nil {
		return nil, err
	}
	if err := tbl.AddTagColumn("profile", types.STRING); err != nil {
		return nil, err
	}
	if err := tbl.AddTagColumn("node", types.STRING); err != nil {
		return nil, err
	}
	if err := tbl.AddFieldColumn("level", types.STRING); err != nil {
		return nil, err
	}
	if err := tbl.AddFieldColumn("level_code", types.INT64); err != nil {
		return nil, err
	}
	if err := tbl.AddFieldColumn("body", types.STRING); err != nil {
		return nil, err
	}
	if err := tbl.AddFieldColumn("fields", types.STRING); err != nil {
		return nil, err
	}
	if err := tbl.AddTimestampColumn("time", types.TIMESTAMP_NANOSECOND); err != nil {
		return nil, err
	}
	return tbl, nil
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

func (c *greptimeConnection) uniqueTime(t time.Time) time.Time {
	ns := t.UnixNano()
	c.tsMutex.Lock()
	if ns <= c.lastTsNs {
		ns = c.lastTsNs + 1
	}
	c.lastTsNs = ns
	c.tsMutex.Unlock()
	return time.Unix(0, ns)
}

func encodeFields(m Map) string {
	if len(m) == 0 {
		return "{}"
	}
	bts, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(bts)
}
