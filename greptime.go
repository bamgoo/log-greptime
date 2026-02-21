package log_greptime

import (
	"context"
	"strconv"
	"strings"
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

func (d *greptimeDriver) Connect(inst *blog.Instance) (blog.Connect, error) {
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
	return nil
}

func (c *greptimeConnection) Write(logs ...blog.Log) error {
	if c.client == nil || len(logs) == 0 {
		return nil
	}

	tbl, err := table.New(c.setting.Table)
	if err != nil {
		return err
	}
	_ = tbl.WithSanitate(false)

	if err := tbl.AddTagColumn("instance", types.STRING); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("level", types.STRING); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("level_num", types.INT64); err != nil {
		return err
	}
	if err := tbl.AddFieldColumn("body", types.STRING); err != nil {
		return err
	}
	if err := tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND); err != nil {
		return err
	}

	levelNames := blog.Levels()
	for _, entry := range logs {
		level := levelNames[entry.Level]
		if level == "" {
			level = "UNKNOWN"
		}

		if err := tbl.AddRow(
			c.instance.Name,
			level,
			int64(entry.Level),
			entry.Body,
			entry.Time,
		); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()

	_, err = c.client.Write(ctx, tbl)
	return err
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
