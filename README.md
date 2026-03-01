# log-greptime

`log-greptime` 是 `log` 模块的 `greptime` 驱动。

## 安装

```bash
go get github.com/infrago/log@latest
go get github.com/infrago/log-greptime@latest
```

## 接入

```go
import (
    _ "github.com/infrago/log"
    _ "github.com/infrago/log-greptime"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[log]
driver = "greptime"
```

## 公开 API（摘自源码）

- `func (d *greptimeDriver) Connect(inst *blog.Instance) (blog.Connection, error)`
- `func (c *greptimeConnection) Open() error`
- `func (c *greptimeConnection) Close() error`
- `func (c *greptimeConnection) Write(logs ...blog.Log) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
