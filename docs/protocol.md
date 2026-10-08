# 请求传输协议 v1

## HTTP

`POST /dsh-transport/v1`，`Content-Type: application/vnd.dsh-request-transport.v1`，`Content-Encoding: gzip`。外层 Bearer 使用服务端认可的身份；内置网关可要求它与内部模型 API key 相同，独立中转使用额外的随机 token。

解压后依次是 4 字节大端无符号元数据长度、UTF-8 JSON 元数据、二进制新增内容。元数据最多 32768 字节，还原体最多 33554432 字节。发送端不修改原请求体字节。

| 字段 | 含义 |
| --- | --- |
| `v` | 整数 `1` |
| `url` | 原模型请求的绝对 URL，服务端须与固定允许的目标匹配 |
| `provider`、`model` | DSH 的原 provider 与模型 ID |
| `session` | 完整会话 ID 的小写 SHA-256，缺失时为 `null` |
| `headers` | 原请求头的名称和值，名称小写 |
| `kind` | `full` 或 `delta` |
| `target`、`length` | 完整原请求体的 SHA-256 和字节长度 |
| `base` | 增量引用的完整基线 SHA-256 |
| `prefix`、`suffix` | 从基线复用的前缀和后缀字节数，不得重叠 |
| `resync` | 可选，客户端在收到安全未命中信号后补传全量的标记 |

全量 payload 就是原请求体；增量还原为 `base[0:prefix] + payload + base[base.length-suffix:]`。检查边界、精确长度、gzip 完整性和 SHA-256 后才能转发。元数据中的模型名称只用于缓存隔离，模型权限仍须以还原后的完整 JSON 为准。

服务端的缓存 namespace 至少包含已鉴权身份、上游凭据、完整会话、provider、模型和目标端点。本实现绑定全部原请求头，避免遗漏租户头。会话 ID 的散列仅用于隔离，不作为鉴权凭据，也不表示匿名化隐私保证。

## 响应与重试

完整还原体成功保存后，响应可携带 `X-Dsh-Transport-Ack: <target>`，以及 `X-Dsh-Transport-Version: 1` 和 `X-Dsh-Transport-Mode: full|delta`。ACK 只表示缓存存在，不表示模型成功生成。

若基线缺失且尚未调用上游，返回 `409`、`X-Dsh-Transport-Version: 1`、`X-Dsh-Transport-Retry: full`。客户端仅对此组合补传一次全量 gzip，并携带 `resync: true`。其余状态、断连和超时不由传输插件重试。服务端必须清除上游返回的所有 `X-Dsh-Transport-*` 头，防止上游错误被误判为安全补传信号。

原响应状态和正文流式透传，清除逐跳头及已被底层解压器处理的长度／编码头，不缓存响应。终端取消须传到上游。

## 计量

`originalBytes` 为完整还原体长度，`reusedBytes` 为复用的前后缀总长度，`newBytes` 为 payload 长度，三者满足 `originalBytes = reusedBytes + newBytes`。`frameBytes` 是 gzip 前包含 4 字节长度和元数据的帧长；`wireBytes` 为实际 gzip 体长度，不含 HTTP、TLS 或链路开销。

API 复用率为 `reusedBytes/originalBytes`；gzip 收益为 `1-wireBytes/frameBytes`；整体上行节省为 `1-wireBytes/originalBytes`。小请求可能因为协议元数据而出现负节省，需要如实显示。完整响应的每行计量只包含对应 HTTP 请求，若另报多次尝试总量，需要明确标为客户端累计。

## 资源与生命周期

默认 TTL 为 1 小时，必须同时限制总缓存字节数、条数、还原体大小和并发解码数。缓存可使用内存或权限为 0600 的临时文件，过期与逐出必须实际释放资源。缓存重启丢失、负载均衡切换和并发逐出允许产生未命中。不得为了缓存命中而跳过本次鉴权、权限校验或完整性校验。
