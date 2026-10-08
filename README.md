# DSH Request Transport

为 **DeepSeek Harness 0.2.0-rc.2** 减少模型请求的上行数据：首次发送全量 gzip，后续引用已缓存的历史字节，再将新增部分 gzip。服务端逐字节还原原请求后调用模型，上游收到的上下文和 token 计费不变。

插件按 provider 显式启用，安装时所有路由默认关闭。支持标准的 OpenAI Chat Completions、Responses 和 Anthropic Messages HTTP 路径。需要 Node.js 22 或更高版本。

## 安装到 DSH

```bash
dsh plugin --profile web add github:fengzee/dsh-request-transport#v0.1.0
```

在 `$DSH_HOME/profiles/web/cordis.patch.yml` 中增加或修改以下条目。已有 YAML 数组为空 `[]` 时，将它替换成条目数组；已有内容时保留其它条目。`provider` 必须是 DSH 的 provider ID。

```yaml
- id: request-transport
  config:
    routes:
      - provider: fengzee-llm
        upstream: https://llm.fengzee.me/v1
        relay: https://llm.fengzee.me/v1/request-transport
        auth: api-key
        delta: true
    cacheTtlMs: 3600000
```

`auth: api-key` 直接复用模型请求中已有的 API key，不另存密钥，也不改 DSH 的模型、凭据或默认模型。网关必须实现本仓库的协议；普通 OpenAI 兼容端点不能直接接收这种帧。

等待活跃请求结束后重启对应 DSH 宿主。宿主插件需要重新加载，单独刷新浏览器不足以替换已加载的 Node 模块。若实例由部署工具管理，应在其配置正本中安装和启用，再通过原有流程收敛。headless 等其它 profile 需要分别安装和配置。

同一路由不要同时开启其它请求压缩或预传输插件。已有压缩请求会明确报错，避免重复编码。未启用的 provider、普通网页请求、模型发现、文件上传和带签名的请求继续走原路径。

## 如何确认生效

在同一会话连续发送两轮请求，检查 LLM 网关的传输指标或响应头：

- `X-Dsh-Transport-Mode: full` 表示全量 gzip。
- `X-Dsh-Transport-Mode: delta` 表示历史复用与 gzip 同时生效。
- `X-Dsh-Transport-Ack` 是服务端确认保存的完整请求体 SHA-256。

Fengzee LLM 面板的「API 复用 / 实传」展示复用量、新增内容和 gzip 后实际上行量。「模型缓存读 / 写」仍然表示模型侧 token 缓存。网关的字节数计当前被接受的 HTTP 请求，缓存失效前的未命中尝试单独属于一次传输尝试；客户端内部指标的 `uploadedBytes` 则包含本次安全补传前后的总量。

首次请求主要从 gzip 获益；已有压缩图片的 base64 内容通常收益较小。后续请求若共享大量历史，可以从增量复用获益。每次会比较压缩后的全量帧和增量帧，只有增量至少小 64 字节才使用它。正文开头多处变化时，复用率可能下降。

## 缓存正确性

缓存按完整会话 ID、provider、模型、目标 URL 和原 HTTP 请求头隔离，头部包含上游凭据。父会话和每个子会话各自独立；相同会话的并发、分叉、编辑和上下文压缩通过不可变内容哈希定位基线。

服务端必须校验完整长度与 SHA-256。只有明确在调用上游之前发现缓存缺失，才返回带协议标记的 409，插件补传一次全量 gzip。插件不会在断网或普通 5xx 后自行重放模型请求；DSH 和 SDK 自带的重试策略保持原样。

客户端默认最多保留 128 MiB、128 个会话基线，TTL 为 1 小时；没有会话 ID 时仅 gzip。重启、热重载、逐出和服务端切换都可能导致全量补传，不会改变模型输入。当前差分复用相同前缀和后缀，中间部分原样传送，支持二进制 UTF-8 边界，无须重排或重新序列化 JSON。

DSH 0.2.0-rc.2 没有适用于这两个内置适配器的公开自定义 fetch 配置，因此插件使用公开的 `llm/stream` waterfall 标记异步调用范围，并在插件生命周期内包装 `globalThis.fetch`。仅有匹配范围的请求会改变；卸载会恢复或停用自己的包装。升级 DSH 后请重新验证适配器链路。

## 自建配套中转

生产网关可按 [协议文档](docs/protocol.md) 内置解码。Fengzee LLM 使用 Go 流式解压和磁盘缓存，保留既有模型权限与计费流程。仓库也提供一个 Node 参考中转，适合已有网关前方的小规模部署：

```bash
git clone --branch v0.1.0 https://github.com/fengzee/dsh-request-transport.git
cd dsh-request-transport
npm ci --omit=dev --ignore-scripts
# 通过部署环境注入 DSH_TRANSPORT_TOKEN，至少 32 字节随机值，不写入仓库。
UPSTREAM_BASE_URL=https://your-api.example/v1 npm run relay
```

默认监听 `127.0.0.1:8788`，在服务端用 HTTPS 反向代理公开 `/v1/request-transport`。应将它放在慢上行链路的远端，放在客户端同一台机器无法节省这段上行流量。该服务持有解开的请求内容与上游凭据，应部署在自己信任的主机上。

客户端对应路由使用 `auth: relay-token`、`tokenEnv: DSH_TRANSPORT_TOKEN`，上游 API key 继续由 DSH 管理。参考中转只允许配置的一个上游及三个标准模型路径，先校验中转 token，再解压；不跟随上游重定向。`/healthz` 可用于存活探针。

参考中转默认使用 128 MiB、128 条、1 小时的进程内缓存，请求上限 32 MiB，同时最多处理两条请求。它会缓冲有上限的请求帧与还原体，响应流式透传；与 Fengzee 的 Go 网关实现有不同的内存需求。可通过 `CACHE_BYTES`、`CACHE_ENTRIES`、`CACHE_TTL_MS`、`MAX_BODY_BYTES`、`MAX_CONCURRENT`、`HOST` 和 `PORT` 调整。禁止在没有完整校验和容量约束的情况下直接转发增量内容。

## 测试

```bash
npm ci --ignore-scripts
npm test
# DSH_APP 指向装有目标 DSH 依赖的目录；测试仅调用本机合成上游。
DSH_APP=/path/to/dsh/app npm run test:dsh
```

覆盖逐字节还原、Unicode、API 路径、父子与兄弟会话、凭据隔离、同会话并发、缓存逐出与 TTL、完整性和大小校验、安全补传、网络错误不重放，以及真实 DSH `LlmRuntime → PiAiAdapter → HTTP` 链路。`test/protocol-v1.json` 是跨语言协议夹具。

## 仓库维护

开发以 [自建 Git 仓库](https://git.fengzee.me/dsh-request-transport.git) 为主，[GitHub](https://github.com/fengzee/dsh-request-transport) 保留公开镜像，供安装和浏览源码。`origin` 从自建端拉取，一次推送依次同步自建端和 GitHub。新开发副本配置如下：

```bash
git remote set-url origin https://git.fengzee.me/dsh-request-transport.git
git config --replace-all remote.origin.pushurl https://git.fengzee.me/dsh-request-transport.git
git config --add remote.origin.pushurl https://github.com/fengzee/dsh-request-transport.git
git push origin main
# 发布新版本时，同步该版本标签到两端。
git push origin <版本标签>
```

GitHub Actions 已在仓库设置中关闭，仓库不保留自动触发的工作流。代码变更在本机运行上述测试后提交；推送后用 `git ls-remote` 核对两端主分支和发布标签。

## 社区相关实现

[HolynnChen/dsh-plugin-model-request-accelerator](https://github.com/HolynnChen/dsh-plugin-model-request-accelerator) 已提供请求压缩、HTTP/2、提前发送历史和耗时显示。其预传输通过提前打开并部分写入请求来减少等待，仍然上传这些历史字节。本项目增加需要服务端配合的历史字节缓存与增量还原。两者思路不同，本仓库为独立实现。

社区的 [dsh-gzip](https://github.com/040822/dsh-gzip) 主要压缩 DSH 网页 API 响应，与模型请求上行不同。

MIT License。
