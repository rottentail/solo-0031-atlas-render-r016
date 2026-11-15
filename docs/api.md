# HTTP 协议

所有业务路径以 `/v1` 开头。请求体使用 JSON，素材上传直接使用 PNG 字节。

| 方法和路径 | 请求 | 结果 |
|---|---|---|
| PUT /scenes/{id} | name, expectedRevision, document | 完整场景，200 |
| GET /scenes/{id} | 无 | 完整场景，200 |
| POST /scenes/{id}/state | expectedRevision, state | 完整场景，200 |
| POST /scenes/{id}/snapshots | expectedRevision | 快照和素材闭包，201 |
| PUT /assets/{id} | PNG字节 | 素材及base64字节，201 |
| POST /assets/{id}/retire | 无 | state，200 |
| GET /snapshots/{id} | 无 | 快照，200 |
| POST /snapshots/{id}/revoke | 无 | state，200 |
| GET /snapshots/{id}/package | 无 | atlas-scene交换包，200 |
| POST /imports/{id} | name, package | 新场景，201 |
| POST /jobs | snapshotId, scale；请求头Idempotency-Key | 任务，202 |
| GET /jobs/{id} | 无 | 任务，200 |
| GET /idempotency/{key} | 无 | 该键已承诺的任务，200；键未使用为404 |
| POST /jobs/{id}/cancel | 无 | 任务，200 |
| POST /jobs/{id}/retry | 无 | 任务，202 |
| GET /artifacts/{id} | 可选If-None-Match | PNG，200或304 |
| DELETE /artifacts/{id} | 无 | state，200 |
| POST /compare | left, right（结果ID） | 像素比较，200 |
| GET /status | 无 | 场景/素材/快照/任务计数和结果字节数 |
| GET /events | 无 | 最多500个有序领域事件 |

`GET /healthz` 不带 `/v1`，返回ready。它表明进程已打开并恢复状态，不包含单独的磁盘可写探测。任务响应省略固定输入的有效载荷，输入细节通过快照接口读取。

## 错误与提交承诺

所有失败响应仍是 `{"code", "error"}` 形状，并额外带有 `commitment` 字段；同样的值也放在 `X-Atlas-Commitment` 响应头。客户端只能依据它决定是否可以原样重试，不得从错误文本推断。

| commitment | 含义 | 客户端动作 |
|---|---|---|
| not-committed | 持久状态未被替换，收据不存在 | 可用同一幂等键重试 |
| committed | 变更已成为持久事实，收据存在 | 用同一幂等键重放，取得原任务；不要换键 |
| unknown | 无法证明是否落地 | 用同一幂等键重放或读取 `GET /idempotency/{key}`；不要当作新任务提交 |

状态码只表示错误类别：400输入不合法、404实体不存在、409状态或版本冲突、410结果已清理、429预算或队列不足、500内部失败。500可能是 not-committed、committed 或 unknown；客户端先读承诺再决定。磁盘写入在原子替换前失败时为 not-committed，替换后无法确认时为 committed 或 unknown。

`POST /jobs` 成功受理返回 202 且 `commitment: committed`；幂等重放返回同一任务并带 `Idempotent-Replay: true`。连接中断后先 `GET /idempotency/{key}`：404表示从未接收，可以原样重试；200返回已承诺任务，不能新建。

文档字段：width、height、background、layers。每个图层有id、kind、width、height、transform、opacity、fill；transform包括x、y、scaleX、scaleY、degrees。图层可以有clip（x/y/width/height），group使用children，polygon使用points，image使用assetId。solid颜料有color；linear还包括endColor、from、to。示例是可直接提交的完整请求。

幂等键长度1–80，使用英文字母、数字、下划线和连字符，首位不能是后两者。它在服务实例持久状态中全局生效，不随任务完成、取消、结果清理或进程重启失效。改换请求语义需使用新键。

服务接受相同内容的素材重复上传，退役素材不接受重新上传。交换包导出保留快照状态；导入会创建全新的active场景，原快照不作为目标服务中的快照记录落地。导入目标ID必须未使用；共享素材必须内容相同且仍可用。
