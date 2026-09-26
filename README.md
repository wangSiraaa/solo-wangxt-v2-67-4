# protocompat — Protobuf 发布兼容性登记服务

纯后端的 Protobuf 注册与兼容性判定服务。新增字段不等于兼容：发布入口必须给出
**有依据** 的判断。判定基于 protocompile 产出的完整链接描述符闭包（不是源码正则），
同时检查二进制线路编码与 JSON 映射两个维度；**无法证明安全的部分一律标为
`NEEDS_REVIEW`，绝不笼统标成通过**。

## 架构

```
cmd/server        ConnectRPC 服务入口（纯后端，无管理页面）
cmd/compatcheck   命令行：回归用例运行器 + 两棵 proto 树的临时比对
cmd/corpusctl     命令行：语料集创建/封存、发起回放、查询结果、新旧 schema 差异对比
internal/schema   protocompile 封装：编译 → 描述符集 → 内容哈希 → 重新加载
internal/compat   兼容性判定核心（字段号复用 / 保留删除 / 类型变化 / 枚举默认值 / 样例载荷）
internal/corpus   单 schema 批量回放引擎（解码 / 字段丢失 / JSON 行为差异 / 双回放对比）
internal/registry ConnectRPC 处理器、Store 接口、PostgreSQL 实现、内存实现
internal/regress  文件型回归用例运行器（CLI 与 go test 共用）
testdata/cases    回归案例：嵌套导入、oneof 迁移、同名不同包……
api/registry/v1   服务契约（ConnectRPC，当前手写 handler + JSON codec，无需 codegen）
```

- **解析**：`github.com/bufbuild/protocompile`，嵌套导入、菱形导入、well-known
  types 均由编译器解析；判断在 `protoreflect` 描述符上进行。
- **传输**：ConnectRPC（connect 协议 + JSON codec），四个方法：
  `RegisterVersion` / `CheckCompatibility` / `DeclareConsumer` / `ListVersions`。
  另有 `registry.v1.Corpus` 十个方法，负责版本化语料库与批量回放（见下）。
- **存储**：PostgreSQL 存包、不可变版本（描述符集 + 内容哈希）、兼容性报告、
  使用方声明（consumer declarations）。`STORE=memory` 可本地冒烟（不持久化）。

## 判定模型

每条 finding 带 `severity`（FAIL/WARN/INFO）、`dimension`（WIRE/JSON/BOTH）、
`message`（全限定名）与 `path`（字段路径）。整体结论：

| verdict | 含义 |
|---|---|
| `COMPATIBLE` | 所有检查都有证据通过 |
| `NEEDS_REVIEW` | 没有证伪，但至少一项**无法证明**安全（如移入 oneof、取消保留号段） |
| `INCOMPATIBLE` | 至少一项被证明破坏（如字段号复用、JSON 表示变化） |

覆盖的检查（详见 `internal/compat`）：

- **字段号复用**：同号不同名且类型不同 → FAIL；同号不同名同类型 → 无法区分改名与
  换义，wire 维度 WARN、JSON 名变化 FAIL。
- **保留字段删除**：删字段且号+名都保留 → INFO；只保留号 → JSON 维度 WARN；
  什么都不保留 → WIRE 维度 FAIL。取消保留号段 / 复用保留号 → WARN。
- **类型变化**：按 wire 类型分类矩阵（varint/fixed32/fixed64/length-delimited）
  与 JSON 表示分类（number/quoted-number/bool/string/base64/enum-name/object）
  分别定级；packed repeated ↔ singular、map 键值类型、消息类型换绑等均覆盖。
- **枚举默认值**：零值（隐式默认值）改名/删除、proto2 封闭枚举新增值
  （老读者落入 unknown fields）、proto3 开放枚举新增值（INFO）。
- **oneof 迁移**：移入 → WARN（旧载荷可能同时设置互斥字段，JSON 双成员文档解析失败）；
  移出 → INFO；oneof 删除 → WARN。合成 oneof（proto3 optional）不计。
- **样例载荷**：提交者可附 `samples`（json 文本或 base64 wire），服务用新旧描述符
  分别解析并比对确定性 wire 字节与 protojson 输出，差异定位到
  `acme.Msg.lines[2].amount` 这样的字段路径；无法验证的样例记 WARN，不算通过。

## 版本化语料库与批量回放

样例不仅能在发布时做新旧双 schema 比对，还可以沉淀成**按版本管理的语料库**，
之后选择任意一个 schema 版本对整组语料做批量回放。

- **内容寻址**：每个样例在包内按 `(message, encoding, 规范化 payload, 期望)` 的
  SHA-256 摘要唯一标识。JSON 先做结构规范化，所以排版/键序不同的相同内容、以及
  重复上传都不会产生第二条样例。样例保存原始 wire（base64）或 JSON 文本、期望结果
  （`status: ok|decode_error`、期望的规范化 JSON）。
- **语料集版本**：语料按 `(package, corpus, version)` 管理。新版本只能基于一个
  **已封存**版本，用 `add`/`remove` 做增量；只有最新的 draft 可改，`seal` 之后
  永久不可变（更新、删除一律 `failed_precondition`）。删除 draft 只是打
  `deleted` 墓碑：版本号不复用，封存历史和历史回放完全不受影响。
- **回放结果逐项独立**：每个样例在选定 schema 下单独解码，分别汇总
  成功（OK）、解码失败（DECODE_FAILED）、字段丢失（FIELD_LOST，wire 的未知字段号
  和 JSON 的未知键，定位到 `acme.M.lines[2].#5` 这样的路径）、JSON 行为差异
  （JSON_DIFF，提交 JSON 与规范化 protojson 的结构化差异，如 int64 加引号、枚举动名），
  以及期望是否满足（EXPECTATION_MISMATCH）。单个坏样例（坏 base64、消息不存在）
  不会影响其余样例完成。
- **安全重试**：发起回放可带 `replay_key`；缺省时由
  `(包, 语料, 语料版本, schema 版本)` 派生稳定键——同一输入键全局只有一份结果。
  执行采用 `pending → claimed → completed` 状态机加租约（`FOR UPDATE SKIP LOCKED`，
  崩溃后过期租约可被回收）。进程中断重启后从未完成项续跑，已完成项绝不重复执行，
  重复完成同一项会被拒绝。
- **可追溯对比**：同一封存语料对新旧两个 schema 版本各回放一次，`CompareReplays`
  按样例摘要对齐，给出稳定的差异汇总（新增解码失败、新增/消失的丢失字段与 JSON
  差异路径、回归项/修复项）。回放项在启动时快照载荷，因此之后删除 draft 或增删
  样例都不改变历史回放。

接口（`registry.v1.Corpus`）：`CreateCorpus` / `UpdateCorpus` / `SealCorpus` /
`GetCorpus` / `ListCorpora` / `DeleteCorpusDraft` / `StartReplay` / `GetReplay` /
`ListReplays` / `CompareReplays`。

```bash
# 1) 上传样例（数组或 {"samples":[...]}），创建 draft
corpusctl upload -pkg acme.evt -corpus events samples.json
# 基于封存 v1 增量出新 draft：-add/ -remove 用摘要引用既有样例
corpusctl upload -pkg acme.evt -corpus events -base 1 more.json
corpusctl update -pkg acme.evt -corpus events -remove <digest>
corpusctl seal   -pkg acme.evt -corpus events          # 封存后不可变
# 2) 对同一封存语料分别用新旧 schema 回放
corpusctl replay -pkg acme.evt -corpus events -version 1 -schema v1
corpusctl replay -pkg acme.evt -corpus events -version 1 -schema v2
# 3) 稳定可追溯的差异对比
corpusctl compare -pkg acme.evt -old 1 -new 2
corpusctl result  -pkg acme.evt -id 1                  # 查询逐项结果
```

## 使用方声明

消费方声明自己读取的消息/字段与编码（wire/json/both），`CheckCompatibility`
带上 `consumer` 后，报告会投影到该消费方的实际使用面：wire-only 消费者不受
纯 JSON 破坏影响，反之亦然。

## 运行

```bash
docker compose up --build        # PostgreSQL + 服务（:8080）
# 或本地：
STORE=memory go run ./cmd/server # 冒烟用，不持久化
```

注册版本（发布入口，自动与上一版本比对）：

```bash
curl -s localhost:8080/registry.v1.Registry/RegisterVersion -H 'Content-Type: application/json' -d '{
  "package": "acme.pay",
  "version": "v2",
  "files": [{"path": "pay/invoice.proto", "content": "syntax = \"proto3\"; ..."}],
  "samples": [{"message": "acme.pay.Invoice", "encoding": "json", "data": "{\"id\":\"i-1\",\"total\":5}"}],
  "require_compatible": true
}'
```

- 同版本同内容 → 幂等成功（`already_existed: true`）。
- **同版本不同内容 → `already_exists` 错误，拒绝覆盖**。
- `require_compatible: true` 且结论为 `INCOMPATIBLE` → `failed_precondition`，
  版本不落库。

声明消费方并做投影检查：

```bash
curl -s localhost:8080/registry.v1.Registry/DeclareConsumer -H 'Content-Type: application/json' -d '{
  "package": "acme.pay", "consumer": "ledger", "encoding": "wire",
  "usages": [{"message": "acme.pay.Invoice", "fields": ["id"]}]
}'
curl -s localhost:8080/registry.v1.Registry/CheckCompatibility -H 'Content-Type: application/json' -d '{
  "package": "acme.pay", "base_version": "v1", "candidate_version": "v2", "consumer": "ledger"
}'
```

## 命令行

```bash
# 兼容性回归与临时比对
go run ./cmd/compatcheck run testdata/cases     # 全部回归案例
go run ./cmd/compatcheck check -old A -new B -format text   # 临时比对两棵 proto 树

# 版本化语料库与批量回放（默认指向 http://localhost:8080，可用 REGISTRY_ADDR 覆盖）
go run ./cmd/corpusctl upload  -pkg P -corpus C [-base N] samples.json
go run ./cmd/corpusctl update  -pkg P -corpus C [-add a,b] [-remove d]
go run ./cmd/corpusctl seal    -pkg P -corpus C
go run ./cmd/corpusctl list    -pkg P
go run ./cmd/corpusctl replay  -pkg P -corpus C -version N -schema V [-key K]
go run ./cmd/corpusctl result  -pkg P -id N
go run ./cmd/corpusctl compare -pkg P -old N1 -new N2

go test ./...                                    # 单元测试 + 同一套回归案例
```

`testdata/cases` 下每个案例是 `old/`、`new/` 两棵 proto 树加 `case.json` 期望，
覆盖嵌套导入（菱形依赖）、oneof 迁移、同名不同包、字段号复用、保留/未保留删除、
枚举默认值变化、类型变化矩阵与样例载荷。期望未列出的 WARN/FAIL 会使案例失败——
沉默不算通过。

## 测试

```bash
go test ./...                    # 全部（PostgreSQL 集成测试在无 DATABASE_URL 时跳过）
DATABASE_URL=postgres://... go test ./internal/registry/ -run TestPGStore
DATABASE_URL=postgres://... go test ./internal/registry/ -run TestPGCorpusReplay
```

语料库验收由 `internal/registry/corpus_service_test.go`（内存存储）与
`pgcorpus_test.go`（真实 PostgreSQL）覆盖：相同内容重复上传不产生新样例、封存后
修改被拒绝、混合 wire/JSON 部分失败不影响其余项、中断重启不重复已完成项、同一语料
对新旧 schema 给出稳定可追溯差异、删除草稿不影响历史回放。
