# 气象观测数据湖

FastAPI + PyArrow(Parquet) + SQLite 的观测数据湖示例。网页维护一张观测表：

- 固定列：`station_id`（站号）、`observed_at`（UTC 时刻，写入前统一换算为 UTC）；
- 最多 8 个可空 `int64` 测量列，可新增 / 改名 / 删除；
- 每批上传采集站导出的 CSV（≤1000 行），转换为 Parquet 入湖；
- 可按**当前**列名查看、下载任一批次（CSV / Parquet）；
- 按站号整批维护最多 20 段 UTC 左闭右开区域区间，批次页预览并可下载含区域归属的 CSV。

## 设计要点（对应需求）

| 需求 | 实现 |
| --- | --- |
| 改名沿用旧值 | Parquet 的物理列名是永久 `field_id`（`f_<uuid4>`），列名只存在于 `fields` 表；改名只更新 `fields.name`，历史文件不动 |
| 删除后同名新增不串值 | 删除是软删除（`status='deleted'`，保留 id）；同名新增生成新的 `field_id`；历史批次中没有新 id 的列在读取时补 `NULL` |
| 历史 Parquet 不得重写 | 只做“读 + 内存投影”，从不以写方式打开历史文件；新文件以临时文件写入后 rename |
| 结构版本 | SQLite `meta.schema_version` 单调递增；每次新增/改名/删除提交时 +1 |
| 过期整次拒绝 | 表单/API 必须携带页面所见版本；写事务内 `BEGIN IMMEDIATE` 先比对版本，不等则整次拒绝（HTTP 409 / 网页提示） |
| 转换期间仍可改结构 | 上传路径只在解析前后短暂读库，写 Parquet 时不持锁；提交登记时二次比对版本，若结构已变则拒绝登记并删除刚写的文件 |
| 双进程并发 | SQLite WAL + `busy_timeout` + `BEGIN IMMEDIATE` 串行化写入；两实例共享数据目录 |
| 失败/重启后一致 | 批次登记与结构变更同一事务；文件先落盘再登记，登记失败删文件；启动时清理陈旧临时文件与（超过 1 小时的）孤儿 Parquet |
| 查询只用同一已提交结构 | `get_batch` / 页面在单个读事务中读取结构与批次登记；不读未提交数据 |
| 失败保留已有批次 | 任何失败路径都不改已有登记与文件；`/api/integrity` 可核对登记与文件 sha256 |
| 站点区域时间轴 | 独立 `region_timelines` / `region_segments` 表，按站号整批替换；每站 `revision` 单调递增，携带所见修订号的提交在 `BEGIN IMMEDIATE` 内比对，冲突整次拒绝（409 `stale_region`），旧时间轴不变 |
| 区域区间约束 | UTC 左闭右开 `[start, end)`；每站最多 20 段；空区间、重叠（首尾相接不算重叠）拒绝；按开始时刻排序存储；空档不归属 |
| 批次区域归属 | 读取批次时在内存中按“站号 + 精确 `observed_at`”匹配时间轴，追加 `region_code` 列；沿用当前测量列名与同名重建补空规则；原 Parquet 不改 |
| 下载一致性 | 预览与 CSV 下载在同一次 API 调用内由同一个读事务给出批次登记、当前列名和区域时间轴，再在内存组装；下载期间另一实例的上传 / 改列 / 区域修订不会混合进本次响应，后续请求反映新状态 |

## 本机运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DATA_DIR=./data INSTANCE_NAME=node-a uvicorn app.main:app --port 8000
```

另一个终端用相同 `DATA_DIR`、`INSTANCE_NAME=node-b` 起第二个实例（端口 8001），
分别打开网页即可演示双进程共享结构与批次。

## Docker Compose（双实例 + 共享持久卷）

```bash
docker compose up --build -d
# 浏览器打开 http://localhost:8001 (node-a) 与 http://localhost:8002 (node-b)
docker compose down          # 保留命名卷 lake-data
docker compose down -v       # 连同数据一起删除
```

两个服务共用命名卷 `lake-data`，`DATA_DIR=/data`。

## 网页演示流程

1. `http://localhost:8001/` 新增测量列 `temp_c`、`humidity`；
2. 下载“示例 CSV”，或自行准备：

   ```csv
   station_id,observed_at,temp_c,humidity
   54511,2026-01-01T00:00:00Z,7,40
   54512,2026-01-01T01:00:00Z,9,
   ```

3. 上传后进入批次页查看；
4. 在 `node-b` 把 `temp_c` 改名为 `temperature_c`，回到 `node-a` 刷新批次页：列名新、值仍为旧值；
5. 删除 `temp_c`（或已改名的列）后新增同名 `temp_c`，再上传新批次：
   旧批次该列显示 `NULL`，新批次显示新值；旧 Parquet 文件字节未变；
6. 打开两个标签页（或分别用两个实例）同时提交基于同一版本的结构修改：
   一个成功、另一个提示“结构版本已过期”并被整次拒绝；
7. 打开“站点区域归属”页，输入站号（如 `54511`）并载入；新增若干段
   开始 / 结束（UTC ISO 8601）与区域代码后提交。两个实例同时基于同一修订号提交，
   只有一个成功，另一个 409 且旧时间轴不变；
8. 回到批次页：预览表末列 `region_code` 按站号与精确观测时刻匹配；区间空档为空；
   下载 CSV 含该列，Parquet 下载与原文件不含该列、字节未变；
9. `/api/integrity` 或首页“一致性自检”核对登记与文件一致。

## 区域归属示例

```bash
curl -X POST http://localhost:8001/api/regions/54511 \
  -H 'Content-Type: application/json' \
  -d '{"revision": 0, "segments": [
        {"start": "2026-01-01T00:00:00Z", "end": "2026-02-01T00:00:00Z", "region_code": "华北"},
        {"start": "2026-03-01T00:00:00Z", "end": "2026-04-01T00:00:00Z", "region_code": "华东"}
      ]}'
```

`observed_at == start` 归属该段，`observed_at == end` 不归属（左闭右开）；
两段之间与时间轴之外的时刻都归空。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 结构快照 + 批次列表 |
| GET | `/healthz` | 实例名、pid、版本 |
| POST | `/api/fields` | `{"schema_version": v, "name": "..."}` |
| PATCH | `/api/fields/{field_id}` | 改名，body 同上 |
| DELETE | `/api/fields/{field_id}` | 软删除，body `{"schema_version": v}` |
| POST | `/api/batches` | multipart：`version` + `file` |
| GET | `/api/batches/{id}?format=json\|csv\|parquet` | 按当前结构读取/下载；JSON 另含 `region_column`、`region_codes`、`regions` |
| GET | `/api/regions` | 全部站点区域时间轴 |
| GET | `/api/regions/{station}` | 单站时间轴（未维护返回 r0 空段） |
| POST | `/api/regions/{station}` | `{"revision": r, "segments": [...]}` 整批替换时间轴 |
| GET | `/api/integrity` | 文件与登记一致性自检 |

过期版本返回 409 `{"error": "stale_schema", ...}`；区域修订过期返回 409
`{"error": "stale_region", "station_id", "expected_version", "revision"}`；
参数错误 400；文件缺失 500 `integrity_error`。

## 测试

```bash
python -m pytest -q                 # 37 项：核心/网页/区域归属/一致性；含两个真实 uvicorn 进程的并发测试
python scripts/selftest_dual.py     # 端到端演示脚本：起两个真实进程 + 共享目录，逐项打印 PASS/FAIL
```

## 已知限制

- 单机 SQLite + 共享卷方案适用于演示与中小规模；跨主机需换成服务端数据库与对象存储。
- 网页为服务端渲染，无前端路由；并发冲突通过版本号拒绝，不做自动合并。
- 删除只做逻辑删除，Parquet 中历史字段的物理列会一直保留（这是“不重写历史”的代价）。
- 区域时间轴按站号精确整批替换，没有逐段增量编辑或历史版本回溯。
- 沙箱环境无 `docker`，Compose 配置与镜像构建未在本机实际运行；已用两个真实 uvicorn 进程共享目录完成同等并发验证。
