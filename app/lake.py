"""数据湖核心：结构版本、字段身份、批次登记与不可变 Parquet。

设计要点
--------
* 测量列的物理标识是永久不变的 ``field_id``；Parquet 中保存的就是 field_id。
  列名只是 ``fields`` 表上的一个可修改属性。
* 删除字段仅把状态置为 deleted。之后用同名新增会得到新的 field_id，
  旧批次里没有新 id 的列，读取时补 NULL，因此不会串值。
* 结构变更与批次登记都在 SQLite ``BEGIN IMMEDIATE`` 事务中完成，并校验
  调用方看到的 schema_version；过期则整次拒绝。
* 新 Parquet 先写临时文件、fsync、rename，再登记数据库。数据库提交失败会
  删除刚写的文件。历史 Parquet 永不以写方式打开。
* 一次页面/API 查询只在一个 SQLite 读事务里取得“结构 + 批次登记”快照。
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import io
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("metlake")

MAX_FIELDS = 8
MAX_ROWS = 1000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024
MAX_STATION_LENGTH = 256
MAX_FIELD_NAME_LENGTH = 64
MAX_REGION_SEGMENTS = 20
MAX_REGION_CODE_LENGTH = 64

STATION_COLUMN = "station_id"
TIME_COLUMN = "observed_at"
REGION_COLUMN = "region_code"
RESERVED_NAMES = {
    STATION_COLUMN.casefold(),
    TIME_COLUMN.casefold(),
    REGION_COLUMN.casefold(),
}
FIELD_ID_RE = re.compile(r"^f_[0-9a-f]{32}$")
INT_RE = re.compile(r"^[+-]?\d+$")
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

SCHEMA_VERSION_KEY = "schema_version"
UPLOAD_METADATA_PREFIX = b"metlake."


class LakeError(Exception):
    """可安全展示给用户的领域错误。"""

    status_code = 400
    code = "bad_request"


class ValidationError(LakeError):
    status_code = 400
    code = "validation"


class StaleSchema(LakeError):
    status_code = 409
    code = "stale_schema"

    def __init__(self, expected: int, actual: int) -> None:
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"结构版本已过期：本次操作基于 v{expected}，当前已提交结构为 v{actual}。"
            "为避免旧结构写入，整次操作已被拒绝，请刷新页面后重试。"
        )


class StaleRegion(LakeError):
    """站点区域时间轴的修订号冲突。"""

    status_code = 409
    code = "stale_region"

    def __init__(self, station_id: str, expected: int, actual: int) -> None:
        self.station_id = station_id
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"站点 {station_id} 的区域归属已过期：本次操作基于修订 r{expected}，"
            f"当前已提交修订为 r{actual}。整次操作已被拒绝，旧时间轴保持不变；"
            "请刷新后重试。"
        )


class NotFound(LakeError):
    status_code = 404
    code = "not_found"


class StorageError(LakeError):
    status_code = 500
    code = "storage_error"


class BatchIntegrityError(LakeError):
    status_code = 500
    code = "integrity_error"


# ---------------------------------------------------------------------------
# 配置 / 数据库基础
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    instance_name: str = "local"

    @classmethod
    def from_env(
        cls,
        data_dir: str | os.PathLike[str] | None = None,
        instance_name: str | None = None,
    ) -> "Settings":
        resolved = Path(data_dir or os.environ.get("DATA_DIR", "./data")).expanduser()
        name = instance_name or os.environ.get("INSTANCE_NAME", "local")
        return cls(data_dir=resolved.resolve(), instance_name=name)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "lake.sqlite3"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Field:
    id: str
    name: str
    ordinal: int
    status: str
    created_version: int
    deleted_version: int | None


@dataclass(frozen=True)
class Schema:
    version: int
    fields: tuple[Field, ...]

    @property
    def active(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "active")

    @property
    def deleted(self) -> tuple[Field, ...]:
        return tuple(f for f in self.fields if f.status == "deleted")


@dataclass(frozen=True)
class Batch:
    id: str
    created_at: str
    schema_version: int
    row_count: int
    file_name: str
    file_sha256: str
    fields_at_upload: tuple[dict, ...]

    @property
    def short_id(self) -> str:
        return self.id[:8]


@dataclass(frozen=True)
class RegionSegment:
    """一段 UTC 左闭右开有效区间：``start <= t < end``。"""

    start: datetime
    end: datetime
    region_code: str


@dataclass(frozen=True)
class RegionTimeline:
    station_id: str
    revision: int
    segments: tuple[RegionSegment, ...]
    updated_at: str | None = None

    @property
    def segment_count(self) -> int:
        return len(self.segments)


@dataclass(frozen=True)
class LakeState:
    schema: Schema
    batches: tuple[Batch, ...]
    regions: tuple[RegionTimeline, ...] = ()


@dataclass(frozen=True)
class BatchView:
    schema: Schema
    batch: Batch
    table: pa.Table
    assignment_table: pa.Table | None = None
    regions: dict[str, "RegionTimeline"] = field(default_factory=dict)

    @property
    def preview_table(self) -> pa.Table:
        """预览 / CSV 导出用：当前测量列 + 区域归属列。"""

        return self.assignment_table if self.assignment_table is not None else self.table


@dataclass(frozen=True)
class ParsedCsv:
    stations: tuple[str, ...]
    times: tuple[datetime, ...]
    values: dict[str, tuple[int | None, ...]]
    header_fields: tuple[Field, ...]


@dataclass(frozen=True)
class IntegrityReport:
    ok: bool
    batches: int
    missing_files: tuple[str, ...]
    hash_mismatches: tuple[str, ...]
    orphan_files: tuple[str, ...]
    temp_files: tuple[str, ...]


# 仅测试使用：Parquet 已写完、数据库提交前的钩子。
_before_commit_hook: Callable[[], None] | None = None


def set_before_commit_hook(hook: Callable[[], None] | None) -> None:
    """测试钩子：在转换完成、批次登记提交前执行。生产路径不设置。"""

    global _before_commit_hook
    _before_commit_hook = hook


def _connect(settings: Settings) -> sqlite3.Connection:
    conn = sqlite3.connect(
        settings.db_path,
        timeout=30.0,
        isolation_level=None,  # 手动控制事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 15000")
    conn.execute("PRAGMA synchronous = FULL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize(settings: Settings) -> None:
    """幂等初始化；两个实例同时启动也安全。"""

    settings.ensure_dirs()
    last_error: Exception | None = None
    for attempt in range(60):
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(
                settings.db_path,
                timeout=30.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout = 15000")
            mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                log.warning("SQLite journal_mode=%s (期望 wal)", mode)
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fields (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('active', 'deleted')),
                    created_version INTEGER NOT NULL,
                    deleted_version INTEGER,
                    created_at TEXT NOT NULL,
                    deleted_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fields_active_name
                ON fields(name COLLATE NOCASE)
                WHERE status = 'active'
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_fields_status_ordinal "
                "ON fields(status, ordinal)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL,
                    row_count INTEGER NOT NULL,
                    file_name TEXT NOT NULL UNIQUE,
                    file_sha256 TEXT NOT NULL,
                    schema_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_batches_created_at "
                "ON batches(created_at DESC)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS region_timelines (
                    station_id TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL CHECK (revision >= 0),
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS region_segments (
                    station_id TEXT NOT NULL,
                    seq INTEGER NOT NULL CHECK (seq >= 0),
                    start_us INTEGER NOT NULL,
                    end_us INTEGER NOT NULL,
                    region_code TEXT NOT NULL,
                    PRIMARY KEY (station_id, seq),
                    FOREIGN KEY (station_id)
                        REFERENCES region_timelines(station_id) ON DELETE CASCADE,
                    CHECK (start_us < end_us)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_region_segments_station_start "
                "ON region_segments(station_id, start_us)"
            )
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES (?, '0')",
                (SCHEMA_VERSION_KEY,),
            )
            conn.execute("COMMIT")
            _cleanup_stale_temps(settings)
            _cleanup_orphan_files(settings)
            return
        except sqlite3.Error as exc:  # 并发启动时可能 busy
            last_error = exc
            if conn is not None:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            time.sleep(min(0.05 * (attempt + 1), 0.5))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
    raise StorageError(
        f"无法初始化数据库 {settings.db_path}：{last_error}"
    ) from last_error


@contextmanager
def _write_transaction(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _connect(settings)
    committed = False
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
        committed = True
    except BaseException:
        if not committed:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        raise
    finally:
        try:
            conn.close()
        except sqlite3.Error:
            # 关闭失败不改变事务结果；已有提交/回滚结论。
            log.exception("关闭数据库连接失败")


@contextmanager
def _read_transaction(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = _connect(settings)
    try:
        conn.execute("BEGIN")
        yield conn
    finally:
        try:
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        conn.close()


def _rows_to_batch(row: sqlite3.Row) -> Batch:
    try:
        fields = tuple(json.loads(row["schema_json"]))
    except (TypeError, ValueError):
        fields = ()
    return Batch(
        id=row["id"],
        created_at=row["created_at"],
        schema_version=int(row["schema_version"]),
        row_count=int(row["row_count"]),
        file_name=row["file_name"],
        file_sha256=row["file_sha256"],
        fields_at_upload=fields,
    )


def _query_schema(conn: sqlite3.Connection) -> Schema:
    version_row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if version_row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    version = int(version_row["value"])
    rows = conn.execute(
        "SELECT id, name, ordinal, status, created_version, deleted_version "
        "FROM fields ORDER BY ordinal ASC, created_at ASC"
    ).fetchall()
    fields = tuple(
        Field(
            id=r["id"],
            name=r["name"],
            ordinal=int(r["ordinal"]),
            status=r["status"],
            created_version=int(r["created_version"]),
            deleted_version=(
                int(r["deleted_version"]) if r["deleted_version"] is not None else None
            ),
        )
        for r in rows
    )
    return Schema(version=version, fields=fields)


def read_state(settings: Settings) -> LakeState:
    """在一个读事务中取得一致的结构 + 批次登记 + 区域时间轴快照。"""

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            rows = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches "
                "ORDER BY created_at DESC, id DESC"
            ).fetchall()
            batches = tuple(_rows_to_batch(r) for r in rows)
            regions = _query_region_timelines(conn)
    except sqlite3.Error as exc:
        raise StorageError("读取数据湖状态失败。") from exc
    return LakeState(schema=schema, batches=batches, regions=regions)


def read_schema(settings: Settings) -> Schema:
    try:
        with _read_transaction(settings) as conn:
            return _query_schema(conn)
    except sqlite3.Error as exc:
        raise StorageError("读取结构失败。") from exc


# ---------------------------------------------------------------------------
# 站点区域时间轴
# ---------------------------------------------------------------------------


def normalize_station_id(raw: object) -> str:
    if raw is None:
        raise ValidationError("站号不能为空。")
    station = str(raw).strip()
    if not station:
        raise ValidationError("站号不能为空。")
    if len(station) > MAX_STATION_LENGTH:
        raise ValidationError(f"站号最长 {MAX_STATION_LENGTH} 个字符。")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in station):
        raise ValidationError("站号不能包含控制字符。")
    return station


def _to_epoch_us(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return (value - EPOCH) // timedelta(microseconds=1)


def _from_epoch_us(value: int) -> datetime:
    return EPOCH + timedelta(microseconds=int(value))


def parse_region_time(raw_start: object, raw_end: object, row_no: int) -> tuple[int, int]:
    """把一段区间的起止解析为 UTC 微秒；返回半开区间 [start, end)。"""

    if raw_start is None or str(raw_start).strip() == "":
        raise ValidationError(f"第 {row_no} 段缺少开始时刻。")
    if raw_end is None or str(raw_end).strip() == "":
        raise ValidationError(f"第 {row_no} 段缺少结束时刻。")
    try:
        start = datetime.fromisoformat(str(raw_start).strip().replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(raw_end).strip().replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(
            f"第 {row_no} 段的起止时刻必须是 ISO 8601 格式（UTC）。"
        ) from None
    start_us = _to_epoch_us(start)
    end_us = _to_epoch_us(end)
    if start_us >= end_us:
        raise ValidationError(
            f"第 {row_no} 段的开始时刻必须早于结束时刻（左闭右开区间不能为空）。"
        )
    return start_us, end_us


def normalize_region_code(raw: object, row_no: int) -> str:
    if raw is None or str(raw).strip() == "":
        raise ValidationError(f"第 {row_no} 段缺少区域代码。")
    code = unicodedata.normalize("NFC", str(raw).strip())
    if len(code) > MAX_REGION_CODE_LENGTH:
        raise ValidationError(
            f"第 {row_no} 段区域代码最长 {MAX_REGION_CODE_LENGTH} 个字符。"
        )
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in code):
        raise ValidationError(f"第 {row_no} 段区域代码不能包含控制字符。")
    return code


def parse_region_segments(raw_segments: object) -> tuple[tuple[int, int, str], ...]:
    """校验并排序区域段；返回 ``(start_us, end_us, region_code)``。"""

    if not isinstance(raw_segments, (list, tuple)):
        raise ValidationError("区域段必须是数组。")
    if len(raw_segments) > MAX_REGION_SEGMENTS:
        raise ValidationError(f"每个站点最多维护 {MAX_REGION_SEGMENTS} 段区域区间。")

    parsed: list[tuple[int, int, str]] = []
    for index, item in enumerate(raw_segments, start=1):
        if not isinstance(item, dict):
            raise ValidationError(f"第 {index} 段格式无效，必须是对象。")
        start_us, end_us = parse_region_time(
            item.get("start", item.get("start_at")),
            item.get("end", item.get("end_at")),
            index,
        )
        code = normalize_region_code(
            item.get("region_code", item.get("region")), index
        )
        parsed.append((start_us, end_us, code))

    parsed.sort(key=lambda item: (item[0], item[1]))
    previous_end: int | None = None
    for index, (start_us, end_us, _code) in enumerate(parsed, start=1):
        if previous_end is not None and start_us < previous_end:
            raise ValidationError(
                f"第 {index} 段与上一段区间重叠；同一站点的有效区间不得重叠。"
            )
        previous_end = end_us if previous_end is None else max(previous_end, end_us)
    return tuple(parsed)


def _query_region_segments(
    conn: sqlite3.Connection, station_id: str
) -> tuple[RegionSegment, ...]:
    rows = conn.execute(
        "SELECT start_us, end_us, region_code FROM region_segments "
        "WHERE station_id = ? ORDER BY start_us ASC, end_us ASC",
        (station_id,),
    ).fetchall()
    return tuple(
        RegionSegment(
            start=_from_epoch_us(r["start_us"]),
            end=_from_epoch_us(r["end_us"]),
            region_code=r["region_code"],
        )
        for r in rows
    )


def _query_region_timelines(
    conn: sqlite3.Connection, station_id: str | None = None
) -> tuple[RegionTimeline, ...]:
    if station_id is None:
        rows = conn.execute(
            "SELECT station_id, revision, updated_at FROM region_timelines "
            "ORDER BY station_id ASC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT station_id, revision, updated_at FROM region_timelines "
            "WHERE station_id = ?",
            (station_id,),
        ).fetchall()
    return tuple(
        RegionTimeline(
            station_id=r["station_id"],
            revision=int(r["revision"]),
            segments=_query_region_segments(conn, r["station_id"]),
            updated_at=r["updated_at"],
        )
        for r in rows
    )


def read_regions(settings: Settings) -> tuple[RegionTimeline, ...]:
    try:
        with _read_transaction(settings) as conn:
            return _query_region_timelines(conn)
    except sqlite3.Error as exc:
        raise StorageError("读取区域时间轴失败。") from exc


def read_region(settings: Settings, station_id: str) -> RegionTimeline:
    station = normalize_station_id(station_id)
    try:
        with _read_transaction(settings) as conn:
            timelines = _query_region_timelines(conn, station)
    except sqlite3.Error as exc:
        raise StorageError("读取区域时间轴失败。") from exc
    if timelines:
        return timelines[0]
    return RegionTimeline(station_id=station, revision=0, segments=())


def set_region_timeline(
    settings: Settings,
    station_id: str,
    expected_revision: int,
    raw_segments: object,
) -> RegionTimeline:
    """整批替换某站点的区域时间轴。

    写入在 ``BEGIN IMMEDIATE`` 中完成；若另一实例已用同一修订号提交，
    整次拒绝并保留旧时间轴。
    """

    station = normalize_station_id(station_id)
    segments = parse_region_segments(raw_segments)
    updated_at = _utc_now_iso()
    try:
        with _write_transaction(settings) as conn:
            row = conn.execute(
                "SELECT revision FROM region_timelines WHERE station_id = ?",
                (station,),
            ).fetchone()
            actual = int(row["revision"]) if row is not None else 0
            if expected_revision != actual:
                raise StaleRegion(station, expected_revision, actual)
            new_revision = actual + 1
            conn.execute(
                "INSERT INTO region_timelines(station_id, revision, updated_at) "
                "VALUES (?, ?, ?) ON CONFLICT(station_id) DO UPDATE SET "
                "revision = excluded.revision, updated_at = excluded.updated_at",
                (station, new_revision, updated_at),
            )
            conn.execute(
                "DELETE FROM region_segments WHERE station_id = ?", (station,)
            )
            conn.executemany(
                "INSERT INTO region_segments(station_id, seq, start_us, end_us, "
                "region_code) VALUES (?, ?, ?, ?, ?)",
                [
                    (station, seq, start_us, end_us, code)
                    for seq, (start_us, end_us, code) in enumerate(segments)
                ],
            )
            log.info(
                "region timeline %s committed r%s (%s segments)",
                station,
                new_revision,
                len(segments),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("保存区域时间轴失败，原时间轴未改变。") from exc

    return RegionTimeline(
        station_id=station,
        revision=new_revision,
        segments=tuple(
            RegionSegment(
                start=_from_epoch_us(start_us),
                end=_from_epoch_us(end_us),
                region_code=code,
            )
            for start_us, end_us, code in segments
        ),
        updated_at=updated_at,
    )


def lookup_region(
    timeline: RegionTimeline | None, observed_at_us: int
) -> str | None:
    """在时间轴中查找精确时刻的归属；空档返回 None。"""

    if timeline is None or not timeline.segments:
        return None
    starts = [_to_epoch_us(segment.start) for segment in timeline.segments]
    index = bisect.bisect_right(starts, observed_at_us) - 1
    if index < 0:
        return None
    segment = timeline.segments[index]
    if observed_at_us < _to_epoch_us(segment.end):
        return segment.region_code
    return None


def _with_region_column(
    table: pa.Table, timelines: dict[str, RegionTimeline]
) -> pa.Table:
    """按站号 + 精确观测时刻追加 ``region_code``；空档 / 无时间轴为 NULL。"""

    if REGION_COLUMN in table.column_names:
        raise BatchIntegrityError(
            f"当前测量列名与保留的归属列 {REGION_COLUMN} 冲突，请先重命名该测量列。"
        )
    stations = table.column(STATION_COLUMN).combine_chunks().to_pylist()
    observed = table.column(TIME_COLUMN).combine_chunks().to_pylist()
    cache: dict[tuple[str, int], str | None] = {}
    codes: list[str | None] = []
    for station, moment in zip(stations, observed):
        if moment is None:
            codes.append(None)
            continue
        moment_us = _to_epoch_us(moment)
        key = (str(station), moment_us)
        if key not in cache:
            cache[key] = lookup_region(timelines.get(str(station)), moment_us)
        codes.append(cache[key])
    arrays = [table.column(name).combine_chunks() for name in table.column_names]
    arrays.append(pa.array(codes, type=pa.string()))
    return pa.Table.from_arrays(arrays, names=[*table.column_names, REGION_COLUMN])


# ---------------------------------------------------------------------------
# 字段结构变更
# ---------------------------------------------------------------------------


def normalize_field_name(raw: str | None) -> str:
    if raw is None:
        raise ValidationError("字段名不能为空。")
    name = unicodedata.normalize("NFC", str(raw).strip())
    if not name:
        raise ValidationError("字段名不能为空。")
    if len(name) > MAX_FIELD_NAME_LENGTH:
        raise ValidationError(f"字段名最长 {MAX_FIELD_NAME_LENGTH} 个字符。")
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in name):
        raise ValidationError("字段名不能包含控制字符。")
    if name.casefold() in RESERVED_NAMES:
        raise ValidationError(
            f"“{name}”是固定列名（{STATION_COLUMN} / {TIME_COLUMN}），不能用作测量列名。"
        )
    return name


def _read_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if row is None:
        raise BatchIntegrityError("数据库缺少 schema_version")
    return int(row["value"])


def _check_expected_version(conn: sqlite3.Connection, expected: int) -> int:
    actual = _read_version(conn)
    if expected != actual:
        raise StaleSchema(expected, actual)
    return actual


def _bump_version(conn: sqlite3.Connection, current: int) -> int:
    new_version = current + 1
    conn.execute(
        "UPDATE meta SET value = ? WHERE key = ?",
        (str(new_version), SCHEMA_VERSION_KEY),
    )
    return new_version


def _next_ordinal(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(ordinal), -1) AS m FROM fields").fetchone()
    return int(row["m"]) + 1


def _ensure_name_free(
    conn: sqlite3.Connection, name: str, exclude_id: str | None = None
) -> None:
    rows = conn.execute(
        "SELECT id, name FROM fields WHERE status = 'active'"
    ).fetchall()
    folded = name.casefold()
    for row in rows:
        if exclude_id is not None and row["id"] == exclude_id:
            continue
        if row["name"].casefold() == folded:
            raise ValidationError(f"测量列“{name}”已存在。")


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def add_field(settings: Settings, expected_version: int, raw_name: str) -> Schema:
    name = normalize_field_name(raw_name)
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            active = conn.execute(
                "SELECT COUNT(*) AS n FROM fields WHERE status = 'active'"
            ).fetchone()["n"]
            if int(active) >= MAX_FIELDS:
                raise ValidationError(
                    f"测量列最多 {MAX_FIELDS} 个，请先删除不再使用的列。"
                )
            _ensure_name_free(conn, name)
            new_version = _bump_version(conn, version)
            conn.execute(
                "INSERT INTO fields(id, name, ordinal, status, created_version, "
                "deleted_version, created_at, deleted_at) "
                "VALUES (?, ?, ?, 'active', ?, NULL, ?, NULL)",
                (
                    f"f_{uuid.uuid4().hex}",
                    name,
                    _next_ordinal(conn),
                    new_version,
                    _utc_now_iso(),
                ),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("新增测量列失败，结构未改变。") from exc
    return read_schema(settings)


def rename_field(
    settings: Settings, expected_version: int, field_id: str, raw_name: str
) -> Schema:
    name = normalize_field_name(raw_name)
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id, name FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要改名的测量列不存在或已删除。")
            if row["name"] == name:
                raise ValidationError("新名称与当前名称相同。")
            _ensure_name_free(conn, name, exclude_id=field_id)
            new_version = _bump_version(conn, version)
            conn.execute(
                "UPDATE fields SET name = ? WHERE id = ?", (name, field_id)
            )
            log.info("renamed %s -> %s at v%s", field_id, name, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("测量列改名失败，结构未改变。") from exc
    return read_schema(settings)


def delete_field(settings: Settings, expected_version: int, field_id: str) -> Schema:
    try:
        with _write_transaction(settings) as conn:
            version = _check_expected_version(conn, expected_version)
            row = conn.execute(
                "SELECT id FROM fields WHERE id = ? AND status = 'active'",
                (field_id,),
            ).fetchone()
            if row is None:
                raise NotFound("要删除的测量列不存在或已删除。")
            new_version = _bump_version(conn, version)
            conn.execute(
                "UPDATE fields SET status = 'deleted', deleted_version = ?, "
                "deleted_at = ? WHERE id = ?",
                (new_version, _utc_now_iso(), field_id),
            )
            log.info("deleted %s at v%s", field_id, new_version)
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("删除测量列失败，结构未改变。") from exc
    return read_schema(settings)


# ---------------------------------------------------------------------------
# CSV -> Parquet
# ---------------------------------------------------------------------------


def parse_csv(data: bytes, schema: Schema) -> ParsedCsv:
    if not data:
        raise ValidationError("上传文件为空。")
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValidationError(
            f"CSV 文件不能超过 {MAX_UPLOAD_BYTES // (1024 * 1024)} MiB。"
        )
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("CSV 必须是 UTF-8 编码（可带 BOM）。") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        raw_header = next(reader)
    except StopIteration:
        raise ValidationError("CSV 缺少表头。") from None

    header = [unicodedata.normalize("NFC", h.strip()) for h in raw_header]
    if not header or all(not h for h in header):
        raise ValidationError("CSV 表头为空。")
    if any(not h for h in header):
        raise ValidationError("CSV 表头中存在空列名。")
    if len(set(header)) != len(header):
        raise ValidationError("CSV 表头中存在重复列名。")

    fixed: dict[str, int] = {}
    for required in (STATION_COLUMN, TIME_COLUMN):
        try:
            fixed[required] = header.index(required)
        except ValueError:
            raise ValidationError(
                f"CSV 必须包含固定列“{required}”。"
            ) from None
        if header.count(required) != 1:
            raise ValidationError(f"固定列“{required}”只能出现一次。")

    active = schema.active
    by_name = {f.name: f for f in active}
    selected: dict[str, Field] = {}
    unknown: list[str] = []
    for idx, name in enumerate(header):
        if idx in fixed.values():
            continue
        field = by_name.get(name)
        if field is None:
            unknown.append(name)
        else:
            selected[name] = field
    if unknown:
        available = "、".join(f.name for f in active) or "（无）"
        raise ValidationError(
            "CSV 含当前结构之外的测量列："
            + "、".join(unknown)
            + f"。当前可接收的测量列为：{available}。"
            "请刷新页面使用最新结构导出/上传。"
        )

    stations: list[str] = []
    times: list[datetime] = []
    values: dict[str, list[int | None]] = {
        field.id: [] for field in selected.values()
    }

    station_idx = header.index(STATION_COLUMN)
    time_idx = header.index(TIME_COLUMN)
    selected_by_index = {
        idx: field for idx, name in enumerate(header) if (field := selected.get(name))
    }

    row_count = 0
    for row in reader:
        if not row:
            continue
        row_count += 1
        if row_count > MAX_ROWS:
            raise ValidationError(f"每批最多 {MAX_ROWS} 行数据。")
        line_no = reader.line_num
        if len(row) != len(header):
            raise ValidationError(
                f"CSV 第 {line_no} 行列数为 {len(row)}，表头为 {len(header)} 列。"
            )

        station = row[station_idx].strip()
        if not station:
            raise ValidationError(f"CSV 第 {line_no} 行站号为空。")
        if len(station) > MAX_STATION_LENGTH:
            raise ValidationError(f"CSV 第 {line_no} 行站号过长。")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in station):
            raise ValidationError(f"CSV 第 {line_no} 行站号含控制字符。")
        stations.append(station)

        raw_time = row[time_idx].strip()
        if not raw_time:
            raise ValidationError(f"CSV 第 {line_no} 行 UTC 时刻为空。")
        try:
            parsed_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(
                f"CSV 第 {line_no} 行时刻“{raw_time}”不是 ISO 8601 格式。"
            ) from None
        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)
        else:
            parsed_time = parsed_time.astimezone(timezone.utc)
        times.append(parsed_time)

        for idx, field in selected_by_index.items():
            raw_value = row[idx].strip()
            if raw_value == "":
                parsed_value: int | None = None
            else:
                if not INT_RE.fullmatch(raw_value):
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值“{raw_value}”"
                        "不是整数。空值请留空。"
                    )
                parsed_value = int(raw_value)
                if parsed_value < INT64_MIN or parsed_value > INT64_MAX:
                    raise ValidationError(
                        f"CSV 第 {line_no} 行“{field.name}”的值超出 64 位整数范围。"
                    )
            values[field.id].append(parsed_value)

    if row_count == 0:
        raise ValidationError("CSV 至少需要 1 行数据。")

    filled: dict[str, tuple[int | None, ...]] = {}
    for field in active:
        if field.id in values:
            filled[field.id] = tuple(values[field.id])
        else:
            filled[field.id] = tuple([None] * row_count)

    return ParsedCsv(
        stations=tuple(stations),
        times=tuple(times),
        values=filled,
        header_fields=tuple(selected.values()),
    )


def _build_upload_table(parsed: ParsedCsv, schema: Schema) -> pa.Table:
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        pa.array(parsed.stations, type=pa.string()),
        pa.array(parsed.times, type=pa.timestamp("us", tz="UTC")),
    ]
    for field in schema.active:
        names.append(field.id)
        arrays.append(pa.array(parsed.values[field.id], type=pa.int64()))
    return pa.Table.from_arrays(arrays, names=names)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_path(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    try:
        _fsync_path(path)
    except OSError as exc:  # 某些文件系统不支持目录 fsync
        log.warning("目录 fsync 失败 %s: %s", path, exc)


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        log.exception("清理文件失败 %s", path)


def _cleanup_stale_temps(settings: Settings) -> None:
    """清理超过 1 小时的临时文件。

    新写入的临时文件命名以点号开头（``.xxx.tmp``），这里的 glob 覆盖两种形态；
    用“足够旧”阈值避免误删另一实例正在写入的临时文件。
    """

    cutoff = time.time() - 3600
    try:
        candidates = [
            p
            for p in settings.parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file() and p.stat().st_mtime < cutoff
        ]
    except OSError:
        log.warning("扫描临时文件失败")
        return
    for path in candidates:
        try:
            path.unlink(missing_ok=True)
            log.warning("清理陈旧临时文件：%s", path.name)
        except OSError:
            log.warning("无法清理临时文件 %s", path)


def _cleanup_orphan_files(settings: Settings) -> None:
    """清理崩溃遗留的、未被任何批次登记的 Parquet。

    只处理超过 1 小时的文件，避免误删另一实例正在转换 / 即将登记的新文件。
    """

    cutoff = time.time() - 3600
    try:
        candidates = [
            p
            for p in settings.parquet_dir.glob("*.parquet")
            if p.is_file() and p.stat().st_mtime < cutoff
        ]
    except OSError:
        log.warning("扫描孤立 Parquet 失败")
        return
    if not candidates:
        return
    try:
        registered = {b.file_name for b in read_state(settings).batches}
    except LakeError:
        return
    for path in candidates:
        if path.name in registered:
            continue
        try:
            # 再确认一次：另一实例可能刚登记该文件
            if path.name in {b.file_name for b in read_state(settings).batches}:
                continue
            path.unlink(missing_ok=True)
            log.warning("清理未登记的孤立 Parquet：%s", path.name)
        except (OSError, LakeError):
            log.warning("无法清理孤立 Parquet：%s", path.name)


def _write_parquet_atomic(
    table: pa.Table, final_path: Path, metadata: dict[bytes, bytes]
) -> str:
    """临时文件 -> fsync -> rename -> fsync 目录；返回内容 sha256。"""

    table = table.replace_schema_metadata(metadata)
    tmp_path = final_path.with_name(f".{final_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, tmp_path, compression="snappy")
        _fsync_path(tmp_path)
        digest = _sha256_file(tmp_path)
        os.replace(tmp_path, final_path)
        _fsync_dir(final_path.parent)
        return digest
    except BaseException:
        _safe_unlink(tmp_path)
        raise


# ---------------------------------------------------------------------------
# 批次上传与读取
# ---------------------------------------------------------------------------


def upload_csv(settings: Settings, expected_version: int, data: bytes) -> Batch:
    """解析并转换后，在最终提交前再次校验结构版本。

    转换/写文件期间不持有数据库写锁，因此结构仍可被另一进程修改；提交时
    若版本已变化，则批次绝不登记，刚写的 Parquet 也会被清理。
    """

    schema = read_schema(settings)
    if schema.version != expected_version:
        raise StaleSchema(expected_version, schema.version)

    parsed = parse_csv(data, schema)
    table = _build_upload_table(parsed, schema)

    batch_id = uuid.uuid4().hex
    created_at = _utc_now_iso()
    file_name = f"{batch_id}.parquet"
    final_path = settings.parquet_dir / file_name
    fields_at_upload = [
        {"id": f.id, "name": f.name, "ordinal": f.ordinal} for f in schema.active
    ]
    schema_json = json.dumps(fields_at_upload, ensure_ascii=False, separators=(",", ":"))
    metadata = {
        UPLOAD_METADATA_PREFIX + b"format": b"1",
        UPLOAD_METADATA_PREFIX + b"batch_id": batch_id.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"schema_version": str(schema.version).encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"created_at": created_at.encode("ascii"),
        UPLOAD_METADATA_PREFIX + b"fields": schema_json.encode("utf-8"),
    }

    try:
        file_sha256 = _write_parquet_atomic(table, final_path, metadata)
    except OSError as exc:
        raise StorageError("写入 Parquet 失败，批次未登记。") from exc

    try:
        if _before_commit_hook is not None:
            _before_commit_hook()
        batch = _commit_batch(
            settings=settings,
            expected_version=expected_version,
            batch_id=batch_id,
            created_at=created_at,
            row_count=len(parsed.stations),
            file_name=file_name,
            file_sha256=file_sha256,
            schema_json=schema_json,
            fields_at_upload=fields_at_upload,
        )
    except BaseException:
        _safe_unlink(final_path)
        raise
    log.info(
        "batch %s committed rows=%s schema=v%s", batch_id, len(parsed.stations), schema.version
    )
    return batch


def _commit_batch(
    *,
    settings: Settings,
    expected_version: int,
    batch_id: str,
    created_at: str,
    row_count: int,
    file_name: str,
    file_sha256: str,
    schema_json: str,
    fields_at_upload: Sequence[dict],
) -> Batch:
    try:
        with _write_transaction(settings) as conn:
            _check_expected_version(conn, expected_version)
            conn.execute(
                "INSERT INTO batches(id, created_at, schema_version, row_count, "
                "file_name, file_sha256, schema_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    created_at,
                    expected_version,
                    row_count,
                    file_name,
                    file_sha256,
                    schema_json,
                ),
            )
    except LakeError:
        raise
    except sqlite3.Error as exc:
        raise StorageError("批次登记失败，未写入新批次。") from exc
    return Batch(
        id=batch_id,
        created_at=created_at,
        schema_version=expected_version,
        row_count=row_count,
        file_name=file_name,
        file_sha256=file_sha256,
        fields_at_upload=tuple(fields_at_upload),
    )


def get_batch(settings: Settings, batch_id: str) -> BatchView:
    """读取批次；结构、批次登记与区域时间轴来自同一个已提交读事务。"""

    try:
        with _read_transaction(settings) as conn:
            schema = _query_schema(conn)
            row = conn.execute(
                "SELECT id, created_at, schema_version, row_count, file_name, "
                "file_sha256, schema_json FROM batches WHERE id = ?",
                (batch_id,),
            ).fetchone()
            timelines = (
                _query_region_timelines(conn) if row is not None else ()
            )
    except sqlite3.Error as exc:
        raise StorageError("读取批次失败。") from exc

    if row is None:
        raise NotFound("批次不存在。")
    batch = _rows_to_batch(row)
    physical = _read_physical_table(settings, batch)
    current = _materialize_current_table(physical, schema)
    timelines_by_station = {t.station_id: t for t in timelines}
    assigned = _with_region_column(current, timelines_by_station)
    used = {
        str(station)
        for station in current.column(STATION_COLUMN).combine_chunks().to_pylist()
    }
    regions = {
        station: timelines_by_station[station]
        for station in used
        if station in timelines_by_station
    }
    return BatchView(
        schema=schema,
        batch=batch,
        table=current,
        assignment_table=assigned,
        regions=regions,
    )


def _read_physical_table(settings: Settings, batch: Batch) -> pa.Table:
    path = settings.parquet_dir / batch.file_name
    if not path.is_file():
        raise BatchIntegrityError(
            f"批次 {batch.id[:8]} 的 Parquet 文件缺失：{batch.file_name}"
        )
    try:
        table = pq.read_table(path)
    except (OSError, pa.ArrowException) as exc:
        raise BatchIntegrityError(f"批次 {batch.id[:8]} 的 Parquet 无法读取。") from exc
    for fixed in (STATION_COLUMN, TIME_COLUMN):
        if fixed not in table.column_names:
            raise BatchIntegrityError(f"批次 {batch.id[:8]} 缺少固定列 {fixed}。")
    return table


def _materialize_current_table(physical: pa.Table, schema: Schema) -> pa.Table:
    """按当前已提交结构生成只读视图；不重写历史文件。

    当前字段 id 在历史文件中不存在（新字段，或删除后同名重建）时补 NULL。
    """

    row_count = physical.num_rows
    names = [STATION_COLUMN, TIME_COLUMN]
    arrays: list[pa.Array] = [
        physical.column(STATION_COLUMN).combine_chunks().cast(pa.string()),
        physical.column(TIME_COLUMN).combine_chunks().cast(
            pa.timestamp("us", tz="UTC")
        ),
    ]
    for field in schema.active:
        names.append(field.name)
        if field.id in physical.column_names:
            try:
                column = physical.column(field.id).combine_chunks().cast(pa.int64())
            except (pa.ArrowException, KeyError) as exc:
                raise BatchIntegrityError(
                    f"历史 Parquet 的列 {field.id} 类型异常。"
                ) from exc
        else:
            column = pa.nulls(row_count, type=pa.int64())
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=names)


def csv_bytes(table: pa.Table) -> bytes:
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(table.column_names)
    for record in table.to_pylist():
        row: list[str] = []
        for name in table.column_names:
            value = record[name]
            if value is None:
                row.append("")
            elif isinstance(value, datetime):
                row.append(format_utc(value))
            else:
                row.append(str(value))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def parquet_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def format_utc_precise(value: datetime) -> str:
    """区域边界用：保留微秒精度，保证页面/API 往返不改变已提交区间。"""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    timespec = "microseconds" if value.microsecond else "seconds"
    return value.isoformat(timespec=timespec).replace("+00:00", "Z")


def display_cell(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return format_utc(value)
    return str(value)


def rows_as_display(table: pa.Table) -> tuple[list[str], list[list[str | None]]]:
    columns = list(table.column_names)
    rows = [
        [display_cell(record[name]) for name in columns]
        for record in table.to_pylist()
    ]
    return columns, rows


def sample_csv(schema: Schema, rows: int = 5) -> bytes:
    """按当前结构生成可直接上传的示例 CSV，用于网页演示。"""

    stations = ["54511", "54512", "58362", "50953", "45007"]
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    active = schema.active
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow([STATION_COLUMN, TIME_COLUMN, *[f.name for f in active]])
    for i in range(rows):
        row = [
            stations[i % len(stations)],
            format_utc(base.replace(hour=i % 24, minute=(i * 7) % 60)),
        ]
        for j, _field in enumerate(active):
            # 留一个空值，演示可空整数列
            row.append("" if (i + j) % 4 == 3 else str(10 + (i * 3 + j * 5) % 40))
        writer.writerow(row)
    return out.getvalue().encode("utf-8")


def check_integrity(settings: Settings) -> IntegrityReport:
    state = read_state(settings)
    registered = {b.file_name for b in state.batches}
    missing: list[str] = []
    mismatched: list[str] = []
    try:
        disk_files = {
            p.name for p in settings.parquet_dir.glob("*.parquet") if p.is_file()
        }
        temp_files = sorted(
            p.name
            for p in settings.parquet_dir.iterdir()
            if p.suffix == ".tmp" and p.is_file()
        )
    except OSError as exc:
        raise StorageError("扫描 Parquet 目录失败。") from exc

    for batch in state.batches:
        path = settings.parquet_dir / batch.file_name
        if not path.is_file():
            missing.append(batch.file_name)
            continue
        try:
            if _sha256_file(path) != batch.file_sha256:
                mismatched.append(batch.file_name)
        except OSError:
            missing.append(batch.file_name)

    # 正在转换、尚未登记的新文件是正常瞬时状态；只把 60 秒以上的未登记文件
    # 视为孤立文件，避免并发上传时误报。
    orphan_cutoff = time.time() - 60
    orphans: list[str] = []
    for name in sorted(disk_files - registered):
        try:
            if (settings.parquet_dir / name).stat().st_mtime < orphan_cutoff:
                orphans.append(name)
        except OSError:
            continue
    return IntegrityReport(
        ok=not missing and not mismatched,
        batches=len(state.batches),
        missing_files=tuple(missing),
        hash_mismatches=tuple(mismatched),
        orphan_files=tuple(orphans),
        temp_files=tuple(temp_files),
    )
