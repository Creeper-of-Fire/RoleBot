"""通用 TOML + pydantic 配置管理器。

设计目标：
- toml 是 admin 编辑的脚本，bot 是**只读消费者**——不在磁盘上"替 admin 决定"。
- bot 启动时从 toml 读取；admin 通过 `/上传配置` 命令覆盖；其余时间 bot 不动 toml。

读 / 写 / 验证分工：
- 读：tomllib（标准库，快、严格）
- 验证：pydantic（schema 化、嵌套结构 + 跨字段约束）
- 写：`validate_and_save` 接受 admin 上传的 bytes，**直接原样落盘**
  （仅剥 UTF-8 BOM）。不经过 tomlkit 重新 dump——避免丢注释 / 空行 / 格式选择。
- 唯一性：sha256 用于乐观锁（write-time prefix check 防 TOCTOU）

如果 toml 文件不存在：
- `load()` 直接返回 pydantic 默认值（in-memory），**不**创建空 toml。
  这意味着 admin 必须主动 `/上传配置`，bot 不会替它"先存一份空的"。
"""

from __future__ import annotations

import hashlib
import logging
import tomllib
from pathlib import Path
from typing import Generic, TypeVar

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# UTF-8 BOM 的字节签名
_UTF8_BOM = b"\xef\xbb\xbf"


class HashMismatchError(Exception):
    """write-time hash check 失败：expected_hash（用户提供）的前 12 字符
    与当前磁盘文件 sha256 的前 12 字符不匹配。

    抛出此异常意味着 TOCTOU——用户上传期间，磁盘文件已被别人更新。
    manager 不会写入；调用方应提示用户重新 `/下载配置` 后再上传。
    """

    def __init__(self, *, expected: str, current: str) -> None:
        super().__init__(
            f"hash mismatch: expected prefix {expected!r} != current {current!r}"
        )
        self.expected = expected
        self.current = current


class TomlConfigManager(Generic[T]):
    """TOML + pydantic 配置管理器。

    每个 guild 一份 toml 文件；manager 持有 data_dir、文件名模板、模型类。
    业务调用 `load(guild_id)` / `validate_and_save(raw, guild_id)`。

    写盘的**唯一**入口是 `validate_and_save`——它会原样落盘用户上传的 bytes。
    本 manager **不**主动把 pydantic 默认值 serialize 成 toml 落盘；
    缺 toml 时 admin 应通过 `/上传配置` 自己写一份。
    """

    def __init__(
        self,
        data_dir: Path | str,
        filename_pattern: str,
        model_class: type[T],
        *,
        doc_path: Path | str,
    ) -> None:
        """初始化。

        Args:
            data_dir: toml 文件存放目录（自动 mkdir，只是为了让 `read_raw` 能跑通，
                不创建任何 toml 文件本身）。
            filename_pattern: 文件名模板，必须含 `{guild_id}` 占位符。
                例如 `honor_{guild_id}.toml` →
                `data/honor_1134557553011998840.toml`
            model_class: pydantic 模型类（用作 schema 验证）。
            doc_path: AI 友好的 doc.md 路径（建议放仓库 `docs/` 下）。
                必选；文件不存在时 download handler 会提示"请联系管理员"。
        """
        self.data_dir = Path(data_dir)
        self.filename_pattern = filename_pattern
        self.model_class = model_class
        self.doc_path = Path(doc_path)

        self.data_dir.mkdir(parents=True, exist_ok=True)

    # --- 路径 ---

    def path_for(self, guild_id: int) -> Path:
        """返回 guild 对应的 toml 文件绝对路径（不一定存在）。"""
        return self.data_dir / self.filename_pattern.format(guild_id=guild_id)

    # --- 读 ---

    def read_raw(self, guild_id: int) -> bytes | None:
        """读取 toml 文件原始字节，不存在返回 None。"""
        path = self.path_for(guild_id)
        if not path.exists():
            return None
        return path.read_bytes()

    def read_doc(self) -> str | None:
        """读取 doc.md 文本。

        doc_path 为 None 或文件不存在 → None。
        """
        if self.doc_path is None or not self.doc_path.exists():
            return None
        return self.doc_path.read_text(encoding="utf-8")

    def content_hash(self, guild_id: int) -> str | None:
        """当前 toml 文件的 sha256 hex digest，不存在返回 None。

        用于上传时的乐观锁校验。
        """
        raw = self.read_raw(guild_id)
        if raw is None:
            return None
        return hashlib.sha256(raw).hexdigest()

    # --- 加载（只读） ---

    def load(self, guild_id: int) -> T:
        """从 toml 加载 + pydantic 验证。

        文件不存在 → 直接返回 `model_class()` 的 in-memory 默认值，**不**创建文件。
        文件存在 → 解析 + 验证，返回配置对象。

        Raises:
            ValidationError: 文件内容无法通过 schema 验证。
            ValueError: TOML 解析失败。
        """
        raw = self.read_raw(guild_id)
        if raw is None:
            logger.info(
                "guild %s 配置文件不存在，返回 pydantic 默认值（不写盘）: %s",
                guild_id,
                self.path_for(guild_id),
            )
            return self.model_class()

        return self._parse(raw, guild_id)

    # --- 校验 + 落盘（admin 上传入口） ---

    def validate_and_save(
        self,
        raw: bytes,
        guild_id: int,
        *,
        expected_hash: str | None = None,
    ) -> T:
        """从原始字节解析 + pydantic 验证 + write-time hash 校验 + **原样落盘**。

        Args:
            raw: toml 文件字节（admin 通过 `/上传配置` 上传的内容）。
            guild_id: 目标 guild。
            expected_hash: 用户填的 hash_str（前 12 字符即可）。
                - 本地无配置：忽略（首次上传不需要校验）。
                - 本地有配置 + 提供：保存瞬间校验 toml 磁盘 hash 前 12 字符
                  与 expected_hash 前 12 字符一致（write-time atomic，
                  防 TOCTOU）。
                - 本地有配置 + 未提供：视为缺失，本方法不抛——由 handler
                  在 read-time 检测并拒绝。

        顺序：
        1. pydantic 验证（先验证 toml 合法性，否则 hash check 通过但 toml 烂也没意义）
        2. write-time hash 检查（如果本地有配置）
        3. 上述都过才落盘——**写 admin 上传的 bytes 原样**，剥 UTF-8 BOM 后落到磁盘。
           不走 tomlkit dump，因此保留 admin 写的所有注释 / 空行 / 格式选择。

        Raises:
            ValidationError: pydantic 验证失败。
            ValueError: TOML 解析失败。
            HashMismatchError: write-time hash check 失败——不写入。
        """
        # 1. pydantic 验证
        config = self._parse(raw, guild_id)

        # 2. write-time hash check（只在本地已有配置 + 用户给了 hash_str 时）
        if expected_hash is not None:
            current_hash = self.content_hash(guild_id)
            if current_hash is not None:
                expected_lower = expected_hash.strip().lower()[:12]
                if expected_lower and not current_hash.lower().startswith(
                    expected_lower
                ):
                    # 不写入
                    raise HashMismatchError(
                        expected=expected_lower,
                        current=current_hash[:12],
                    )

        # 3. 原样落盘（剥 UTF-8 BOM 即可）
        path = self.path_for(guild_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_strip_utf8_bom(raw))
        logger.info("guild %s 配置已写入: %s", guild_id, path)

        return config

    # --- private ---

    def _parse(self, raw: bytes, guild_id: int) -> T:
        """从字节流解析 + 验证。

        用 `utf-8-sig` 解码：自动剥 UTF-8 BOM（PowerShell `Set-Content -Encoding UTF8`
        默认会写 BOM；不剥掉的话 tomllib 第一个字符会解析失败）。
        """
        try:
            data = tomllib.loads(raw.decode("utf-8-sig"))
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"TOML 解析失败（guild {guild_id}）: {e}") from e

        try:
            return self.model_class.model_validate(data)
        except ValidationError as e:
            logger.error("配置验证失败（guild %s）:\n%s", guild_id, e)
            raise


def _strip_utf8_bom(raw: bytes) -> bytes:
    """剥 UTF-8 BOM 字节（如果存在）。

    PowerShell `Set-Content -Encoding UTF8` 默认会写 BOM；tomllib 用 utf-8-sig
    解码能消化 BOM，但写到磁盘后下次再读 sha256 / 看文件头会多 3 字节噪声，
    没必要。剥掉首字节即可，原始内容不动。
    """
    if raw.startswith(_UTF8_BOM):
        return raw[len(_UTF8_BOM):]
    return raw


__all__ = ["TomlConfigManager", "HashMismatchError"]
