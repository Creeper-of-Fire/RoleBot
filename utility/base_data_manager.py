import asyncio
import json
import logging
import os
import threading
from typing import TypeVar, Generic, Type, Union, Dict, List, Optional, Self, Any

from pydantic import BaseModel

# T 可以是 BaseModel，也可以是普通的 dict 或 list
T = TypeVar("T", bound=Union[BaseModel, Dict, List])

DATA_DIR = "data"


class AsyncJsonDataManager(Generic[T]):
    """
    通用异步数据管理器，基于 Pydantic V2。
    处理文件加载、节流保存和并发锁。
    """
    # 子类必须/可选定义的类属性
    DATA_FILENAME: str
    DATA_MODEL: Optional[Type[T]] = None
    THROTTLE_INTERVAL: float = 3.0

    # 全局单例注册表，Key 是类对象，Value 是该类的唯一实例
    _instances: Dict[Type, Any] = {}

    def __init__(
            self,
            logger: Optional[logging.Logger] = None,
            *args,
            **kwargs
    ):
        # 日志设置：如果没传则按子类类名生成
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        # 自动补全后缀
        filename = self.DATA_FILENAME
        if not filename.endswith(".json"):
            filename += ".json"

        # 构建文件路径
        self.file_path = os.path.join(DATA_DIR, filename)
        # 存储模型类
        self.model_cls = self.DATA_MODEL

        # 初始化数据容器
        # 如果没有提供 model_cls，默认视为 dict
        self.data: T = self.model_cls() if self.model_cls else {}

        # 创建异步锁用于并发控制
        self._lock = asyncio.Lock()

        # 保存任务
        self._save_task: Optional[asyncio.Task] = None

        # 节流相关属性
        self._dirty = False
        self._is_cooling_down = False
        self._throttle_interval = self.THROTTLE_INTERVAL

        # 确保数据目录存在
        os.makedirs(DATA_DIR, exist_ok=True)
        # 加载初始数据
        self.load_data()

    _creation_lock = threading.Lock()

    @classmethod
    def get_instance(cls, logger: Optional[logging.Logger] = None, *args, **kwargs) -> Self:
        """
        获取该类的唯一单例。
        每一个子类都会在 _instances 字典中拥有自己独立的条目。
        """
        # 1. 快速检查：如果实例已存在，直接返回，无需加锁（性能最高）
        if cls in AsyncJsonDataManager._instances:
            return AsyncJsonDataManager._instances[cls]

        # 2. 加锁创建：确保在并发环境下（虽然 discord.py 主要是单线程）不会创建两个实例
        with cls._creation_lock:
            # 3. 再次检查：防止在获取锁的过程中实例已经被其他线程创建
            if cls not in AsyncJsonDataManager._instances:
                # 实例化会调用 __init__，这是同步阻塞操作
                instance = cls(logger=logger, *args, **kwargs)
                AsyncJsonDataManager._instances[cls] = instance

        return AsyncJsonDataManager._instances[cls]

    def load_data(self):
        """同步加载数据（通常在初始化时调用）。"""
        # 如果文件不存在，初始化为空模型
        if not os.path.exists(self.file_path):
            self._reset_data()
            return

        try:
            # 读取文件内容
            with open(self.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if not content:
                    self._reset_data()
                else:
                    if self.model_cls:
                        # Pydantic 模式
                        self.data = self.model_cls.model_validate_json(content)
                    else:
                        # 原生 Dict/List 模式
                        self.data = json.loads(content)
        except (ValueError, OSError) as e:
            # 出现错误时打印日志并使用空模型
            print(f"加载文件 {self.file_path} 时出错: {e}。使用默认空数据。")
            self._reset_data()

    def _reset_data(self):
        """重置数据为模型的默认状态。"""
        if self.model_cls:
            try:
                # 尝试用空数据触发 Pydantic 的默认值填充
                # 这比直接调用 self.model_cls() 更鲁棒
                self.data = self.model_cls.model_validate({})
            except Exception:
                # 如果模型某些字段是必填且没默认值，则回退到直接实例化
                self.data = self.model_cls()
        else:
            self.data = {}

    def _serialize_data(self) -> str:
        """根据模式将数据序列化为 JSON 字符串。"""
        if self.model_cls and isinstance(self.data, BaseModel):
            # Pydantic V2 序列化
            return self.data.model_dump_json(indent=4)
        else:
            # 原生 JSON 序列化
            return json.dumps(self.data, indent=4, ensure_ascii=False)

    def _write_to_file_sync(self, content: str):
        """同步写入逻辑（在线程池中执行）。只负责写磁盘。"""
        temp_file = f"{self.file_path}.tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(content)
            os.replace(temp_file, self.file_path)
        except Exception as e:
            # 在子线程中出错需要捕获，否则可能导致整个程序崩溃
            self.logger.error(f"[DataManager] 物理写入失败 {self.file_path}: {e}")
            # 写入失败时尝试删除临时文件
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as cleanup_error:
                self.logger.error(f"[DataManager] 清理临时文件失败 {temp_file}: {cleanup_error}")

    async def _write_to_file(self, content: str):
        """异步写入逻辑。"""
        await asyncio.to_thread(self._write_to_file_sync, content)

    async def _background_save_loop(self):
        """后台保存循环，处理节流。"""
        try:
            while self._dirty:
                # 等待节流时间
                await asyncio.sleep(self._throttle_interval)

                # 获取锁并写入
                async with self._lock:
                    # 标记为 False 必须在写入前（或写入中），
                    # 这样如果在写入期间又有新的 save_data 调用，_dirty 会再次变 True，循环继续
                    if not self._dirty:
                        continue
                    self._dirty = False
                    # 在持有锁的情况下完成序列化，保证此时数据不会被其他任务修改
                    content = self._serialize_data()
                    # 在线程池中执行 IO，避免阻塞事件循环
                    await self._write_to_file(content)

        except asyncio.CancelledError:
            # 如果被取消，尝试最后保存一次
            if self._dirty:
                content = self._serialize_data()
                await self._write_to_file(content)
        except Exception as e:
            print(f"[DataManager] 后台保存出错 {self.file_path}: {e}")
        finally:
            self._save_task = None

    async def save_data(self):
        """
        请求保存数据（同步方法，非阻塞）。
        调用此方法会标记数据为脏，并确保后台保存任务正在运行。
        """
        self._dirty = True
        if self._save_task is None:
            # 获取当前的事件循环来调度任务
            try:
                loop = asyncio.get_running_loop()
                self._save_task = loop.create_task(self._background_save_loop())
            except RuntimeError:
                # 如果没有运行的 loop（比如在脚本测试中），则无法调度后台任务
                # 这种情况下通常意味着需要手动处理，或者直接同步写
                print(f"[DataManager] 警告: 在没有事件循环的环境下调用了 save_data，执行同步写入。")
                content = self._serialize_data()
                self._write_to_file_sync(content)
                self._dirty = False

    async def force_save(self):
        """强制立即保存（异步）。"""
        async with self._lock:
            # 无论 _dirty 与否，都执行一次强制物理保存
            content = self._serialize_data()
            await self._write_to_file(content)
            self._dirty = False

    async def clear_all_data(self):
        """重置所有数据并删除文件。"""
        async with self._lock:
            # 取消正在运行的保存任务
            if self._save_task:
                self._save_task.cancel()
                try:
                    await self._save_task
                except asyncio.CancelledError:
                    pass
                self._save_task = None

            self._reset_data()
            self._dirty = False
            # 如果文件存在则删除
            if os.path.exists(self.file_path):
                os.remove(self.file_path)

            # 删除临时文件（如果存在）
            temp_file = f"{self.file_path}.tmp"
            if os.path.exists(temp_file):
                os.remove(temp_file)
