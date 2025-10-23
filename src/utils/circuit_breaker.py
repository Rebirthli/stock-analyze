"""
熔断器模块 - 动态熔断机制
Circuit Breaker Module - Dynamic Circuit Breaker Mechanism

版本: 1.0
作者: Stock Analysis Team
创建日期: 2025-10-22

主要功能:
    1. 数据源状态跟踪
    2. 自动熔断和恢复
    3. 半开放状态探测
    4. 状态转换管理
    5. 异步状态存储
"""

import asyncio
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
import structlog

from .cache_manager import CacheManager

logger = structlog.get_logger(__name__)

class CircuitState(Enum):
    """熔断器状态枚举"""
    CLOSED = "CLOSED"      # 关闭状态 - 正常工作
    OPEN = "OPEN"          # 熔断状态 - 暂停使用
    HALF_OPEN = "HALF_OPEN"  # 半开放状态 - 探测恢复

@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5          # 失败阈值 - 连续失败多少次后熔断
    recovery_timeout: int = 60          # 恢复超时时间 - 熔断后多长时间尝试恢复(秒)
    half_open_max_calls: int = 3        # 半开放状态最大调用次数
    success_threshold: int = 2          # 成功阈值 - 半开放状态下成功多少次后恢复
    timeout: int = 30                   # 单次请求超时时间(秒)

@dataclass
class CircuitStatus:
    """熔断器状态信息"""
    state: CircuitState
    failure_count: int
    last_failure_time: Optional[datetime]
    last_success_time: Optional[datetime]
    half_open_calls: int
    half_open_successes: int
    total_requests: int
    total_failures: int
    total_successes: int

class CircuitBreaker:
    """数据源熔断器"""

    def __init__(self,
                 source_name: str,
                 config: CircuitBreakerConfig,
                 cache_manager: CacheManager):
        """
        初始化熔断器

        Args:
            source_name: 数据源名称
            config: 熔断器配置
            cache_manager: 缓存管理器
        """
        self.source_name = source_name
        self.config = config
        self.cache_manager = cache_manager

        # 从缓存加载状态，如果不存在则初始化
        self.status = self._load_status() or CircuitStatus(
            state=CircuitState.CLOSED,
            failure_count=0,
            last_failure_time=None,
            last_success_time=None,
            half_open_calls=0,
            half_open_successes=0,
            total_requests=0,
            total_failures=0,
            total_successes=0
        )

    def _load_status(self) -> Optional[CircuitStatus]:
        """从缓存加载熔断器状态"""
        try:
            cached_status = asyncio.create_task(
                self.cache_manager.get_circuit_breaker_status(self.source_name)
            )
            # 注意：这里在实际使用时需要在异步上下文中调用
            # 为了兼容同步调用，返回None让调用者处理
            return None
        except Exception as e:
            logger.warning("加载熔断器状态失败", source=self.source_name, error=str(e))
            return None

    async def save_status(self):
        """保存熔断器状态到缓存"""
        try:
            status_dict = asdict(self.status)
            # 转换datetime对象为字符串
            if status_dict['last_failure_time']:
                status_dict['last_failure_time'] = self.status.last_failure_time.isoformat()
            if status_dict['last_success_time']:
                status_dict['last_success_time'] = self.status.last_success_time.isoformat()

            await self.cache_manager.set_circuit_breaker_status(self.source_name, status_dict)
            logger.debug("熔断器状态已保存", source=self.source_name, state=self.status.state.value)
        except Exception as e:
            logger.error("保存熔断器状态失败", source=self.source_name, error=str(e))

    async def can_execute(self) -> bool:
        """检查是否可以执行请求"""
        current_time = datetime.now()

        if self.status.state == CircuitState.CLOSED:
            return True

        elif self.status.state == CircuitState.OPEN:
            # 检查是否可以转换为半开放状态
            if (self.status.last_failure_time and
                (current_time - self.status.last_failure_time).total_seconds() >= self.config.recovery_timeout):
                await self._transition_to_half_open()
                return True
            return False

        elif self.status.state == CircuitState.HALF_OPEN:
            # 半开放状态允许有限数量的请求
            return self.status.half_open_calls < self.config.half_open_max_calls

        return False

    async def on_request_start(self):
        """请求开始时的处理"""
        self.status.total_requests += 1
        if self.status.state == CircuitState.HALF_OPEN:
            self.status.half_open_calls += 1
        await self.save_status()

    async def on_success(self):
        """请求成功时的处理"""
        current_time = datetime.now()

        self.status.total_successes += 1
        self.status.last_success_time = current_time

        if self.status.state == CircuitState.CLOSED:
            # 重置失败计数
            self.status.failure_count = 0

        elif self.status.state == CircuitState.HALF_OPEN:
            self.status.half_open_successes += 1
            # 检查是否达到恢复阈值
            if self.status.half_open_successes >= self.config.success_threshold:
                await self._transition_to_closed()

        await self.save_status()

        logger.info("熔断器记录成功",
                   source=self.source_name,
                   state=self.status.state.value,
                   failure_count=self.status.failure_count)

    async def on_failure(self, error: Exception = None):
        """请求失败时的处理"""
        current_time = datetime.now()

        self.status.total_failures += 1
        self.status.failure_count += 1
        self.status.last_failure_time = current_time

        if self.status.state == CircuitState.CLOSED:
            # 检查是否需要熔断
            if self.status.failure_count >= self.config.failure_threshold:
                await self._transition_to_open()

        elif self.status.state == CircuitState.HALF_OPEN:
            # 半开放状态下的失败立即回到熔断状态
            await self._transition_to_open()

        await self.save_status()

        logger.warning("熔断器记录失败",
                      source=self.source_name,
                      state=self.status.state.value,
                      failure_count=self.status.failure_count,
                      error=str(error) if error else "Unknown error")

    async def _transition_to_open(self):
        """转换为熔断状态"""
        self.status.state = CircuitState.OPEN
        logger.warning("熔断器已打开", source=self.source_name, failure_count=self.status.failure_count)

    async def _transition_to_half_open(self):
        """转换为半开放状态"""
        self.status.state = CircuitState.HALF_OPEN
        self.status.half_open_calls = 0
        self.status.half_open_successes = 0
        logger.info("熔断器进入半开放状态", source=self.source_name)

    async def _transition_to_closed(self):
        """转换为关闭状态"""
        self.status.state = CircuitState.CLOSED
        self.status.failure_count = 0
        self.status.half_open_calls = 0
        self.status.half_open_successes = 0
        logger.info("熔断器已关闭", source=self.source_name)

    def get_status_info(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            'source_name': self.source_name,
            'state': self.status.state.value,
            'failure_count': self.status.failure_count,
            'last_failure_time': self.status.last_failure_time.isoformat() if self.status.last_failure_time else None,
            'last_success_time': self.status.last_success_time.isoformat() if self.status.last_success_time else None,
            'half_open_calls': self.status.half_open_calls,
            'half_open_successes': self.status.half_open_successes,
            'total_requests': self.status.total_requests,
            'total_failures': self.status.total_failures,
            'total_successes': self.status.total_successes,
            'success_rate': (
                self.status.total_successes / max(self.status.total_requests, 1) * 100
            ),
            'config': asdict(self.config)
        }

class CircuitBreakerManager:
    """熔断器管理器"""

    def __init__(self, cache_manager: CacheManager):
        """初始化熔断器管理器"""
        self.cache_manager = cache_manager
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.default_config = CircuitBreakerConfig()

    def get_circuit_breaker(self,
                           source_name: str,
                           config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """获取或创建熔断器"""
        if source_name not in self.circuit_breakers:
            breaker_config = config or self.default_config
            self.circuit_breakers[source_name] = CircuitBreaker(
                source_name=source_name,
                config=breaker_config,
                cache_manager=self.cache_manager
            )
        return self.circuit_breakers[source_name]

    async def execute_with_circuit_breaker(self,
                                          source_name: str,
                                          func,
                                          *args,
                                          config: Optional[CircuitBreakerConfig] = None,
                                          **kwargs):
        """使用熔断器执行函数"""
        breaker = self.get_circuit_breaker(source_name, config)

        # 检查是否可以执行
        if not await breaker.can_execute():
            logger.warning("熔断器阻止执行", source=source_name, state=breaker.status.state.value)
            raise CircuitBreakerOpenException(f"熔断器已打开，无法访问数据源: {source_name}")

        # 执行请求
        await breaker.on_request_start()
        start_time = time.time()

        try:
            # 设置超时
            result = await asyncio.wait_for(
                func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else
                asyncio.to_thread(func, *args, **kwargs),
                timeout=breaker.config.timeout
            )

            execution_time = time.time() - start_time
            await breaker.on_success()

            logger.info("熔断器执行成功",
                       source=source_name,
                       execution_time=round(execution_time, 3))

            return result

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            await breaker.on_failure(TimeoutError(f"请求超时: {breaker.config.timeout}s"))
            logger.warning("熔断器执行超时",
                          source=source_name,
                          execution_time=round(execution_time, 3))
            raise

        except Exception as e:
            execution_time = time.time() - start_time
            await breaker.on_failure(e)
            logger.warning("熔断器执行失败",
                          source=source_name,
                          execution_time=round(execution_time, 3),
                          error=str(e))
            raise

    async def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """获取所有熔断器状态"""
        status_dict = {}
        for source_name, breaker in self.circuit_breakers.items():
            status_dict[source_name] = breaker.get_status_info()
        return status_dict

    async def reset_circuit_breaker(self, source_name: str):
        """重置熔断器"""
        if source_name in self.circuit_breakers:
            breaker = self.circuit_breakers[source_name]
            await breaker._transition_to_closed()
            await breaker.save_status()
            logger.info("熔断器已重置", source=source_name)

    async def close_all_circuit_breakers(self):
        """关闭所有熔断器"""
        for source_name, breaker in self.circuit_breakers.items():
            if breaker.status.state != CircuitState.CLOSED:
                await breaker._transition_to_closed()
                await breaker.save_status()
        logger.info("所有熔断器已关闭")

class CircuitBreakerOpenException(Exception):
    """熔断器打开异常"""
    pass

# 装饰器版本
def with_circuit_breaker(source_name: str, config: Optional[CircuitBreakerConfig] = None):
    """熔断器装饰器"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            # 获取缓存管理器
            from .cache_manager import get_cache_manager
            cache_mgr = await get_cache_manager()

            # 创建熔断器管理器
            manager = CircuitBreakerManager(cache_mgr)

            # 执行带熔断器的函数
            return await manager.execute_with_circuit_breaker(
                source_name, func, *args, config=config, **kwargs
            )

        def sync_wrapper(*args, **kwargs):
            # 同步函数的异步包装
            return asyncio.run(async_wrapper(*args, **kwargs))

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator

class CircuitBreakerManagerSingleton:
    """熔断器管理器单例包装类"""

    def __init__(self):
        self._manager: Optional[CircuitBreakerManager] = None
        self._cache_manager = None

    async def initialize(self):
        """初始化熔断器管理器"""
        if self._manager is None:
            from .cache_manager import get_cache_manager
            self._cache_manager = await get_cache_manager()
            self._manager = CircuitBreakerManager(self._cache_manager)
            logger.info("熔断器管理器初始化成功")

    async def is_open(self, source_name: str) -> bool:
        """检查熔断器是否打开"""
        if self._manager is None:
            await self.initialize()
        breaker = self._manager.get_circuit_breaker(source_name)
        can_exec = await breaker.can_execute()
        return not can_exec  # can_execute返回False表示熔断器打开

    async def record_success(self, source_name: str):
        """记录成功"""
        if self._manager is None:
            await self.initialize()
        breaker = self._manager.get_circuit_breaker(source_name)
        await breaker.on_success()

    async def record_failure(self, source_name: str):
        """记录失败"""
        if self._manager is None:
            await self.initialize()
        breaker = self._manager.get_circuit_breaker(source_name)
        await breaker.on_failure()

# 全局熔断器管理器实例
circuit_breaker_manager = CircuitBreakerManagerSingleton()

async def get_circuit_breaker_manager() -> CircuitBreakerManagerSingleton:
    """获取全局熔断器管理器实例"""
    return circuit_breaker_manager

# 使用示例:
# @with_circuit_breaker("akshare_api")
# async def fetch_data():
#     # 数据获取逻辑
#     pass

# 或者直接使用管理器:
# manager = await get_circuit_breaker_manager()
# result = await manager.execute_with_circuit_breaker("akshare_api", fetch_data)