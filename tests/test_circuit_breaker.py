"""
熔断器单元测试 - 修复版
Circuit Breaker Unit Tests - Fixed Version

测试覆盖:
1. 熔断器管理器初始化
2. 熔断器获取和创建
3. 熔断器状态管理
4. 熔断器执行功能
5. 错误处理
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.circuit_breaker import (
    CircuitBreakerManager,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitStatus
)
from src.utils.cache_manager import CacheManager


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerConfig:
    """熔断器配置测试"""

    def test_default_config(self):
        """测试默认配置"""
        config = CircuitBreakerConfig()

        assert config.failure_threshold == 5
        assert config.recovery_timeout == 60
        assert config.half_open_max_calls == 3
        assert config.success_threshold == 2
        assert config.timeout == 30

    def test_custom_config(self):
        """测试自定义配置"""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=120,
            timeout=60
        )

        assert config.failure_threshold == 3
        assert config.recovery_timeout == 120
        assert config.timeout == 60


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerManager:
    """熔断器管理器测试"""

    @pytest.fixture
    async def mock_cache_manager(self):
        """创建mock缓存管理器"""
        cache_mgr = MagicMock(spec=CacheManager)

        # Mock异步方法
        cache_mgr.get = AsyncMock(return_value=None)
        cache_mgr.set = AsyncMock(return_value=True)
        cache_mgr.delete = AsyncMock(return_value=True)

        return cache_mgr

    @pytest.fixture
    def cb_manager(self, mock_cache_manager):
        """创建熔断器管理器实例"""
        return CircuitBreakerManager(cache_manager=mock_cache_manager)

    def test_manager_initialization(self, cb_manager, mock_cache_manager):
        """测试管理器初始化"""
        assert cb_manager.cache_manager == mock_cache_manager
        assert isinstance(cb_manager.default_config, CircuitBreakerConfig)
        assert len(cb_manager.circuit_breakers) == 0

    def test_get_circuit_breaker_create_new(self, cb_manager):
        """测试获取不存在的熔断器会创建新的"""
        source_name = "test_source"

        breaker = cb_manager.get_circuit_breaker(source_name)

        assert isinstance(breaker, CircuitBreaker)
        assert breaker.source_name == source_name
        assert source_name in cb_manager.circuit_breakers

    def test_get_circuit_breaker_reuse_existing(self, cb_manager):
        """测试获取已存在的熔断器会复用"""
        source_name = "test_source"

        breaker1 = cb_manager.get_circuit_breaker(source_name)
        breaker2 = cb_manager.get_circuit_breaker(source_name)

        assert breaker1 is breaker2

    def test_get_circuit_breaker_with_custom_config(self, cb_manager):
        """测试使用自定义配置创建熔断器"""
        source_name = "test_source"
        custom_config = CircuitBreakerConfig(failure_threshold=3)

        breaker = cb_manager.get_circuit_breaker(source_name, config=custom_config)

        assert breaker.config.failure_threshold == 3


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreaker:
    """熔断器基础测试"""

    @pytest.fixture
    async def mock_cache_manager(self):
        """创建mock缓存管理器"""
        cache_mgr = MagicMock(spec=CacheManager)
        cache_mgr.get = AsyncMock(return_value=None)
        cache_mgr.set = AsyncMock(return_value=True)
        cache_mgr.get_circuit_breaker_status = AsyncMock(return_value=None)
        cache_mgr.set_circuit_breaker_status = AsyncMock(return_value=True)
        return cache_mgr

    @pytest.fixture
    def circuit_breaker(self, mock_cache_manager):
        """创建熔断器实例"""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=5,
            success_threshold=2
        )
        return CircuitBreaker(
            source_name="test_source",
            config=config,
            cache_manager=mock_cache_manager
        )

    def test_initial_state(self, circuit_breaker):
        """测试初始状态"""
        assert circuit_breaker.source_name == "test_source"
        assert circuit_breaker.status.state == CircuitState.CLOSED
        assert circuit_breaker.status.failure_count == 0
        assert circuit_breaker.status.total_requests == 0

    @pytest.mark.asyncio
    async def test_can_execute_when_closed(self, circuit_breaker):
        """测试关闭状态可以执行"""
        assert circuit_breaker.status.state == CircuitState.CLOSED
        can_exec = await circuit_breaker.can_execute()
        assert can_exec is True

    @pytest.mark.asyncio
    async def test_record_success(self, circuit_breaker):
        """测试记录成功"""
        await circuit_breaker.on_request_start()
        await circuit_breaker.on_success()

        assert circuit_breaker.status.total_successes == 1
        assert circuit_breaker.status.total_requests == 1
        assert circuit_breaker.status.last_success_time is not None

    @pytest.mark.asyncio
    async def test_record_failure(self, circuit_breaker):
        """测试记录失败"""
        await circuit_breaker.on_request_start()
        await circuit_breaker.on_failure(Exception("测试错误"))

        assert circuit_breaker.status.total_failures == 1
        assert circuit_breaker.status.failure_count == 1
        assert circuit_breaker.status.last_failure_time is not None

    @pytest.mark.asyncio
    async def test_transition_to_open_after_threshold(self, circuit_breaker):
        """测试达到失败阈值后转为熔断状态"""
        # 连续失败3次
        for i in range(3):
            await circuit_breaker.on_request_start()
            await circuit_breaker.on_failure(Exception(f"错误{i}"))

        assert circuit_breaker.status.state == CircuitState.OPEN
        assert circuit_breaker.status.failure_count >= 3

    @pytest.mark.asyncio
    async def test_cannot_execute_when_open(self, circuit_breaker):
        """测试熔断状态不能执行"""
        # 强制设置为OPEN状态
        circuit_breaker.status.state = CircuitState.OPEN
        circuit_breaker.status.last_failure_time = datetime.now()

        can_exec = await circuit_breaker.can_execute()
        assert can_exec is False

    def test_get_status_info(self, circuit_breaker):
        """测试获取状态信息"""
        info = circuit_breaker.get_status_info()

        assert info['source_name'] == "test_source"
        assert info['state'] == CircuitState.CLOSED.value
        assert 'success_rate' in info
        assert 'config' in info


@pytest.mark.unit
@pytest.mark.circuit_breaker
class TestCircuitBreakerExecution:
    """熔断器执行功能测试"""

    @pytest.fixture
    async def mock_cache_manager(self):
        """创建mock缓存管理器"""
        cache_mgr = MagicMock(spec=CacheManager)
        cache_mgr.get = AsyncMock(return_value=None)
        cache_mgr.set = AsyncMock(return_value=True)
        cache_mgr.get_circuit_breaker_status = AsyncMock(return_value=None)
        cache_mgr.set_circuit_breaker_status = AsyncMock(return_value=True)
        return cache_mgr

    @pytest.fixture
    def cb_manager(self, mock_cache_manager):
        """创建熔断器管理器"""
        return CircuitBreakerManager(cache_manager=mock_cache_manager)

    @pytest.mark.asyncio
    async def test_execute_with_circuit_breaker_success(self, cb_manager):
        """测试通过熔断器成功执行"""

        async def successful_function():
            return "success"

        result = await cb_manager.execute_with_circuit_breaker(
            "test_source",
            successful_function
        )

        assert result == "success"

    @pytest.mark.asyncio
    async def test_execute_with_circuit_breaker_failure(self, cb_manager):
        """测试通过熔断器执行失败"""

        async def failing_function():
            raise ValueError("测试错误")

        with pytest.raises(ValueError):
            await cb_manager.execute_with_circuit_breaker(
                "test_source",
                failing_function
            )

    @pytest.mark.asyncio
    async def test_execute_with_timeout(self, cb_manager):
        """测试执行超时"""

        async def slow_function():
            await asyncio.sleep(100)
            return "slow"

        # 使用短超时配置
        config = CircuitBreakerConfig(timeout=0.1)

        with pytest.raises(asyncio.TimeoutError):
            await cb_manager.execute_with_circuit_breaker(
                "test_source",
                slow_function,
                config=config
            )


@pytest.mark.integration
@pytest.mark.redis
@pytest.mark.asyncio
class TestCircuitBreakerIntegration:
    """熔断器集成测试 - 需要Redis"""

    @pytest.mark.asyncio
    async def test_real_circuit_breaker_workflow(self):
        """测试真实熔断器工作流程"""
        from src.utils.cache_manager import CacheManager

        cache_mgr = CacheManager(redis_url="redis://localhost:7219/15")

        try:
            connected = await cache_mgr.connect()
            if not connected:
                pytest.skip("Redis服务未启动")

            cb_manager = CircuitBreakerManager(cache_manager=cache_mgr)

            # 测试成功执行
            async def test_func():
                return "OK"

            result = await cb_manager.execute_with_circuit_breaker(
                "integration_test",
                test_func
            )
            assert result == "OK"

            # 获取熔断器状态
            breaker = cb_manager.get_circuit_breaker("integration_test")
            assert breaker.status.total_successes >= 1

        finally:
            await cache_mgr.disconnect()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-m", "unit"])
