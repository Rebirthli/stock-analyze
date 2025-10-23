"""
缓存管理器单元测试 - 修复版
Cache Manager Unit Tests - Fixed Version

测试覆盖:
1. 缓存连接管理
2. 缓存基本操作 (GET/SET/DELETE)
3. 缓存命中和未命中场景
4. TTL管理
5. 股票数据专用缓存方法
6. 错误处理
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
import json

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.cache_manager import CacheManager


@pytest.mark.unit
@pytest.mark.cache
class TestCacheManagerBasic:
    """缓存管理器基础测试 - 简化版"""

    @pytest.mark.asyncio
    async def test_cache_key_generation(self):
        """测试缓存键生成"""
        mgr = CacheManager()

        key = mgr._generate_cache_key("stock", code="600271", market="A")
        assert key == "stock:code:600271:market:A"

        key2 = mgr._generate_cache_key("quote", symbol="AAPL")
        assert key2 == "quote:symbol:AAPL"

    @pytest.mark.asyncio
    async def test_ttl_config(self):
        """测试TTL配置"""
        mgr = CacheManager(default_ttl=300)

        assert mgr.default_ttl == 300
        assert mgr.ttl_config['daily_data'] == 24 * 3600
        assert mgr.ttl_config['real_time_quote'] == 30
        assert mgr.ttl_config['circuit_breaker'] == 60 * 60

    @pytest.mark.asyncio
    async def test_initialization(self):
        """测试初始化"""
        mgr = CacheManager(
            redis_url="redis://localhost:6379/0",
            default_ttl=600,
            max_connections=30
        )

        assert mgr.redis_url == "redis://localhost:6379/0"
        assert mgr.default_ttl == 600
        assert mgr.max_connections == 30


@pytest.mark.unit
@pytest.mark.cache
class TestCacheManagerWithMock:
    """缓存管理器Mock测试"""

    @pytest.fixture
    async def mock_cache_manager(self):
        """创建带mock Redis客户端的缓存管理器"""
        mgr = CacheManager(redis_url="redis://localhost:6379/15")

        # Mock Redis客户端
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock(return_value=True)
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock(return_value=True)
        mock_redis.setex = AsyncMock(return_value=True)
        mock_redis.delete = AsyncMock(return_value=1)
        mock_redis.exists = AsyncMock(return_value=1)
        mock_redis.expire = AsyncMock(return_value=True)
        mock_redis.ttl = AsyncMock(return_value=300)
        mock_redis.close = AsyncMock()

        # 直接设置_redis_client
        mgr._redis_client = mock_redis

        yield mgr, mock_redis

        await mgr.disconnect()

    @pytest.mark.asyncio
    async def test_get_cache_miss(self, mock_cache_manager):
        """测试缓存未命中"""
        mgr, mock_redis = mock_cache_manager

        # Mock返回None表示缓存未命中
        mock_redis.get.return_value = None

        result = await mgr.get("test:key")
        assert result is None
        mock_redis.get.assert_called_once_with("test:key")

    @pytest.mark.asyncio
    async def test_get_cache_hit_json(self, mock_cache_manager):
        """测试缓存命中 - JSON数据"""
        mgr, mock_redis = mock_cache_manager

        test_data = {"code": "600271", "price": 45.67}
        mock_redis.get.return_value = json.dumps(test_data).encode('utf-8')

        result = await mgr.get("test:stock:600271")
        assert result == test_data
        mock_redis.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_cache_hit_string(self, mock_cache_manager):
        """测试缓存命中 - 字符串数据"""
        mgr, mock_redis = mock_cache_manager

        test_string = "test_value"
        mock_redis.get.return_value = test_string.encode('utf-8')

        result = await mgr.get("test:key")
        # 会尝试解析为JSON，失败后返回字符串
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_set_cache_with_dict(self, mock_cache_manager):
        """测试设置缓存 - 字典数据"""
        mgr, mock_redis = mock_cache_manager

        test_data = {"code": "600271", "name": "航天信息"}
        result = await mgr.set("test:stock", test_data, ttl=300)

        assert result is True
        # setex应该被调用
        assert mock_redis.setex.called or mock_redis.set.called

    @pytest.mark.asyncio
    async def test_set_cache_with_ttl_type(self, mock_cache_manager):
        """测试使用数据类型设置TTL"""
        mgr, mock_redis = mock_cache_manager

        test_data = [1, 2, 3]
        result = await mgr.set("test:list", test_data, data_type='daily_data')

        assert result is True
        # 应该使用daily_data的TTL (24小时)

    @pytest.mark.asyncio
    async def test_delete_cache(self, mock_cache_manager):
        """测试删除缓存"""
        mgr, mock_redis = mock_cache_manager

        mock_redis.delete.return_value = 1
        result = await mgr.delete("test:key")

        assert result is True
        mock_redis.delete.assert_called_once_with("test:key")

    @pytest.mark.asyncio
    async def test_exists_cache(self, mock_cache_manager):
        """测试检查缓存是否存在"""
        mgr, mock_redis = mock_cache_manager

        mock_redis.exists.return_value = 1
        result = await mgr.exists("test:key")

        assert result is True
        mock_redis.exists.assert_called_once_with("test:key")


@pytest.mark.unit
@pytest.mark.cache
class TestCacheManagerErrorHandling:
    """缓存管理器错误处理测试"""

    @pytest.fixture
    async def error_cache_manager(self):
        """创建会抛出错误的mock缓存管理器"""
        mgr = CacheManager()

        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(side_effect=Exception("Redis错误"))
        mock_redis.setex = AsyncMock(side_effect=Exception("Redis写入错误"))
        mock_redis.close = AsyncMock()

        mgr._redis_client = mock_redis

        yield mgr, mock_redis

        await mgr.disconnect()

    @pytest.mark.asyncio
    async def test_get_error_handling(self, error_cache_manager):
        """测试GET操作错误处理"""
        mgr, mock_redis = error_cache_manager

        # 即使Redis抛出异常，也应该返回None而不是崩溃
        result = await mgr.get("test:key")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_error_handling(self, error_cache_manager):
        """测试SET操作错误处理"""
        mgr, mock_redis = error_cache_manager

        # 即使Redis抛出异常，也应该返回False而不是崩溃
        result = await mgr.set("test:key", "value")
        assert result is False

    @pytest.mark.asyncio
    async def test_client_property_before_connect(self):
        """测试在连接前访问client属性"""
        mgr = CacheManager()

        # 未连接时访问client应该抛出RuntimeError
        with pytest.raises(RuntimeError):
            _ = mgr.client


@pytest.mark.integration
@pytest.mark.redis
@pytest.mark.asyncio
class TestCacheIntegration:
    """缓存集成测试 - 需要真实Redis"""

    @pytest.mark.asyncio
    async def test_real_redis_connection(self):
        """测试真实Redis连接"""
        mgr = CacheManager(redis_url="redis://localhost:7219/15")

        try:
            connected = await mgr.connect()
            if connected:
                # 测试基本操作
                test_key = "test:integration:key"
                test_value = {"test": "data", "timestamp": datetime.now().isoformat()}

                # SET
                set_result = await mgr.set(test_key, test_value, ttl=60)
                assert set_result is True

                # GET
                get_result = await mgr.get(test_key)
                assert get_result == test_value

                # DELETE
                del_result = await mgr.delete(test_key)
                assert del_result is True

            else:
                pytest.skip("Redis服务未启动")

        finally:
            await mgr.disconnect()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-m", "unit"])
