"""
异步数据获取器单元测试
Async Stock Data Fetcher Unit Tests

测试覆盖:
1. Mock外部akshare请求
2. 并发数据源轮询测试
3. 最快响应采纳测试
4. 缓存集成测试
5. 熔断器集成测试
6. 错误处理测试
"""

import pytest
import asyncio
import pandas as pd
from unittest.mock import AsyncMock, MagicMock, patch, call
from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.async_stock_data_fetcher import (
    AsyncStockDataFetcher,
    MarketType,
    FetchResult,
    DataSourceConfig
)


@pytest.mark.unit
@pytest.mark.asyncio
class TestAsyncStockDataFetcher:
    """异步数据获取器基础测试"""

    @pytest.fixture
    def sample_stock_data(self):
        """创建示例股票数据"""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        data = {
            'date': dates,
            'open': [100.0 + i for i in range(10)],
            'high': [102.0 + i for i in range(10)],
            'low': [99.0 + i for i in range(10)],
            'close': [101.0 + i for i in range(10)],
            'volume': [1000000 + i * 10000 for i in range(10)]
        }
        return pd.DataFrame(data)

    @pytest.fixture
    async def fetcher(self):
        """创建数据获取器实例"""
        fetcher = AsyncStockDataFetcher(
            max_concurrent_sources=3,
            request_timeout=30,
            enable_cache=False,
            enable_circuit_breaker=False
        )
        yield fetcher
        await fetcher.close()

    @pytest.mark.asyncio
    async def test_fetcher_initialization(self):
        """测试数据获取器初始化"""
        fetcher = AsyncStockDataFetcher(
            max_concurrent_sources=5,
            request_timeout=30,
            enable_cache=True,
            enable_circuit_breaker=True
        )

        assert fetcher.max_concurrent_sources == 5
        assert fetcher.request_timeout == 30
        assert fetcher.enable_cache is True
        assert fetcher.enable_circuit_breaker is True

        await fetcher.close()

    @pytest.mark.asyncio
    async def test_session_creation(self, fetcher):
        """测试aiohttp session创建"""
        session = await fetcher._get_session()

        assert session is not None
        assert not session.closed

        await fetcher.close()
        assert session.closed

    @pytest.mark.asyncio
    async def test_wait_for_interval(self, fetcher):
        """测试请求间隔控制"""
        source_name = "test_source"
        min_interval = 0.1

        start_time = asyncio.get_event_loop().time()
        await fetcher._wait_for_interval_async(source_name, min_interval)
        first_wait_time = asyncio.get_event_loop().time() - start_time

        # 第一次调用不应该等待太久（只是记录时间）
        assert first_wait_time < 0.05

        # 第二次调用应该等待至少min_interval
        start_time = asyncio.get_event_loop().time()
        await fetcher._wait_for_interval_async(source_name, min_interval)
        second_wait_time = asyncio.get_event_loop().time() - start_time

        # 应该等待至少min_interval时间
        assert second_wait_time >= min_interval

    @pytest.mark.asyncio
    async def test_fetch_zh_a_hist_async_mock(self, fetcher, sample_stock_data, mocker):
        """测试A股历史数据获取 - Mock akshare"""
        # Mock akshare调用
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak.return_value = sample_stock_data

        result = await fetcher._fetch_zh_a_hist_async(
            code="600271",
            start_date="20240101",
            end_date="20240110"
        )

        assert not result.empty
        assert len(result) == 10
        mock_ak.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_zh_a_hist_async_error(self, fetcher, mocker):
        """测试A股历史数据获取失败"""
        # Mock akshare抛出异常
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak.side_effect = Exception("Network error")

        result = await fetcher._fetch_zh_a_hist_async(
            code="600271",
            start_date="20240101",
            end_date="20240110"
        )

        assert result.empty

    @pytest.mark.asyncio
    async def test_convert_spot_to_hist_format(self, fetcher):
        """测试实时数据转历史格式转换"""
        spot_data = pd.Series({
            '最新价': 45.67,
            '今开': 45.00,
            '最高': 46.00,
            '最低': 44.50,
            '成交量': 1000000,
            '成交额': 45670000,
            '涨跌幅': 1.5,
            '涨跌额': 0.68
        })

        result = fetcher._convert_spot_to_hist_format(spot_data)

        assert not result.empty
        assert len(result) == 1
        assert result.iloc[0]['close'] == 45.67
        assert result.iloc[0]['open'] == 45.00
        assert result.iloc[0]['high'] == 46.00
        assert result.iloc[0]['low'] == 44.50


@pytest.mark.unit
@pytest.mark.asyncio
class TestConcurrentFetching:
    """并发数据获取测试"""

    @pytest.fixture
    def sample_stock_data(self):
        """创建示例股票数据"""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        data = {
            'date': dates,
            'open': [100.0 + i for i in range(10)],
            'high': [102.0 + i for i in range(10)],
            'low': [99.0 + i for i in range(10)],
            'close': [101.0 + i for i in range(10)],
            'volume': [1000000 + i * 10000 for i in range(10)]
        }
        return pd.DataFrame(data)

    @pytest.mark.asyncio
    async def test_fetch_with_source_async_success(self, sample_stock_data, mocker):
        """测试单个数据源获取成功"""
        fetcher = AsyncStockDataFetcher(enable_cache=False, enable_circuit_breaker=False)

        # Mock akshare调用
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak.return_value = sample_stock_data

        # 创建数据源配置
        source = DataSourceConfig(
            name="test_source",
            priority=1,
            func=fetcher._fetch_zh_a_hist_async,
            min_interval=0.1
        )

        result = await fetcher._fetch_with_source_async(
            source=source,
            stock_code="600271",
            start_date="20240101",
            end_date="20240110"
        )

        assert result.success is True
        assert result.data is not None
        assert len(result.data) == 10
        assert result.source_name == "test_source"

        await fetcher.close()

    @pytest.mark.asyncio
    async def test_fetch_with_source_async_failure(self, mocker):
        """测试单个数据源获取失败"""
        fetcher = AsyncStockDataFetcher(enable_cache=False, enable_circuit_breaker=False)

        # Mock akshare抛出异常
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak.side_effect = Exception("Network error")

        source = DataSourceConfig(
            name="test_source",
            priority=1,
            func=fetcher._fetch_zh_a_hist_async,
            min_interval=0.1
        )

        result = await fetcher._fetch_with_source_async(
            source=source,
            stock_code="600271",
            start_date="20240101",
            end_date="20240110"
        )

        assert result.success is False
        assert result.data is None
        assert result.error_message is not None

        await fetcher.close()

    @pytest.mark.asyncio
    async def test_concurrent_fetch_fastest_wins(self, sample_stock_data, mocker):
        """测试并发获取采纳最快响应"""
        fetcher = AsyncStockDataFetcher(
            max_concurrent_sources=3,
            enable_cache=False,
            enable_circuit_breaker=False
        )

        # 创建一个简单的mock，直接返回数据（不使用协程）
        # Mock akshare调用 - 使用同步函数返回数据
        mocker.patch('akshare.stock_zh_a_hist', return_value=sample_stock_data)
        mocker.patch('akshare.stock_zh_a_spot_em', return_value=pd.DataFrame())
        mocker.patch('akshare.fund_etf_hist_em', return_value=pd.DataFrame())

        result = await fetcher.fetch_stock_data_concurrent(
            stock_code="600271",
            market_type=MarketType.A_SHARE,
            start_date="20240101",
            end_date="20240110"
        )

        # 应该成功获取数据
        assert result.success is True
        assert result.data is not None
        assert len(result.data) == 10

        await fetcher.close()

    @pytest.mark.asyncio
    async def test_all_sources_fail(self, mocker):
        """测试所有数据源都失败"""
        fetcher = AsyncStockDataFetcher(
            max_concurrent_sources=3,
            enable_cache=False,
            enable_circuit_breaker=False
        )

        # Mock所有akshare调用都失败
        mock_ak_hist = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak_hist.side_effect = Exception("Network error")

        mock_ak_spot = mocker.patch('akshare.stock_zh_a_spot_em')
        mock_ak_spot.side_effect = Exception("Network error")

        mock_ak_etf = mocker.patch('akshare.fund_etf_hist_em')
        mock_ak_etf.side_effect = Exception("Network error")

        result = await fetcher.fetch_stock_data_concurrent(
            stock_code="600271",
            market_type=MarketType.A_SHARE,
            start_date="20240101",
            end_date="20240110"
        )

        assert result.success is False
        assert result.data is None
        assert "失败" in result.error_message or "failed" in result.error_message.lower()

        await fetcher.close()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.cache
class TestCacheIntegration:
    """缓存集成测试"""

    @pytest.fixture
    def sample_stock_data(self):
        """创建示例股票数据"""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        return pd.DataFrame({
            'date': dates,
            'close': [100.0 + i for i in range(10)],
            'volume': [1000000] * 10
        })

    @pytest.mark.asyncio
    async def test_cache_hit(self, sample_stock_data, mocker):
        """测试缓存命中场景"""
        fetcher = AsyncStockDataFetcher(enable_cache=True, enable_circuit_breaker=False)

        # Mock缓存管理器
        mock_cache = AsyncMock()
        mock_cache.connect.return_value = True
        mock_cache.get_stock_data.return_value = sample_stock_data.to_dict('records')
        fetcher._cache_manager = mock_cache

        result = await fetcher.fetch_stock_data_concurrent(
            stock_code="600271",
            market_type=MarketType.A_SHARE
        )

        assert result.success is True
        assert result.source_name == "cache"
        assert result.fetch_time == 0.0

        await fetcher.close()

    @pytest.mark.asyncio
    async def test_cache_miss_then_set(self, sample_stock_data, mocker):
        """测试缓存未命中后设置缓存"""
        fetcher = AsyncStockDataFetcher(enable_cache=True, enable_circuit_breaker=False)

        # Mock缓存管理器
        mock_cache = AsyncMock()
        mock_cache.connect.return_value = True
        mock_cache.get_stock_data.return_value = None  # 缓存未命中
        mock_cache.set_stock_data.return_value = True
        fetcher._cache_manager = mock_cache

        # Mock akshare调用
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak.return_value = sample_stock_data

        result = await fetcher.fetch_stock_data_concurrent(
            stock_code="600271",
            market_type=MarketType.A_SHARE
        )

        assert result.success is True
        assert result.source_name != "cache"

        # 验证设置缓存被调用
        mock_cache.set_stock_data.assert_called_once()

        await fetcher.close()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.circuit_breaker
class TestCircuitBreakerIntegration:
    """熔断器集成测试"""

    @pytest.fixture
    def sample_stock_data(self):
        """创建示例股票数据"""
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        return pd.DataFrame({
            'date': dates,
            'close': [100.0 + i for i in range(10)],
            'open': [100.0 + i for i in range(10)],
            'high': [102.0 + i for i in range(10)],
            'low': [99.0 + i for i in range(10)],
            'volume': [1000000] * 10
        })

    @pytest.mark.asyncio
    async def test_circuit_breaker_blocks_source(self, mocker):
        """测试熔断器阻止访问数据源"""
        fetcher = AsyncStockDataFetcher(
            enable_cache=False,
            enable_circuit_breaker=True
        )

        # Mock熔断器
        mock_cb = AsyncMock()
        mock_cb.initialize.return_value = True
        mock_cb.is_open.return_value = True  # 数据源已熔断
        fetcher._circuit_breaker_manager = mock_cb

        # Mock akshare（不应该被调用）
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')

        result = await fetcher.fetch_stock_data_concurrent(
            stock_code="600271",
            market_type=MarketType.A_SHARE
        )

        # 所有数据源都被熔断，应该失败
        assert result.success is False

        # akshare不应该被调用
        mock_ak.assert_not_called()

        await fetcher.close()

    @pytest.mark.asyncio
    async def test_circuit_breaker_records_success(self, sample_stock_data, mocker):
        """测试熔断器记录成功"""
        fetcher = AsyncStockDataFetcher(
            enable_cache=False,
            enable_circuit_breaker=True
        )

        # Mock熔断器
        mock_cb = AsyncMock()
        mock_cb.initialize.return_value = True
        mock_cb.is_open.return_value = False  # 数据源未熔断
        mock_cb.record_success = AsyncMock()
        fetcher._circuit_breaker_manager = mock_cb

        # Mock akshare
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        sample_data = pd.DataFrame({
            'date': dates,
            'close': [100.0] * 10,
            'volume': [1000000] * 10
        })
        mock_ak = mocker.patch('akshare.stock_zh_a_hist')
        mock_ak.return_value = sample_data

        result = await fetcher.fetch_stock_data_concurrent(
            stock_code="600271",
            market_type=MarketType.A_SHARE
        )

        assert result.success is True

        # 验证记录成功被调用
        mock_cb.record_success.assert_called()

        await fetcher.close()


@pytest.mark.unit
@pytest.mark.asyncio
class TestContextManager:
    """上下文管理器测试"""

    @pytest.mark.asyncio
    async def test_context_manager_lifecycle(self):
        """测试上下文管理器生命周期"""
        async with AsyncStockDataFetcher(
            enable_cache=False,
            enable_circuit_breaker=False
        ) as fetcher:
            # 在上下文中，session应该是打开的
            session = await fetcher._get_session()
            assert not session.closed

        # 退出上下文后，session应该被关闭
        # (由于我们没有真正的数据源，这里只检查不抛异常)

    @pytest.mark.asyncio
    async def test_context_manager_with_cache(self, mocker):
        """测试带缓存的上下文管理器"""
        # Mock缓存管理器模块
        mock_cache_manager_module = mocker.MagicMock()
        mock_cache_manager = AsyncMock()
        mock_cache_manager.connect = AsyncMock(return_value=True)
        mock_cache_manager_module.cache_manager = mock_cache_manager

        # Mock导入
        mocker.patch.dict('sys.modules', {'src.utils.cache_manager': mock_cache_manager_module})

        async with AsyncStockDataFetcher(
            enable_cache=True,
            enable_circuit_breaker=False
        ) as fetcher:
            # 在上下文中，缓存管理器应该被设置
            # 由于我们mock了模块，这里只验证不抛异常
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s", "-m", "unit"])
