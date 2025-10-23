"""
异步股票数据获取器 - 基于aiohttp的并发数据获取
Async Stock Data Fetcher - Concurrent Data Fetching with aiohttp

版本: 1.0
作者: Stock Analysis Team
创建日期: 2025-10-22

主要功能:
    1. 异步并发数据获取
    2. 多数据源并发轮询
    3. 采纳最快响应的数据源
    4. 智能取消慢速请求
    5. Redis缓存集成
    6. 熔断器状态管理

技术特点:
    - 基于aiohttp的异步HTTP请求
    - asyncio.gather并发执行
    - asyncio.wait_for超时控制
    - 自动取消未完成的任务
"""

import asyncio
import time
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Callable
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
import aiohttp
import structlog

logger = structlog.get_logger(__name__)


class MarketType(Enum):
    """市场类型枚举"""
    A_SHARE = "A"      # A股
    HK_STOCK = "HK"    # 港股
    US_STOCK = "US"    # 美股
    ETF = "ETF"        # ETF
    LOF = "LOF"        # LOF


@dataclass
class DataSourceConfig:
    """数据源配置"""
    name: str
    priority: int
    func: Callable
    min_interval: float = 0.5
    max_retries: int = 3
    timeout: int = 30
    requires_date_format: str = None


@dataclass
class FetchResult:
    """数据获取结果"""
    success: bool
    data: Optional[pd.DataFrame]
    source_name: str
    fetch_time: float
    error_message: Optional[str] = None


class AsyncStockDataFetcher:
    """
    异步股票数据获取器

    主要特性:
    1. 异步并发数据获取
    2. 多数据源并发轮询
    3. 采纳最快响应
    4. 智能取消慢速请求
    5. Redis缓存集成
    """

    def __init__(self,
                 max_concurrent_sources: int = 5,
                 request_timeout: int = 30,
                 enable_cache: bool = True,
                 enable_circuit_breaker: bool = True):
        """
        初始化异步数据获取器

        Args:
            max_concurrent_sources: 最大并发数据源数量
            request_timeout: 请求超时时间(秒)
            enable_cache: 是否启用缓存
            enable_circuit_breaker: 是否启用熔断器
        """
        self.max_concurrent_sources = max_concurrent_sources
        self.request_timeout = request_timeout
        self.enable_cache = enable_cache
        self.enable_circuit_breaker = enable_circuit_breaker

        # aiohttp session
        self._session: Optional[aiohttp.ClientSession] = None

        # 缓存管理器和熔断器(延迟导入)
        self._cache_manager = None
        self._circuit_breaker_manager = None

        # 请求时间记录
        self.last_request_time: Dict[str, float] = {}

        # 数据源配置
        self._setup_data_sources()

        logger.info("异步股票数据获取器初始化完成",
                   max_concurrent=max_concurrent_sources,
                   timeout=request_timeout,
                   cache_enabled=enable_cache,
                   circuit_breaker_enabled=enable_circuit_breaker)

    def _setup_data_sources(self):
        """配置数据源 - 与原版本保持一致但使用异步函数"""
        self.data_sources = {
            MarketType.A_SHARE: [
                DataSourceConfig(
                    name="stock_zh_a_hist",
                    priority=1,
                    func=self._fetch_zh_a_hist_async,
                    min_interval=0.6,
                    max_retries=4
                ),
                DataSourceConfig(
                    name="stock_zh_a_spot_em",
                    priority=2,
                    func=self._fetch_zh_a_spot_em_async,
                    min_interval=0.5,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_zh_a_hist_em",
                    priority=3,
                    func=self._fetch_zh_a_hist_em_async,
                    min_interval=0.5,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_zh_a_efinance",
                    priority=4,
                    func=self._fetch_zh_a_efinance_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_zh_a_qstock",
                    priority=5,
                    func=self._fetch_zh_a_qstock_async,
                    min_interval=0.7,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_zh_a_baostock",
                    priority=6,
                    func=self._fetch_zh_a_baostock_async,
                    min_interval=0.5,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_zh_a_hist_sina",
                    priority=7,
                    func=self._fetch_zh_a_sina_async,
                    min_interval=0.4,
                    max_retries=3
                ),
            ],
            MarketType.HK_STOCK: [
                DataSourceConfig(
                    name="stock_hk_hist_fixed",
                    priority=1,
                    func=self._fetch_hk_stock_hist_async,
                    min_interval=0.5,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_hk_spot_em_fixed",
                    priority=2,
                    func=self._fetch_hk_stock_spot_async,
                    min_interval=0.4,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_hk_efinance",
                    priority=3,
                    func=self._fetch_hk_efinance_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_hk_qstock",
                    priority=4,
                    func=self._fetch_hk_qstock_async,
                    min_interval=0.7,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_hk_yfinance",
                    priority=5,
                    func=self._fetch_hk_yfinance_async,
                    min_interval=0.8,
                    max_retries=3
                ),
            ],
            MarketType.US_STOCK: [
                DataSourceConfig(
                    name="stock_us_daily",
                    priority=1,
                    func=self._fetch_us_daily_async,
                    min_interval=0.4,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_us_efinance",
                    priority=2,
                    func=self._fetch_us_efinance_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_us_qstock",
                    priority=3,
                    func=self._fetch_us_qstock_async,
                    min_interval=0.7,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="stock_us_yfinance",
                    priority=4,
                    func=self._fetch_us_yfinance_async,
                    min_interval=0.8,
                    max_retries=3
                ),
            ],
            MarketType.ETF: [
                DataSourceConfig(
                    name="fund_etf_hist_em_v2",
                    priority=1,
                    func=self._fetch_etf_hist_em_async,
                    min_interval=0.5,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="fund_etf_efinance",
                    priority=2,
                    func=self._fetch_etf_efinance_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="etf_as_stock_zh_a_hist",
                    priority=3,
                    func=self._fetch_etf_as_stock_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="etf_as_stock_spot",
                    priority=4,
                    func=self._fetch_etf_spot_async,
                    min_interval=0.5,
                    max_retries=3
                ),
            ],
            MarketType.LOF: [
                DataSourceConfig(
                    name="fund_lof_hist_em_v2",
                    priority=1,
                    func=self._fetch_lof_hist_em_async,
                    min_interval=0.5,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="fund_lof_efinance",
                    priority=2,
                    func=self._fetch_lof_efinance_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="lof_as_stock_zh_a_hist",
                    priority=3,
                    func=self._fetch_lof_as_stock_async,
                    min_interval=0.6,
                    max_retries=3
                ),
                DataSourceConfig(
                    name="lof_as_stock_spot",
                    priority=4,
                    func=self._fetch_lof_spot_async,
                    min_interval=0.5,
                    max_retries=3
                ),
            ]
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建aiohttp session"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.request_timeout)
            connector = aiohttp.TCPConnector(
                limit=50,  # 最大连接数
                limit_per_host=10,  # 每个主机最大连接数
                ttl_dns_cache=300,  # DNS缓存时间
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
            )
        return self._session

    async def close(self):
        """关闭session"""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("aiohttp session已关闭")

    async def _wait_for_interval_async(self, source_name: str, min_interval: float):
        """异步等待请求间隔"""
        current_time = time.time()
        last_time = self.last_request_time.get(source_name, 0)
        elapsed = current_time - last_time

        if elapsed < min_interval:
            wait_time = min_interval - elapsed + random.uniform(0.0, 0.2)
            logger.debug(f"等待 {wait_time:.2f} 秒以避免请求过于频繁", source=source_name)
            await asyncio.sleep(wait_time)

        self.last_request_time[source_name] = time.time()

    async def _check_cache_async(self,
                                 stock_code: str,
                                 market_type: MarketType,
                                 start_date: str,
                                 end_date: str) -> Optional[pd.DataFrame]:
        """异步检查缓存"""
        if not self.enable_cache or self._cache_manager is None:
            return None

        try:
            cached_data = await self._cache_manager.get_stock_data(
                stock_code=stock_code,
                market_type=market_type.value,
                start_date=start_date,
                end_date=end_date
            )

            if cached_data:
                logger.info("缓存命中",
                           stock_code=stock_code,
                           market=market_type.value)
                # 将缓存数据转换为DataFrame
                return pd.DataFrame(cached_data)

            return None
        except Exception as e:
            logger.warning("缓存查询失败", error=str(e))
            return None

    async def _set_cache_async(self,
                               stock_code: str,
                               market_type: MarketType,
                               start_date: str,
                               end_date: str,
                               data: pd.DataFrame):
        """异步设置缓存"""
        if not self.enable_cache or self._cache_manager is None:
            return

        try:
            # 将DataFrame转换为字典格式
            data_dict = data.to_dict('records')
            await self._cache_manager.set_stock_data(
                stock_code=stock_code,
                market_type=market_type.value,
                start_date=start_date,
                end_date=end_date,
                data=data_dict
            )
            logger.debug("数据已缓存",
                        stock_code=stock_code,
                        market=market_type.value,
                        records=len(data))
        except Exception as e:
            logger.warning("缓存设置失败", error=str(e))

    async def _check_circuit_breaker_async(self, source_name: str) -> bool:
        """异步检查熔断器状态"""
        if not self.enable_circuit_breaker or self._circuit_breaker_manager is None:
            return False  # 不熔断

        try:
            is_open = await self._circuit_breaker_manager.is_open(source_name)
            if is_open:
                logger.warning("数据源已熔断，跳过", source=source_name)
                return True
            return False
        except Exception as e:
            logger.warning("熔断器检查失败", error=str(e))
            return False

    async def _record_success_async(self, source_name: str):
        """异步记录成功请求"""
        if self._circuit_breaker_manager:
            try:
                await self._circuit_breaker_manager.record_success(source_name)
            except Exception as e:
                logger.warning("记录成功失败", error=str(e))

    async def _record_failure_async(self, source_name: str):
        """异步记录失败请求"""
        if self._circuit_breaker_manager:
            try:
                await self._circuit_breaker_manager.record_failure(source_name)
            except Exception as e:
                logger.warning("记录失败失败", error=str(e))

    # ===== 异步数据获取方法(将在下一步实现具体的akshare调用) =====

    async def _fetch_zh_a_hist_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取A股历史数据"""
        # 这里需要包装akshare的同步调用为异步
        # 使用asyncio.to_thread在线程池中执行同步代码
        try:
            import akshare as ak
            df = await asyncio.to_thread(
                ak.stock_zh_a_hist,
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as e:
            logger.warning(f"东方财富A股历史数据接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_zh_a_spot_em_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取A股实时行情数据"""
        try:
            import akshare as ak
            df_spot = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if df_spot is not None and not df_spot.empty:
                stock_data = df_spot[df_spot['代码'] == code]
                if not stock_data.empty:
                    return self._convert_spot_to_hist_format(stock_data.iloc[0])
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"东方财富A股实时接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_zh_a_hist_em_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取A股历史数据 - 备用"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(
                ak.fund_etf_hist_em,
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as e:
            logger.warning(f"东方财富A股备用接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_hk_stock_hist_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取港股历史数据"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(ak.stock_hk_hist, symbol=code)
            if df is not None and not df.empty and len(df) > 0:
                # 过滤日期范围
                if '日期' in df.columns:
                    df['日期'] = pd.to_datetime(df['日期'])
                    start_dt = pd.to_datetime(start_date)
                    end_dt = pd.to_datetime(end_date)
                    df_filtered = df[(df['日期'] >= start_dt) & (df['日期'] <= end_dt)]
                    return df_filtered
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"港股历史数据接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_hk_stock_spot_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取港股实时数据"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(ak.stock_hk_spot)
            if df is not None and not df.empty:
                for code_format in [f"{code}.HK", code, f"0{code}"]:
                    code_columns = ['代码', 'symbol', 'code', '股票代码']
                    for col in code_columns:
                        if col in df.columns:
                            stock_data = df[df[col] == code_format]
                            if not stock_data.empty:
                                return self._convert_spot_to_hist_format(stock_data.iloc[0])
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"港股实时数据接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_us_daily_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取美股日线数据"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(ak.stock_us_daily, symbol=code, adjust="qfq")
            if df is not None and not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                df_filtered = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
                return df_filtered
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"美股日线接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_etf_hist_em_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取ETF历史数据"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(
                ak.fund_etf_hist_em,
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as e:
            logger.warning(f"ETF历史数据接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_lof_hist_em_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """异步获取LOF历史数据"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(
                ak.fund_lof_hist_em,
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            return df if df is not None and not df.empty else pd.DataFrame()
        except Exception as e:
            logger.warning(f"LOF历史数据接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    # ===== 新增: ETF数据源 - efinance =====

    async def _fetch_zh_a_efinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用efinance获取A股历史数据"""
        try:
            import efinance as ef
            df = await asyncio.to_thread(
                ef.stock.get_quote_history,
                stock_codes=code,
                beg=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_efinance_columns(df)
                logger.info(f"A股 {code} 使用efinance接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"efinance A股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_hk_efinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用efinance获取港股历史数据"""
        try:
            import efinance as ef
            # efinance港股代码格式可能需要调整
            hk_code = code if code.endswith('.HK') else f"{code}.HK"
            df = await asyncio.to_thread(
                ef.stock.get_quote_history,
                stock_codes=hk_code,
                beg=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_efinance_columns(df)
                logger.info(f"港股 {code} 使用efinance接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"efinance 港股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_us_efinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用efinance获取美股历史数据"""
        try:
            import efinance as ef
            df = await asyncio.to_thread(
                ef.stock.get_quote_history,
                stock_codes=code,
                beg=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_efinance_columns(df)
                logger.info(f"美股 {code} 使用efinance接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"efinance 美股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    # ===== 新增: qstock数据源 =====

    async def _fetch_zh_a_qstock_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用qstock获取A股历史数据"""
        try:
            import qstock as qs
            # qstock使用get_data函数，参数是code_list（列表）
            df = await asyncio.to_thread(
                qs.get_data,
                code_list=[code],  # 注意：参数名是code_list，传入列表
                start=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_qstock_columns(df)
                logger.info(f"A股 {code} 使用qstock接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"qstock A股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_hk_qstock_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用qstock获取港股历史数据"""
        try:
            import qstock as qs
            # qstock港股代码格式，参数是code_list（列表）
            hk_code = code if code.endswith('.HK') else f"{code}.HK"
            df = await asyncio.to_thread(
                qs.get_data,
                code_list=[hk_code],  # 注意：参数名是code_list，传入列表
                start=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_qstock_columns(df)
                logger.info(f"港股 {code} 使用qstock接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"qstock 港股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_us_qstock_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用qstock获取美股历史数据"""
        try:
            import qstock as qs
            # qstock参数是code_list（列表）
            df = await asyncio.to_thread(
                qs.get_data,
                code_list=[code],  # 注意：参数名是code_list，传入列表
                start=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_qstock_columns(df)
                logger.info(f"美股 {code} 使用qstock接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"qstock 美股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    # ===== 新增: yfinance数据源 =====

    async def _fetch_us_yfinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用yfinance获取美股历史数据"""
        try:
            import yfinance as yf
            import logging

            # 临时禁用yfinance的错误日志
            yf_logger = logging.getLogger('yfinance')
            original_level = yf_logger.level
            yf_logger.setLevel(logging.CRITICAL)

            try:
                # yfinance需要日期格式为YYYY-MM-DD
                start_dt = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                end_dt = pd.to_datetime(end_date).strftime('%Y-%m-%d')

                ticker = yf.Ticker(code)
                df = await asyncio.to_thread(
                    ticker.history,
                    start=start_dt,
                    end=end_dt
                )

                if df is not None and not df.empty:
                    # yfinance返回的列名需要标准化
                    df = self._normalize_yfinance_columns(df)
                    logger.info(f"美股 {code} 使用yfinance接口成功")
                    return df
                return pd.DataFrame()
            finally:
                # 恢复yfinance日志级别
                yf_logger.setLevel(original_level)
        except Exception as e:
            logger.warning(f"yfinance 美股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_hk_yfinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用yfinance获取港股历史数据"""
        try:
            import yfinance as yf
            import logging

            # 临时禁用yfinance的错误日志
            yf_logger = logging.getLogger('yfinance')
            original_level = yf_logger.level
            yf_logger.setLevel(logging.CRITICAL)

            try:
                # 港股代码格式验证和转换
                # 确保代码格式正确: 5位数字.HK (如 01065.HK)
                if code.endswith('.HK'):
                    # 已经是正确格式,提取数字部分验证
                    hk_code = code
                    code_num = code.replace('.HK', '')
                else:
                    code_num = code
                    hk_code = f"{code}.HK"

                # 补齐到5位数字
                code_num = code_num.zfill(5)
                hk_code = f"{code_num}.HK"

                start_dt = pd.to_datetime(start_date).strftime('%Y-%m-%d')
                end_dt = pd.to_datetime(end_date).strftime('%Y-%m-%d')

                ticker = yf.Ticker(hk_code)
                df = await asyncio.to_thread(
                    ticker.history,
                    start=start_dt,
                    end=end_dt
                )

                if df is not None and not df.empty:
                    df = self._normalize_yfinance_columns(df)
                    logger.info(f"港股 {code} 使用yfinance接口成功")
                    return df

                # 如果数据为空,记录警告但不抛出错误
                logger.warning(f"yfinance 港股 {hk_code} 返回空数据,可能股票代码不存在或已退市")
                return pd.DataFrame()
            finally:
                # 恢复yfinance日志级别
                yf_logger.setLevel(original_level)
        except Exception as e:
            logger.warning(f"yfinance 港股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    # ===== 新增: baostock数据源（仅A股）=====

    async def _fetch_zh_a_baostock_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用baostock获取A股历史数据"""
        try:
            import baostock as bs

            # baostock需要格式化代码（如sh.600000或sz.000001）
            if code.startswith('6'):
                bs_code = f"sh.{code}"
            else:
                bs_code = f"sz.{code}"

            # baostock日期格式: YYYY-MM-DD
            start_dt = pd.to_datetime(start_date).strftime('%Y-%m-%d')
            end_dt = pd.to_datetime(end_date).strftime('%Y-%m-%d')

            # baostock需要先登录
            def fetch_baostock():
                lg = bs.login()
                if lg.error_code != '0':
                    raise Exception(f"baostock登录失败: {lg.error_msg}")

                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,code,open,high,low,close,volume,amount",
                    start_date=start_dt,
                    end_date=end_dt,
                    frequency="d",
                    adjustflag="2"  # 2表示前复权
                )

                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

                bs.logout()

                if not data_list:
                    return pd.DataFrame()

                df = pd.DataFrame(data_list, columns=rs.fields)
                return df

            df = await asyncio.to_thread(fetch_baostock)

            if df is not None and not df.empty:
                df = self._normalize_baostock_columns(df)
                logger.info(f"A股 {code} 使用baostock接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"baostock A股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_zh_a_sina_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用新浪财经获取A股历史数据"""
        try:
            import akshare as ak
            # 新浪财经代码格式：sh600000 或 sz000001
            sina_code = f"sh{code}" if code.startswith(('6', '5', '9')) else f"sz{code}"

            df = await asyncio.to_thread(
                ak.stock_zh_a_daily,
                symbol=sina_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is not None and not df.empty:
                logger.info(f"A股 {code} 使用新浪财经接口成功")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"新浪财经 A股接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_etf_efinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用efinance获取ETF历史数据"""
        try:
            import efinance as ef
            df = await asyncio.to_thread(
                ef.stock.get_quote_history,
                stock_codes=code,
                beg=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                # efinance返回的列名可能需要映射
                df = self._normalize_efinance_columns(df)
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"efinance ETF接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_lof_efinance_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用efinance获取LOF历史数据"""
        try:
            import efinance as ef
            df = await asyncio.to_thread(
                ef.stock.get_quote_history,
                stock_codes=code,
                beg=start_date,
                end=end_date
            )
            if df is not None and not df.empty:
                df = self._normalize_efinance_columns(df)
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"efinance LOF接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    # ===== 新增: ETF/LOF作为股票查询（回退方案） =====

    async def _fetch_etf_as_stock_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """将ETF作为股票查询（使用股票接口）"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(
                ak.stock_zh_a_hist,
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            if df is not None and not df.empty:
                logger.info(f"ETF {code} 使用股票接口成功获取数据")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"ETF作为股票查询失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_lof_as_stock_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """将LOF作为股票查询（使用股票接口）"""
        try:
            import akshare as ak
            df = await asyncio.to_thread(
                ak.stock_zh_a_hist,
                symbol=code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            if df is not None and not df.empty:
                logger.info(f"LOF {code} 使用股票接口成功获取数据")
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"LOF作为股票查询失败: {str(e)[:100]}")
            return pd.DataFrame()

    # ===== 新增: ETF/LOF实时行情作为回退 =====

    async def _fetch_etf_spot_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取ETF实时行情数据"""
        try:
            import akshare as ak
            df_spot = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if df_spot is not None and not df_spot.empty:
                stock_data = df_spot[df_spot['代码'] == code]
                if not stock_data.empty:
                    return self._convert_spot_to_hist_format(stock_data.iloc[0])
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"ETF实时行情接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    async def _fetch_lof_spot_async(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取LOF实时行情数据"""
        try:
            import akshare as ak
            df_spot = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if df_spot is not None and not df_spot.empty:
                stock_data = df_spot[df_spot['代码'] == code]
                if not stock_data.empty:
                    return self._convert_spot_to_hist_format(stock_data.iloc[0])
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"LOF实时行情接口失败: {str(e)[:100]}")
            return pd.DataFrame()

    def _normalize_efinance_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化efinance返回的列名"""
        try:
            # efinance可能使用的列名映射
            column_mapping = {
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '涨跌幅': '涨跌幅',
                '涨跌额': '涨跌额',
                '换手率': '换手率'
            }

            # 只重命名存在的列
            existing_mappings = {k: v for k, v in column_mapping.items() if k in df.columns}
            if existing_mappings:
                df = df.rename(columns=existing_mappings)

            return df
        except Exception as e:
            logger.warning(f"列名标准化失败: {str(e)}")
            return df

    def _normalize_qstock_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化qstock返回的列名"""
        try:
            # qstock可能使用的列名映射
            column_mapping = {
                'Date': 'date',
                'date': 'date',
                'Open': 'open',
                'open': 'open',
                'Close': 'close',
                'close': 'close',
                'High': 'high',
                'high': 'high',
                'Low': 'low',
                'low': 'low',
                'Volume': 'volume',
                'volume': 'volume',
                'Amount': 'amount',
                'amount': 'amount',
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount'
            }

            # 只重命名存在的列
            existing_mappings = {k: v for k, v in column_mapping.items() if k in df.columns}
            if existing_mappings:
                df = df.rename(columns=existing_mappings)

            return df
        except Exception as e:
            logger.warning(f"qstock列名标准化失败: {str(e)}")
            return df

    def _normalize_yfinance_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化yfinance返回的列名"""
        try:
            # yfinance返回的列名是标准的英文大写
            column_mapping = {
                'Date': 'date',
                'Open': 'open',
                'Close': 'close',
                'High': 'high',
                'Low': 'low',
                'Volume': 'volume',
                'Adj Close': 'adj_close'
            }

            # yfinance的索引是日期，需要重置为列
            if df.index.name == 'Date' or isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()

            # 只重命名存在的列
            existing_mappings = {k: v for k, v in column_mapping.items() if k in df.columns}
            if existing_mappings:
                df = df.rename(columns=existing_mappings)

            return df
        except Exception as e:
            logger.warning(f"yfinance列名标准化失败: {str(e)}")
            return df

    def _normalize_baostock_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化baostock返回的列名"""
        try:
            # baostock返回的列名都是小写字符串
            # 需要将字符串转换为数值
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])

            for col in ['open', 'close', 'high', 'low', 'volume', 'amount']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            return df
        except Exception as e:
            logger.warning(f"baostock列名标准化失败: {str(e)}")
            return df


    def _convert_spot_to_hist_format(self, spot_data: pd.Series) -> pd.DataFrame:
        """将实时数据转换为历史数据格式"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')

            current_price = self._get_price_value(spot_data, ['最新价', 'current', 'close', '收盘价'])
            open_price = self._get_price_value(spot_data, ['今开', 'open', '开盘价'], current_price)
            high_price = self._get_price_value(spot_data, ['最高', 'high', '最高价'], current_price)
            low_price = self._get_price_value(spot_data, ['最低', 'low', '最低价'], current_price)

            volume = self._get_numeric_value(spot_data, ['成交量', 'volume', 'vol'], 0)
            amount = self._get_numeric_value(spot_data, ['成交额', 'amount', '成交金额'], 0)

            hist_format = pd.DataFrame([{
                'date': today,
                'open': open_price,
                'close': current_price,
                'high': high_price,
                'low': low_price,
                'volume': volume,
                'amount': amount,
                '涨跌幅': self._get_numeric_value(spot_data, ['涨跌幅', 'change_percent'], 0),
                '涨跌额': self._get_numeric_value(spot_data, ['涨跌额', 'change_amount'], 0),
                '换手率': self._get_numeric_value(spot_data, ['换手率', 'turnover_rate'], 0)
            }])
            return hist_format
        except Exception as e:
            logger.error(f"实时数据转换失败: {str(e)}")
            return pd.DataFrame()

    def _get_price_value(self, data: pd.Series, field_names: List[str], default: float = 0.0) -> float:
        """获取价格数值"""
        for field in field_names:
            if field in data.index and pd.notna(data[field]):
                try:
                    return float(data[field])
                except (ValueError, TypeError):
                    continue
        return default

    def _get_numeric_value(self, data: pd.Series, field_names: List[str], default: float = 0.0) -> float:
        """获取数值"""
        for field in field_names:
            if field in data.index and pd.notna(data[field]):
                try:
                    value = str(data[field]).replace(',', '')
                    return float(value)
                except (ValueError, TypeError):
                    continue
        return default

    async def _fetch_with_source_async(self,
                                      source: DataSourceConfig,
                                      stock_code: str,
                                      start_date: str,
                                      end_date: str) -> FetchResult:
        """异步获取单个数据源的数据"""
        start_time = time.time()

        try:
            # 检查熔断器
            if await self._check_circuit_breaker_async(source.name):
                return FetchResult(
                    success=False,
                    data=None,
                    source_name=source.name,
                    fetch_time=time.time() - start_time,
                    error_message="数据源已熔断"
                )

            # 控制请求间隔
            await self._wait_for_interval_async(source.name, source.min_interval)

            # 执行数据获取
            df = await source.func(stock_code, start_date, end_date)

            fetch_time = time.time() - start_time

            if df is not None and not df.empty and len(df) >= 1:
                # 记录成功
                await self._record_success_async(source.name)

                logger.info("数据源获取成功",
                           source=source.name,
                           records=len(df),
                           fetch_time=f"{fetch_time:.3f}s")

                return FetchResult(
                    success=True,
                    data=df,
                    source_name=source.name,
                    fetch_time=fetch_time
                )
            else:
                # 记录失败
                await self._record_failure_async(source.name)

                return FetchResult(
                    success=False,
                    data=None,
                    source_name=source.name,
                    fetch_time=fetch_time,
                    error_message="返回空数据"
                )

        except asyncio.TimeoutError:
            await self._record_failure_async(source.name)
            return FetchResult(
                success=False,
                data=None,
                source_name=source.name,
                fetch_time=time.time() - start_time,
                error_message="请求超时"
            )
        except Exception as e:
            await self._record_failure_async(source.name)
            return FetchResult(
                success=False,
                data=None,
                source_name=source.name,
                fetch_time=time.time() - start_time,
                error_message=str(e)
            )

    async def fetch_stock_data_concurrent(self,
                                         stock_code: str,
                                         market_type: MarketType,
                                         start_date: str = None,
                                         end_date: str = None) -> FetchResult:
        """
        并发获取股票数据 - 采纳最快响应的数据源

        Args:
            stock_code: 股票代码
            market_type: 市场类型
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            FetchResult: 获取结果
        """
        # 默认日期范围
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

        logger.info("开始并发数据获取",
                   stock_code=stock_code,
                   market=market_type.value,
                   start_date=start_date,
                   end_date=end_date)

        # 1. 检查缓存
        cached_data = await self._check_cache_async(stock_code, market_type, start_date, end_date)
        if cached_data is not None and not cached_data.empty:
            return FetchResult(
                success=True,
                data=cached_data,
                source_name="cache",
                fetch_time=0.0
            )

        # 2. 获取数据源配置
        sources = self.data_sources.get(market_type, [])
        if not sources:
            return FetchResult(
                success=False,
                data=None,
                source_name="none",
                fetch_time=0.0,
                error_message=f"不支持的市场类型: {market_type.value}"
            )

        # 3. 并发执行前N个数据源
        concurrent_sources = sources[:self.max_concurrent_sources]
        tasks = [
            self._fetch_with_source_async(source, stock_code, start_date, end_date)
            for source in concurrent_sources
        ]

        # 4. 等待第一个成功的结果
        pending_tasks = set(asyncio.create_task(task) for task in tasks)

        try:
            while pending_tasks:
                done, pending_tasks = await asyncio.wait(
                    pending_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=self.request_timeout
                )

                # 检查已完成的任务
                for task in done:
                    result = await task
                    if result.success:
                        # 找到成功的结果，取消其他任务
                        for pending_task in pending_tasks:
                            pending_task.cancel()

                        logger.info("采纳最快数据源",
                                   source=result.source_name,
                                   fetch_time=f"{result.fetch_time:.3f}s",
                                   cancelled_tasks=len(pending_tasks))

                        # 设置缓存
                        await self._set_cache_async(stock_code, market_type, start_date, end_date, result.data)

                        return result

            # 所有任务都失败了
            logger.error("所有数据源都失败",
                        stock_code=stock_code,
                        market=market_type.value,
                        sources_tried=len(concurrent_sources))

            return FetchResult(
                success=False,
                data=None,
                source_name="all_failed",
                fetch_time=0.0,
                error_message="所有数据源都失败"
            )

        except asyncio.TimeoutError:
            # 超时,取消所有任务
            for task in pending_tasks:
                task.cancel()

            return FetchResult(
                success=False,
                data=None,
                source_name="timeout",
                fetch_time=self.request_timeout,
                error_message="所有请求超时"
            )

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self._get_session()

        # 初始化缓存管理器
        if self.enable_cache:
            try:
                from src.utils.cache_manager import cache_manager
                self._cache_manager = cache_manager
                await self._cache_manager.connect()
            except Exception as e:
                logger.warning("缓存管理器初始化失败", error=str(e))
                self._cache_manager = None

        # 初始化熔断器
        if self.enable_circuit_breaker:
            try:
                from src.utils.circuit_breaker import circuit_breaker_manager
                self._circuit_breaker_manager = circuit_breaker_manager
                await self._circuit_breaker_manager.initialize()
            except Exception as e:
                logger.warning("熔断器初始化失败", error=str(e))
                self._circuit_breaker_manager = None

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        await self.close()


# 全局实例
async_fetcher = AsyncStockDataFetcher()


# 使用示例:
# async with AsyncStockDataFetcher() as fetcher:
#     result = await fetcher.fetch_stock_data_concurrent(
#         stock_code="600271",
#         market_type=MarketType.A_SHARE
#     )
#     if result.success:
#         print(f"数据获取成功，来源: {result.source_name}")
#         print(result.data.head())
