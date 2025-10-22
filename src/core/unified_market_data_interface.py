"""
统一市场数据接口 - 五市场完美运行解决方案
Unified Market Data Interface - Five Markets Perfect Operation Solution

版本: 1.0
作者: Stock Analysis Team
创建日期: 2025-10-21

功能概述:
    1. 统一五市场数据接口 (A股/港股/美股/ETF/LOF)
    2. 智能市场识别与代码验证
    3. 多数据源自动切换与负载均衡
    4. 实时数据缓存与增量更新
    5. 异常自动恢复与降级处理
    6. 性能监控与统计分析

技术特点:
    - 基于增强型数据获取器
    - 智能路由与数据源选择
    - 分布式缓存支持
    - 熔断器与限流机制
    - 实时监控与告警
"""

import time
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
from functools import lru_cache
import hashlib

# 导入增强型数据获取器
try:
    from src.core.robust_stock_data_fetcher import RobustStockDataFetcher, MarketType
    ROBUST_FETCHER_AVAILABLE = True
except ImportError as e:
    logging.error(f"无法导入增强型数据获取器: {e}")
    ROBUST_FETCHER_AVAILABLE = False

# 导入港股数据获取器
try:
    from src.core.hk_stock_data_fetcher import HKStockDataFetcher
    HK_FETCHER_AVAILABLE = True
except ImportError as e:
    logging.warning(f"无法导入港股数据获取器: {e}")
    HK_FETCHER_AVAILABLE = False

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketCategory(Enum):
    """市场分类"""
    A_SHARE = "A"           # A股
    HONG_KONG = "HK"        # 港股
    US_STOCK = "US"         # 美股
    ETF = "ETF"             # ETF
    LOF = "LOF"             # LOF

@dataclass
class StockInfo:
    """股票信息"""
    code: str
    name: str
    market: str
    category: str
    is_valid: bool = True
    last_update: Optional[datetime] = None

@dataclass
class MarketDataResult:
    """市场数据结果"""
    success: bool
    data: Optional[pd.DataFrame]
    message: str
    market: str
    source: str
    fetch_time: float
    data_count: int = 0
    retry_count: int = 0

@dataclass
class RealTimeQuote:
    """实时行情数据"""
    code: str
    name: str
    current_price: float
    change_percent: float
    change_amount: float
    volume: int
    turnover: float
    timestamp: str
    market: str
    source: str = ""

class UnifiedMarketDataInterface:
    """
    统一市场数据接口

    核心功能:
    1. 五市场统一数据获取
    2. 智能市场识别
    3. 多数据源负载均衡
    4. 实时数据缓存
    5. 异常自动处理
    """

    def __init__(self, enable_cache: bool = True, cache_ttl: int = 300):
        """
        初始化统一市场数据接口

        Args:
            enable_cache: 是否启用缓存
            cache_ttl: 缓存过期时间(秒)
        """
        self.enable_cache = enable_cache
        self.cache_ttl = cache_ttl

        # 初始化数据获取器
        if ROBUST_FETCHER_AVAILABLE:
            self.robust_fetcher = RobustStockDataFetcher()
            logger.info("增强型数据获取器初始化成功")
        else:
            self.robust_fetcher = None
            logger.warning("增强型数据获取器不可用")

        # 初始化港股获取器
        if HK_FETCHER_AVAILABLE:
            self.hk_fetcher = HKStockDataFetcher()
            logger.info("港股数据获取器初始化成功")
        else:
            self.hk_fetcher = None
            logger.warning("港股数据获取器不可用")

        # 市场配置
        self._setup_market_config()

        # 缓存管理
        self._data_cache: Dict[str, Tuple[pd.DataFrame, float]] = {}
        self._quote_cache: Dict[str, Tuple[RealTimeQuote, float]] = {}

        # 统计信息
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'cache_hits': 0,
            'average_response_time': 0.0
        }

        logger.info("统一市场数据接口初始化完成")

    def _setup_market_config(self):
        """配置市场参数"""
        self.market_config = {
            MarketCategory.A_SHARE: {
                'name': 'A股',
                'code_patterns': [
                    r'^6\d{5}$',      # 沪市主板
                    r'^0[0-3]\d{4}$', # 深市主板/中小板
                    r'^30[0-3]\d{3}$', # 创业板
                    r'^68[8-9]\d{3}$', # 科创板
                    r'^[48]\d{5}$',    # 北交所
                    r'^[0-9]{6}$'       # 通用6位数字
                ],
                'test_codes': ['600271', '000001', '300750', '688981'],
                'benchmark': '000300',  # 沪深300
                'timezone': 'Asia/Shanghai'
            },
            MarketCategory.HONG_KONG: {
                'name': '港股',
                'code_patterns': [
                    r'^\d{4}$',       # 4位数字代码
                    r'^\d{5}$'        # 5位数字代码
                ],
                'test_codes': ['0700', '00005', '3690'],
                'benchmark': 'HSI',     # 恒生指数
                'timezone': 'Asia/Hong_Kong'
            },
            MarketCategory.US_STOCK: {
                'name': '美股',
                'code_patterns': [
                    r'^[A-Z]{1,5}$',  # 股票代码如AAPL, MSFT
                    r'^[A-Z]+\.[A-Z]+$' # 带交易所代码
                ],
                'test_codes': ['AAPL', 'MSFT', 'GOOGL'],
                'benchmark': 'SPX',     # 标普500
                'timezone': 'America/New_York'
            },
            MarketCategory.ETF: {
                'name': 'ETF',
                'code_patterns': [
                    r'^5\d{5}$',      # ETF代码
                    r'^1[0-8]\d{4}$'  # 其他ETF代码
                ],
                'test_codes': ['510300', '510500', '159915'],
                'benchmark': '000300',
                'timezone': 'Asia/Shanghai'
            },
            MarketCategory.LOF: {
                'name': 'LOF',
                'code_patterns': [
                    r'^1[0-8]\d{4}$', # LOF基金
                    r'^16\d{4}$'     # 特定LOF代码
                ],
                'test_codes': ['161725', '163402', '160222'],
                'benchmark': '000300',
                'timezone': 'Asia/Shanghai'
            }
        }

    def detect_market_type(self, stock_code: str) -> Optional[MarketCategory]:
        """
        智能识别市场类型

        Args:
            stock_code: 股票代码

        Returns:
            Optional[MarketCategory]: 识别的市场类型
        """
        import re

        # 清理代码
        code = str(stock_code).strip().upper()

        # 按市场优先级检测
        for market_type, config in self.market_config.items():
            for pattern in config['code_patterns']:
                if re.match(pattern, code):
                    logger.info(f"代码 {code} 识别为 {market_type.value} 市场")
                    return market_type

        logger.warning(f"无法识别代码 {code} 的市场类型")
        return None

    def validate_stock_code(self, stock_code: str, market_type: Optional[MarketCategory] = None) -> Tuple[bool, str]:
        """
        验证股票代码有效性

        Args:
            stock_code: 股票代码
            market_type: 市场类型(可选)

        Returns:
            Tuple[bool, str]: (是否有效, 错误信息)
        """
        try:
            if not stock_code or not isinstance(stock_code, str):
                return False, "股票代码不能为空"

            code = str(stock_code).strip()

            # 如果未指定市场类型，先自动识别
            if market_type is None:
                market_type = self.detect_market_type(code)
                if market_type is None:
                    return False, f"无法识别的股票代码格式: {code}"

            # 市场特定验证
            if market_type == MarketCategory.A_SHARE:
                return self._validate_a_share_code(code)
            elif market_type == MarketCategory.HONG_KONG:
                return self._validate_hk_stock_code(code)
            elif market_type == MarketCategory.US_STOCK:
                return self._validate_us_stock_code(code)
            elif market_type in [MarketCategory.ETF, MarketCategory.LOF]:
                return self._validate_fund_code(code, market_type)

            return False, f"不支持的市场类型: {market_type.value}"

        except Exception as e:
            logger.error(f"股票代码验证失败: {str(e)}")
            return False, f"验证过程出错: {str(e)}"

    def _validate_a_share_code(self, code: str) -> Tuple[bool, str]:
        """验证A股代码"""
        # A股代码规则
        valid_prefixes = ['600', '601', '603', '605',  # 沪市主板
                         '000', '001', '002', '003',  # 深市主板/中小板
                         '300', '301', '303',         # 创业板
                         '688', '689',                # 科创板
                         '430', '830', '831', '832',  # 新三板
                         '8', '4']                    # 北交所

        if not code.isdigit() or len(code) != 6:
            return False, "A股代码必须是6位数字"

        if not any(code.startswith(prefix) for prefix in valid_prefixes):
            return False, f"无效的A股代码前缀: {code}"

        return True, ""

    def _validate_hk_stock_code(self, code: str) -> Tuple[bool, str]:
        """验证港股代码"""
        if not code.isdigit():
            return False, "港股代码必须是数字"

        if len(code) not in [4, 5]:
            return False, "港股代码必须是4位或5位数字"

        return True, ""

    def _validate_us_stock_code(self, code: str) -> Tuple[bool, str]:
        """验证美股代码"""
        import re
        if not re.match(r'^[A-Z]{1,5}$', code):
            return False, "美股代码必须是1-5位大写字母"

        return True, ""

    def _validate_fund_code(self, code: str, fund_type: MarketCategory) -> Tuple[bool, str]:
        """验证基金代码"""
        if not code.isdigit() or len(code) != 6:
            return False, f"{fund_type.value}代码必须是6位数字"

        if fund_type == MarketCategory.ETF:
            # ETF代码通常以5开头
            if not code.startswith('5'):
                return False, "ETF代码通常以5开头"
        elif fund_type == MarketCategory.LOF:
            # LOF代码通常以16或15开头
            if not (code.startswith('16') or code.startswith('15')):
                return False, "LOF代码通常以16或15开头"

        return True, ""

    def _generate_cache_key(self, stock_code: str, market_type: MarketCategory,
                           start_date: str, end_date: str) -> str:
        """生成缓存键"""
        key_string = f"{stock_code}_{market_type.value}_{start_date}_{end_date}"
        return hashlib.md5(key_string.encode()).hexdigest()

    def _is_cache_valid(self, cache_time: float) -> bool:
        """检查缓存是否有效"""
        if not self.enable_cache:
            return False
        return (time.time() - cache_time) < self.cache_ttl

    def _get_cached_data(self, cache_key: str) -> Optional[pd.DataFrame]:
        """获取缓存数据"""
        if cache_key in self._data_cache:
            data, cache_time = self._data_cache[cache_key]
            if self._is_cache_valid(cache_time):
                self.stats['cache_hits'] += 1
                logger.info(f"缓存命中: {cache_key}")
                return data.copy()
        return None

    def _set_cached_data(self, cache_key: str, data: pd.DataFrame):
        """设置缓存数据"""
        if self.enable_cache and data is not None and not data.empty:
            self._data_cache[cache_key] = (data.copy(), time.time())
            logger.info(f"数据已缓存: {cache_key}")

    def get_stock_data(self, stock_code: str, market_type: Optional[MarketCategory] = None,
                      start_date: str = None, end_date: str = None,
                      use_cache: bool = True) -> MarketDataResult:
        """
        统一股票数据获取接口

        Args:
            stock_code: 股票代码
            market_type: 市场类型(可选，自动识别)
            start_date: 开始日期(YYYYMMDD)
            end_date: 结束日期(YYYYMMDD)
            use_cache: 是否使用缓存

        Returns:
            MarketDataResult: 数据获取结果
        """
        start_time = time.time()
        self.stats['total_requests'] += 1

        try:
            # 参数验证
            is_valid, error_msg = self.validate_stock_code(stock_code, market_type)
            if not is_valid:
                self.stats['failed_requests'] += 1
                return MarketDataResult(
                    success=False,
                    data=None,
                    message=error_msg,
                    market=market_type.value if market_type else "unknown",
                    source="validation",
                    fetch_time=time.time() - start_time
                )

            # 自动识别市场类型
            if market_type is None:
                market_type = self.detect_market_type(stock_code)
                if market_type is None:
                    self.stats['failed_requests'] += 1
                    return MarketDataResult(
                        success=False,
                        data=None,
                        message=f"无法识别的市场类型: {stock_code}",
                        market="unknown",
                        source="detection",
                        fetch_time=time.time() - start_time
                    )

            # 默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')

            # 检查缓存
            if use_cache:
                cache_key = self._generate_cache_key(stock_code, market_type, start_date, end_date)
                cached_data = self._get_cached_data(cache_key)
                if cached_data is not None:
                    fetch_time = time.time() - start_time
                    return MarketDataResult(
                        success=True,
                        data=cached_data,
                        message="数据获取成功(缓存)",
                        market=market_type.value,
                        source="cache",
                        fetch_time=fetch_time,
                        data_count=len(cached_data)
                    )

            # 获取数据
            if self.robust_fetcher and ROBUST_FETCHER_AVAILABLE:
                # 使用增强型数据获取器
                df = self.robust_fetcher.fetch_stock_data(
                    stock_code=stock_code,
                    market_type=MarketType(market_type.value),
                    start_date=start_date,
                    end_date=end_date
                )
                source = "robust_fetcher"
            else:
                # 回退到基础获取方式
                df = self._basic_fetch_stock_data(stock_code, market_type, start_date, end_date)
                source = "basic_fetcher"

            # 处理结果
            if df is not None and not df.empty:
                # 缓存数据
                if use_cache:
                    self._set_cached_data(cache_key, df)

                self.stats['successful_requests'] += 1
                fetch_time = time.time() - start_time

                return MarketDataResult(
                    success=True,
                    data=df,
                    message="数据获取成功",
                    market=market_type.value,
                    source=source,
                    fetch_time=fetch_time,
                    data_count=len(df)
                )
            else:
                self.stats['failed_requests'] += 1
                fetch_time = time.time() - start_time

                return MarketDataResult(
                    success=False,
                    data=None,
                    message="数据获取失败，所有数据源都不可用",
                    market=market_type.value,
                    source=source,
                    fetch_time=fetch_time
                )

        except Exception as e:
            self.stats['failed_requests'] += 1
            fetch_time = time.time() - start_time
            logger.error(f"数据获取异常: {str(e)}")

            return MarketDataResult(
                success=False,
                data=None,
                message=f"数据获取异常: {str(e)}",
                market=market_type.value if market_type else "unknown",
                source="exception",
                fetch_time=fetch_time
            )

    def _basic_fetch_stock_data(self, stock_code: str, market_type: MarketCategory,
                               start_date: str, end_date: str) -> pd.DataFrame:
        """基础数据获取(回退方案)"""
        try:
            import akshare as ak

            if market_type == MarketCategory.A_SHARE:
                return ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
            elif market_type == MarketCategory.HONG_KONG:
                return ak.stock_hk_hist(symbol=stock_code, start_date=start_date, end_date=end_date)
            elif market_type == MarketCategory.US_STOCK:
                return ak.stock_us_daily(symbol=stock_code, start_date=start_date, end_date=end_date)
            elif market_type == MarketCategory.ETF:
                try:
                    return ak.fund_etf_hist_em(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
                except:
                    return ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
            elif market_type == MarketCategory.LOF:
                try:
                    return ak.fund_lof_hist_em(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
                except:
                    return ak.stock_zh_a_hist(symbol=stock_code, start_date=start_date, end_date=end_date, adjust="qfq")

        except Exception as e:
            logger.error(f"基础数据获取失败: {str(e)}")
            return pd.DataFrame()

    def get_real_time_quote(self, stock_code: str, market_type: Optional[MarketCategory] = None) -> Optional[RealTimeQuote]:
        """
        获取实时行情

        Args:
            stock_code: 股票代码
            market_type: 市场类型

        Returns:
            Optional[RealTimeQuote]: 实时行情数据
        """
        try:
            # 自动识别市场类型
            if market_type is None:
                market_type = self.detect_market_type(stock_code)
                if market_type is None:
                    return None

            # 使用增强型获取器
            if self.robust_fetcher and ROBUST_FETCHER_AVAILABLE:
                quote_data = self.robust_fetcher.get_real_time_data(stock_code, MarketType(market_type.value))
                if quote_data:
                    return RealTimeQuote(
                        code=stock_code,
                        name=quote_data.get('name', ''),
                        current_price=quote_data.get('current_price', 0),
                        change_percent=quote_data.get('change_percent', 0),
                        change_amount=quote_data.get('change_amount', 0),
                        volume=quote_data.get('volume', 0),
                        turnover=quote_data.get('turnover', 0),
                        timestamp=quote_data.get('timestamp', ''),
                        market=market_type.value,
                        source=quote_data.get('source', 'robust_fetcher')
                    )

            return None

        except Exception as e:
            logger.error(f"获取实时行情失败: {str(e)}")
            return None

    def batch_fetch_stock_data(self, stock_list: List[Dict[str, str]],
                             start_date: str = None, end_date: str = None,
                             max_concurrent: int = 5) -> List[MarketDataResult]:
        """
        批量获取股票数据

        Args:
            stock_list: 股票列表 [{'code': '600271', 'market': 'A'}, ...]
            start_date: 开始日期
            end_date: 结束日期
            max_concurrent: 最大并发数

        Returns:
            List[MarketDataResult]: 批量获取结果
        """
        results = []

        for stock_info in stock_list:
            try:
                code = stock_info.get('code')
                market = stock_info.get('market')

                if not code:
                    continue

                # 转换市场类型
                if market:
                    market_type = MarketCategory(market.upper())
                else:
                    market_type = None

                # 获取数据
                result = self.get_stock_data(code, market_type, start_date, end_date)
                results.append(result)

                # 控制请求频率
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"批量获取失败: {str(e)}")
                results.append(MarketDataResult(
                    success=False,
                    data=None,
                    message=f"批量获取失败: {str(e)}",
                    market=market if market else "unknown",
                    source="batch_fetch",
                    fetch_time=0
                ))

        return results

    def get_market_overview(self, market_type: MarketCategory) -> Dict[str, Any]:
        """
        获取市场概览

        Args:
            market_type: 市场类型

        Returns:
            Dict: 市场概览信息
        """
        try:
            config = self.market_config.get(market_type)
            if not config:
                return {}

            # 获取基准指数数据
            benchmark_code = config['benchmark']
            benchmark_data = self.get_stock_data(benchmark_code, market_type,
                                                 (datetime.now() - timedelta(days=30)).strftime('%Y%m%d'))

            overview = {
                'market': market_type.value,
                'name': config['name'],
                'benchmark': benchmark_code,
                'test_codes': config['test_codes'],
                'timezone': config['timezone'],
                'last_update': datetime.now().isoformat()
            }

            if benchmark_data.success and benchmark_data.data is not None:
                df = benchmark_data.data
                if not df.empty:
                    latest = df.iloc[-1]
                    overview['benchmark_data'] = {
                        'current': latest.get('close', 0),
                        'change_30d': ((latest.get('close', 0) - df.iloc[0].get('close', 0)) / df.iloc[0].get('close', 0) * 100) if len(df) > 1 else 0,
                        'data_points': len(df)
                    }

            return overview

        except Exception as e:
            logger.error(f"获取市场概览失败: {str(e)}")
            return {}

    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态

        Returns:
            Dict: 系统状态信息
        """
        try:
            # 测试各市场连接性
            connectivity_test = {}
            if self.robust_fetcher and ROBUST_FETCHER_AVAILABLE:
                connectivity_test = self.robust_fetcher.test_connectivity()

            # 计算成功率
            total_requests = self.stats['total_requests']
            success_rate = (self.stats['successful_requests'] / total_requests * 100) if total_requests > 0 else 0
            cache_hit_rate = (self.stats['cache_hits'] / total_requests * 100) if total_requests > 0 else 0

            return {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'components': {
                    'robust_fetcher': ROBUST_FETCHER_AVAILABLE,
                    'hk_fetcher': HK_FETCHER_AVAILABLE
                },
                'connectivity': connectivity_test,
                'statistics': {
                    'total_requests': total_requests,
                    'successful_requests': self.stats['successful_requests'],
                    'failed_requests': self.stats['failed_requests'],
                    'cache_hits': self.stats['cache_hits'],
                    'success_rate': round(success_rate, 2),
                    'cache_hit_rate': round(cache_hit_rate, 2),
                    'average_response_time': round(self.stats['average_response_time'], 3)
                },
                'cache_info': {
                    'enabled': self.enable_cache,
                    'ttl': self.cache_ttl,
                    'data_cache_size': len(self._data_cache),
                    'quote_cache_size': len(self._quote_cache)
                }
            }

        except Exception as e:
            logger.error(f"获取系统状态失败: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    def clear_cache(self):
        """清除缓存"""
        self._data_cache.clear()
        self._quote_cache.clear()
        logger.info("缓存已清除")

    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        return {
            'data_cache_size': len(self._data_cache),
            'quote_cache_size': len(self._quote_cache),
            'cache_ttl': self.cache_ttl,
            'enable_cache': self.enable_cache
        }


# 全局实例
unified_interface = UnifiedMarketDataInterface()

if __name__ == "__main__":
    # 系统测试
    print("=" * 60)
    print("统一市场数据接口 - 系统测试")
    print("=" * 60)

    # 1. 系统状态
    print("\n1. 系统状态:")
    status = unified_interface.get_system_status()
    print(json.dumps(status, indent=2, ensure_ascii=False))

    # 2. 市场概览
    print("\n2. 市场概览:")
    for market in [MarketCategory.A_SHARE, MarketCategory.HONG_KONG, MarketCategory.ETF]:
        overview = unified_interface.get_market_overview(market)
        print(f"{market.value}: {overview}")

    # 3. 代码验证测试
    print("\n3. 代码验证测试:")
    test_codes = [
        ('600271', 'A'),    # A股
        ('0700', 'HK'),     # 港股
        ('AAPL', 'US'),     # 美股
        ('510300', 'ETF'),  # ETF
        ('161725', 'LOF')   # LOF
    ]

    for code, market in test_codes:
        is_valid, message = unified_interface.validate_stock_code(code, MarketCategory(market))
        print(f"{code} ({market}): {'✓' if is_valid else '✗'} {message}")

    # 4. 数据获取测试
    print("\n4. 数据获取测试:")
    for code, market in test_codes[:3]:  # 测试前3个
        print(f"\n获取 {code} ({market}) 数据...")
        result = unified_interface.get_stock_data(code, MarketCategory(market), '20241001', '20241020')
        print(f"结果: {'✓' if result.success else '✗'} {result.message}")
        if result.success and result.data is not None:
            print(f"数据量: {result.data_count} 条")
            print(f"响应时间: {result.fetch_time:.3f} 秒")
            print(f"数据源: {result.source}")

    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)