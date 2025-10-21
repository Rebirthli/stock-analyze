"""
股票分析API服务 - 国际级技术指标分析系统
Stock Analysis API Service - International-Grade Technical Analysis System

版本: 2.0
作者: Stock Analysis Team
创建日期: 2025-10-20
权威性等级: 85/100 (机构级)

功能概述:
    1. 支持多市场股票分析(A股/港股/美股/ETF/LOF)
    2. 提供12+项国际级技术指标
    3. 计算风险调整绩效指标(Sharpe/Sortino/Alpha/Beta等)
    4. 智能评分系统(0-100分)
    5. RESTful API接口

主要指标:
    技术指标: MA, MACD, RSI, Bollinger, ATR, ADX, Stochastic, Williams %R, OBV, MFI
    风险指标: Sharpe Ratio, Sortino Ratio, Max Drawdown, Calmar Ratio
    因子指标: Alpha, Beta, Momentum Factor
    微观结构: VWAP, Volume Profile

使用方式:
    POST /analyze-stock/
    Header: Authorization: Bearer <token>
    Body: {"stock_code": "600271", "market_type": "A"}
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import akshare as ak
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 导入增强版港股数据获取器
try:
    from src.core.hk_stock_data_fetcher import HKStockDataFetcher
    HK_FETCHER_AVAILABLE = True
except ImportError as e:
    print(f"[WARNING] 无法导入增强版港股数据获取器: {e}")
    HK_FETCHER_AVAILABLE = False

# 导入网络重试管理器
try:
    from src.utils.network_retry_manager import network_retry_manager, data_source_manager, with_network_retry
    NETWORK_RETRY_AVAILABLE = True
except ImportError as e:
    print(f"[WARNING] 无法导入网络重试管理器: {e}")
    NETWORK_RETRY_AVAILABLE = False

# ============================================================================
# FastAPI应用实例
# ============================================================================
app = FastAPI(
    title="股票分析API - Stock Analysis API",
    description="国际级技术指标分析系统 / International-Grade Technical Analysis System",
    version="2.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# ============================================================================
# 全局参数配置
# Global Parameters Configuration
# ============================================================================
params = {
    # 移动平均线周期 (Moving Average Periods)
    'ma_periods': {
        'short': 5,      # 短期均线 (Short-term MA)
        'medium': 20,    # 中期均线 (Medium-term MA)
        'long': 60       # 长期均线 (Long-term MA)
    },

    # 技术指标周期 (Technical Indicators Periods)
    'rsi_period': 14,              # RSI相对强弱指标周期 (RSI Period)
    'bollinger_period': 20,         # 布林带周期 (Bollinger Bands Period)
    'bollinger_std': 2,             # 布林带标准差倍数 (Bollinger Std Dev Multiplier)
    'volume_ma_period': 20,         # 成交量均线周期 (Volume MA Period)
    'atr_period': 14,               # ATR平均真实波幅周期 (ATR Period)
    'adx_period': 14,               # ADX平均趋向指数周期 (ADX Period)
    'stochastic_period': 14,        # 随机振荡器周期 (Stochastic Period)
    'williams_period': 14,          # 威廉指标周期 (Williams %R Period)
    'momentum_period': 252          # 动量因子周期/年化交易日 (Momentum Factor Period/Annual Trading Days)
}

# ============================================================================
# 全局港股数据获取器实例
# Global HK Stock Data Fetcher Instance
# ============================================================================
if HK_FETCHER_AVAILABLE:
    try:
        hk_stock_fetcher = HKStockDataFetcher()
        print("[INFO] 增强版港股数据获取器初始化成功")
    except Exception as e:
        print(f"[ERROR] 港股数据获取器初始化失败: {e}")
        hk_stock_fetcher = None
else:
    hk_stock_fetcher = None
    print("[WARNING] 使用基础港股数据获取模式")

# ============================================================================
# 认证与授权
# Authentication & Authorization
# ============================================================================

def verify_auth_token(authorization: str = Header(None)):
    """
    验证Authorization Header中的Bearer Token
    Verify Bearer Token in Authorization Header

    参数 (Parameters):
        authorization (str): Authorization请求头，格式: "Bearer <token>"
                            Authorization header, format: "Bearer <token>"

    返回 (Returns):
        str: 验证通过的token字符串
             Verified token string

    异常 (Raises):
        HTTPException 401: Authorization Header缺失
        HTTPException 401: 认证方案无效(非Bearer)
        HTTPException 403: Token无效或已过期

    示例 (Example):
        >>> headers = {"Authorization": "Bearer sk-xykj-001"}
        >>> token = verify_auth_token(headers["Authorization"])
    """
    # 检查Authorization Header是否存在
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization Header"
        )

    # 解析认证方案和token
    scheme, _, token = authorization.partition(" ")

    # 验证认证方案是否为Bearer
    if scheme.lower() != "bearer":
        raise HTTPException(
            status_code=401,
            detail="Invalid Authorization Scheme"
        )

    # 验证token是否在有效列表中
    # TODO: 生产环境应从数据库或配置中心加载
    valid_tokens = ["sk-xykj-tykj-001"]
    if token not in valid_tokens:
        raise HTTPException(
            status_code=403,
            detail="Invalid or Expired Token"
        )

    return token


# ============================================================================
# 数据模型定义
# Data Models Definition
# ============================================================================

class StockAnalysisRequest(BaseModel):
    """
    股票分析请求模型
    Stock Analysis Request Model

    属性 (Attributes):
        stock_code (str): 股票代码
                         Stock code
                         示例: "600271" (A股), "0700" (港股), "AAPL" (美股)

        market_type (str): 市场类型，默认'A'
                          Market type, default 'A'
                          可选值: 'A' (A股), 'HK' (港股), 'US' (美股),
                                 'ETF' (ETF基金), 'LOF' (LOF基金)

        start_date (str, optional): 开始日期，格式YYYYMMDD
                                    Start date, format YYYYMMDD
                                    默认: 当前日期-365天

        end_date (str, optional): 结束日期，格式YYYYMMDD
                                 End date, format YYYYMMDD
                                 默认: 当前日期

        benchmark_code (str, optional): 基准指数代码(用于Alpha/Beta计算)
                                       Benchmark index code (for Alpha/Beta calculation)
                                       默认: 根据market_type自动选择

    示例 (Example):
        >>> request = StockAnalysisRequest(
        ...     stock_code="600271",
        ...     market_type="A",
        ...     start_date="20240101",
        ...     end_date="20251020"
        ... )
    """
    stock_code: str
    market_type: str = 'A'
    start_date: str = None
    end_date: str = None
    benchmark_code: str = None


# ============================================================================
# 数据获取模块
# Data Acquisition Module
# ============================================================================

def get_stock_data(stock_code, market_type='A', start_date=None, end_date=None):
    """
    获取股票或基金历史数据
    Get historical stock or fund data

    功能说明:
        - 支持多市场数据获取(A股/港股/美股/ETF/LOF)
        - 自动进行前复权调整
        - 数据格式标准化处理
        - 日期范围默认为最近1年

    参数 (Parameters):
        stock_code (str): 股票代码
                         Stock code
                         A股示例: "600271" (南京化纤)
                         港股示例: "0700" (腾讯)
                         美股示例: "AAPL" (苹果)

        market_type (str): 市场类型，默认'A'
                          Market type, default 'A'
                          - 'A': A股市场
                          - 'HK': 香港股市
                          - 'US': 美国股市
                          - 'ETF': ETF基金
                          - 'LOF': LOF基金

        start_date (str, optional): 开始日期，格式YYYYMMDD
                                    Start date, format YYYYMMDD
                                    默认: 当前日期-365天

        end_date (str, optional): 结束日期，格式YYYYMMDD
                                 End date, format YYYYMMDD
                                 默认: 当前日期

    返回 (Returns):
        pd.DataFrame: 标准化的股票数据
                     Standardized stock data
                     列: date, open, close, high, low, volume

    异常 (Raises):
        ValueError: 股票代码格式无效
        Exception: 数据获取失败

    数据来源:
        使用akshare库从公开数据源获取
        - A股: 东方财富/新浪财经
        - 港股: 东方财富
        - 美股: 新浪财经
        - ETF/LOF: 东方财富

    示例 (Example):
        >>> df = get_stock_data("600271", "A", "20240101", "20251020")
        >>> print(df.head())
                date   open  close   high    low    volume
        0 2024-01-01  25.50  25.68  25.80  25.40  15680000
    """
    # 去除股票代码前后空格
    stock_code = stock_code.strip()

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    try:
        # 验证股票代码格式
        if market_type == 'A':
            valid_prefixes = ['0', '3', '6', '688', '8']
            valid_format = False

            for prefix in valid_prefixes:
                if stock_code.startswith(prefix):
                    valid_format = True
                    break

            if not valid_format:
                error_msg = f"无效的A股股票代码格式: {stock_code}。A股代码应以0、3、6、688或8开头"
                raise ValueError(error_msg)

            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
        elif market_type == 'HK':
            print(f"[DEBUG] 获取港股数据: symbol={stock_code}")

            # 使用增强版港股数据获取器（如果可用）
            if HK_FETCHER_AVAILABLE and hk_stock_fetcher is not None:
                try:
                    print(f"[DEBUG] 使用增强版港股数据获取器")
                    result = hk_stock_fetcher.fetch_hk_stock_data(
                        stock_code=stock_code,
                        start_date=start_date,
                        end_date=end_date
                    )

                    if result['success']:
                        df = result['data']
                        print(f"[DEBUG] 增强版获取器成功获取{len(df)}条数据，数据源: {result['data_source']}")
                        print(f"[DEBUG] 港股数据列名: {df.columns.tolist()}")
                    else:
                        raise Exception(result['error_message'])

                except Exception as e:
                    print(f"[DEBUG] 增强版获取器失败: {str(e)}，回退到基础模式")
                    # 回退到基础模式
                    df = self._basic_hk_stock_fetch(stock_code, start_date, end_date)
            else:
                print(f"[DEBUG] 使用基础港股数据获取模式")
                df = self._basic_hk_stock_fetch(stock_code, start_date, end_date)
        elif market_type == 'US':
            print(f"[DEBUG] 获取美股数据: symbol={stock_code}")
            # 使用 stock_us_daily 替代 stock_us_hist，支持简单代码格式
            df = ak.stock_us_daily(
                symbol=stock_code,
                adjust="qfq"
            )
            print(f"[DEBUG] 美股数据返回类型: {type(df)}, 是否为None: {df is None}")
            if df is not None:
                print(f"[DEBUG] 美股数据列名: {df.columns.tolist()}")
                print(f"[DEBUG] 美股数据行数: {len(df)}")
                # 过滤日期范围
                df['date'] = pd.to_datetime(df['date'])
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
                print(f"[DEBUG] 过滤后数据行数: {len(df)}")
        elif market_type == 'ETF':
            # ETF数据获取，优先使用专用接口，失败时使用股票接口作为回退
            try:
                df = ak.fund_etf_hist_em(
                    symbol=stock_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                # 如果专用接口返回空数据，尝试使用股票接口
                if df is None or len(df) == 0:
                    print(f"[DEBUG] ETF专用接口返回空数据，尝试股票接口: {stock_code}")
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )
            except Exception as e1:
                print(f"[DEBUG] ETF专用接口失败: {str(e1)[:100]}，尝试股票接口: {stock_code}")
                try:
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )
                except Exception as e2:
                    raise Exception(f"ETF数据获取失败: 专用接口错误: {str(e1)[:100]}, 股票接口错误: {str(e2)[:100]}")

        elif market_type == 'LOF':
            # LOF数据获取，优先使用专用接口，失败时使用股票接口作为回退
            try:
                df = ak.fund_lof_hist_em(
                    symbol=stock_code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                # 如果专用接口返回空数据，尝试使用股票接口
                if df is None or len(df) == 0:
                    print(f"[DEBUG] LOF专用接口返回空数据，尝试股票接口: {stock_code}")
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )
            except Exception as e1:
                print(f"[DEBUG] LOF专用接口失败: {str(e1)[:100]}，尝试股票接口: {stock_code}")
                try:
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )
                except Exception as e2:
                    raise Exception(f"LOF数据获取失败: 专用接口错误: {str(e1)[:100]}, 股票接口错误: {str(e2)[:100]}")
        else:
            raise ValueError(f"不支持的市场类型: {market_type}")

        # 检查数据是否为空
        if df is None:
            raise Exception(f"无法获取{market_type}市场股票 {stock_code} 的数据，API返回None。请检查: 1) 股票代码是否正确 2) 日期范围是否有效 3) 网络连接是否正常")

        if len(df) == 0:
            raise Exception(f"获取到的数据为空，股票代码: {stock_code}, 市场: {market_type}, 日期范围: {start_date} 到 {end_date}")

        # 重命名列名以匹配分析需求
        df = df.rename(columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume"
        })

        # 确保日期格式正确
        df['date'] = pd.to_datetime(df['date'])

        # 数据类型转换
        numeric_columns = ['open', 'close', 'high', 'low', 'volume']
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

        # 删除空值
        df = df.dropna()

        return df.sort_values('date')

    except Exception as e:
        raise Exception(f"获取数据失败: {str(e)}")


def _basic_hk_stock_fetch(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    基础港股数据获取函数（回退方案）
    Basic HK Stock Data Fetch Function (Fallback)
    """
    print(f"[DEBUG] 使用基础港股数据获取模式: {stock_code}")

    # 港股代码格式转换 - 确保5位数字格式
    if stock_code.isdigit():
        hk_symbol = stock_code.zfill(5)  # 前面补0到5位
    else:
        hk_symbol = stock_code

    print(f"[DEBUG] 基础模式港股转换后代码: {hk_symbol}")

    try:
        # 首先尝试使用 stock_hk_hist
        try:
            df = ak.stock_hk_hist(
                symbol=hk_symbol,
                period="daily"
            )
            print(f"[DEBUG] 基础模式 stock_hk_hist 返回类型: {type(df)}, 是否为None: {df is None}")

            if df is not None and len(df) > 0:
                print(f"[DEBUG] 基础模式成功获取{len(df)}条数据")
            else:
                raise ValueError("stock_hk_hist 返回空数据")

        except Exception as e:
            print(f"[DEBUG] 基础模式 stock_hk_hist 失败: {str(e)}，尝试 stock_hk_daily")
            # 使用 stock_hk_daily 作为备选方案
            df = ak.stock_hk_daily(
                symbol=hk_symbol,
                adjust="qfq"  # 前复权
            )
            print(f"[DEBUG] 基础模式 stock_hk_daily 返回类型: {type(df)}, 是否为None: {df is None}")

        if df is not None and len(df) > 0:
            print(f"[DEBUG] 基础模式港股数据列名: {df.columns.tolist()}")
            print(f"[DEBUG] 基础模式港股数据行数: {len(df)}")

            # 检查列名格式，进行相应的重命名
            if 'date' in df.columns:
                # stock_hk_daily 格式，已经是英文列名
                pass
            else:
                # stock_hk_hist 格式，需要重命名中文列名到英文标准列名
                df = df.rename(columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume"
                })

            # 确保 date 列存在且格式正确
            if 'date' not in df.columns:
                raise Exception(f"港股数据缺少必要的date列，可用列: {df.columns.tolist()}")

            # 过滤日期范围
            df['date'] = pd.to_datetime(df['date'])
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            print(f"[DEBUG] 基础模式港股过滤后数据行数: {len(df)}")

            return df
        else:
            raise Exception(f"基础模式港股数据获取返回None或空数据，代码: {hk_symbol}")

    except Exception as hk_e:
        print(f"[DEBUG] 基础模式港股数据获取失败: {str(hk_e)}")
        raise Exception(f"基础模式获取港股数据失败: {str(hk_e)}。请检查股票代码格式，港股代码通常为5位数字，如 '00700' (腾讯)、'00005' (汇丰)")


def get_benchmark_data(market_type='A', start_date=None, end_date=None):
    """获取基准指数数据（用于计算Alpha/Beta）"""
    try:
        # 根据市场类型选择合适的基准指数
        if market_type == 'A':
            # 使用沪深300作为A股基准
            benchmark_code = '000300'
            df = ak.stock_zh_index_daily(symbol=f"sh{benchmark_code}")
        elif market_type == 'HK':
            # 使用恒生指数
            benchmark_code = 'HSI'
            df = ak.stock_hk_index_daily(symbol=benchmark_code)
        elif market_type == 'US':
            # 使用标普500
            df = ak.index_us_stock_sina(symbol='.INX')
        elif market_type in ['ETF', 'LOF']:
            # ETF和LOF使用沪深300
            benchmark_code = '000300'
            df = ak.stock_zh_index_daily(symbol=f"sh{benchmark_code}")
        else:
            return None

        # 标准化列名
        df = df.rename(columns={
            "date": "date",
            "close": "close",
            "收盘": "close",
            "日期": "date"
        })

        df['date'] = pd.to_datetime(df['date'])
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df[['date', 'close']].dropna()

        # 过滤日期范围
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df = df[df['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df = df[df['date'] <= end_dt]

        return df.sort_values('date')

    except Exception as e:
        print(f"获取基准数据失败: {str(e)}")
        return None


# ========== 基础技术指标函数 ==========

def calculate_ema(series, period):
    """计算指数移动平均线"""
    return series.ewm(span=period, adjust=False).mean()


def calculate_rsi(series, period):
    """计算RSI指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calculate_macd(series):
    """计算MACD指标"""
    exp1 = series.ewm(span=12, adjust=False).mean()
    exp2 = series.ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return macd, signal, hist


def calculate_bollinger_bands(series, period, std_dev):
    """计算布林带"""
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    return upper, middle, lower


def calculate_atr(df, period):
    """计算ATR指标"""
    high = df['high']
    low = df['low']
    close = df['close'].shift(1)

    tr1 = high - low
    tr2 = abs(high - close)
    tr3 = abs(low - close)

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


# ========== 新增国际级指标函数 ==========

def calculate_vwap(df):
    """
    计算VWAP (成交量加权平均价)
    这是机构交易员最核心的执行基准
    """
    df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()

    # 计算日内VWAP（按日期分组）
    df['date_only'] = df['date'].dt.date
    df['daily_vwap'] = df.groupby('date_only').apply(
        lambda x: (x['close'] * x['volume']).cumsum() / x['volume'].cumsum()
    ).reset_index(level=0, drop=True)
    df.drop('date_only', axis=1, inplace=True)

    return df


def calculate_adx(df, period=14):
    """
    计算ADX (平均趋向指数)
    衡量趋势强度，不判断方向
    ADX > 25: 强趋势
    ADX < 20: 震荡市
    """
    high = df['high']
    low = df['low']
    close = df['close']

    # 计算+DM和-DM
    plus_dm = high.diff()
    minus_dm = -low.diff()

    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm < 0] = 0

    # 计算TR (True Range)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 计算平滑的+DI和-DI
    atr = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

    # 计算DX和ADX
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    adx = dx.rolling(window=period).mean()

    return adx, plus_di, minus_di


def calculate_stochastic(df, period=14):
    """
    计算Stochastic Oscillator (随机振荡器)
    国际市场认可度高，比KDJ更平滑
    """
    low_min = df['low'].rolling(window=period).min()
    high_max = df['high'].rolling(window=period).max()

    k = 100 * (df['close'] - low_min) / (high_max - low_min)
    d = k.rolling(window=3).mean()

    return k, d


def calculate_williams_r(df, period=14):
    """
    计算Williams %R
    专业交易员标配动量指标，对超买超卖更敏感
    """
    high_max = df['high'].rolling(window=period).max()
    low_min = df['low'].rolling(window=period).min()

    williams_r = -100 * (high_max - df['close']) / (high_max - low_min)

    return williams_r


def calculate_obv(df):
    """
    计算OBV (能量潮)
    成交量分析的经典指标，预测价格趋势反转
    """
    obv = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
    return obv


def calculate_mfi(df, period=14):
    """
    计算MFI (资金流量指数)
    结合价格和成交量的RSI
    """
    typical_price = (df['high'] + df['low'] + df['close']) / 3
    money_flow = typical_price * df['volume']

    # 计算正负资金流
    positive_flow = money_flow.where(typical_price > typical_price.shift(1), 0)
    negative_flow = money_flow.where(typical_price < typical_price.shift(1), 0)

    # 计算资金比率
    positive_mf = positive_flow.rolling(window=period).sum()
    negative_mf = negative_flow.rolling(window=period).sum()

    mfi = 100 - (100 / (1 + positive_mf / negative_mf))

    return mfi


def calculate_momentum_factor(df, period=252):
    """
    计算Momentum Factor (动量因子)
    学术界最稳健的异象之一 (Jegadeesh & Titman, 1993)
    period默认252为年化交易日
    """
    momentum = df['close'].pct_change(periods=period) * 100
    return momentum


def calculate_risk_metrics(df, benchmark_df=None, risk_free_rate=0.03):
    """
    计算风险调整绩效指标
    - Sharpe Ratio: 夏普比率
    - Sortino Ratio: 索提诺比率
    - Maximum Drawdown: 最大回撤
    - Calmar Ratio: 卡玛比率
    - Alpha & Beta: 阿尔法和贝塔
    """
    # 计算日收益率
    returns = df['close'].pct_change().dropna()

    # 年化收益率 (假设252个交易日)
    annual_return = returns.mean() * 252

    # 年化波动率
    annual_volatility = returns.std() * np.sqrt(252)

    # Sharpe Ratio (夏普比率)
    sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility > 0 else 0

    # Sortino Ratio (索提诺比率 - 只考虑下行风险)
    downside_returns = returns[returns < 0]
    downside_volatility = downside_returns.std() * np.sqrt(252)
    sortino_ratio = (annual_return - risk_free_rate) / downside_volatility if downside_volatility > 0 else 0

    # Maximum Drawdown (最大回撤)
    cumulative_returns = (1 + returns).cumprod()
    running_max = cumulative_returns.cummax()
    drawdown = (cumulative_returns - running_max) / running_max
    max_drawdown = drawdown.min()

    # Calmar Ratio (卡玛比率)
    calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0

    # 计算Alpha和Beta
    alpha, beta = 0, 1
    if benchmark_df is not None and len(benchmark_df) > 0:
        try:
            # 合并股票和基准数据
            merged = pd.merge(
                df[['date', 'close']].rename(columns={'close': 'stock_close'}),
                benchmark_df[['date', 'close']].rename(columns={'close': 'benchmark_close'}),
                on='date',
                how='inner'
            )

            if len(merged) > 30:  # 确保有足够数据
                stock_returns = merged['stock_close'].pct_change().dropna()
                benchmark_returns = merged['benchmark_close'].pct_change().dropna()

                # 计算Beta (协方差 / 基准方差)
                covariance = stock_returns.cov(benchmark_returns)
                benchmark_variance = benchmark_returns.var()
                beta = covariance / benchmark_variance if benchmark_variance > 0 else 1

                # 计算Alpha (年化超额收益)
                stock_annual_return = stock_returns.mean() * 252
                benchmark_annual_return = benchmark_returns.mean() * 252
                alpha = stock_annual_return - (risk_free_rate + beta * (benchmark_annual_return - risk_free_rate))
        except Exception as e:
            print(f"计算Alpha/Beta时出错: {str(e)}")

    return {
        'sharpe_ratio': sharpe_ratio,
        'sortino_ratio': sortino_ratio,
        'max_drawdown': max_drawdown,
        'calmar_ratio': calmar_ratio,
        'alpha': alpha,
        'beta': beta,
        'annual_return': annual_return,
        'annual_volatility': annual_volatility
    }


def calculate_indicators(df, benchmark_df=None):
    """计算所有技术指标（包括新增的国际级指标）"""
    try:
        # ========== 原有指标 ==========
        # 计算移动平均线
        df['MA5'] = calculate_ema(df['close'], params['ma_periods']['short'])
        df['MA20'] = calculate_ema(df['close'], params['ma_periods']['medium'])
        df['MA60'] = calculate_ema(df['close'], params['ma_periods']['long'])

        # 计算RSI
        df['RSI'] = calculate_rsi(df['close'], params['rsi_period'])

        # 计算MACD
        df['MACD'], df['Signal'], df['MACD_hist'] = calculate_macd(df['close'])

        # 计算布林带
        df['BB_upper'], df['BB_middle'], df['BB_lower'] = calculate_bollinger_bands(
            df['close'],
            params['bollinger_period'],
            params['bollinger_std']
        )

        # 成交量分析
        df['Volume_MA'] = df['volume'].rolling(window=params['volume_ma_period']).mean()
        df['Volume_Ratio'] = df['volume'] / df['Volume_MA']

        # 计算ATR和波动率
        df['ATR'] = calculate_atr(df, params['atr_period'])
        df['Volatility'] = df['ATR'] / df['close'] * 100

        # 动量指标
        df['ROC'] = df['close'].pct_change(periods=10) * 100

        # ========== 新增国际级指标 ==========
        # VWAP (成交量加权平均价)
        df = calculate_vwap(df)

        # ADX (趋势强度指标)
        df['ADX'], df['Plus_DI'], df['Minus_DI'] = calculate_adx(df, params['adx_period'])

        # Stochastic Oscillator (随机振荡器)
        df['Stochastic_K'], df['Stochastic_D'] = calculate_stochastic(df, params['stochastic_period'])

        # Williams %R
        df['Williams_R'] = calculate_williams_r(df, params['williams_period'])

        # OBV (能量潮)
        df['OBV'] = calculate_obv(df)

        # MFI (资金流量指数)
        df['MFI'] = calculate_mfi(df)

        # Momentum Factor (动量因子)
        df['Momentum_Factor'] = calculate_momentum_factor(df, params['momentum_period'])

        # 风险调整指标（在后续单独计算并返回）
        risk_metrics = calculate_risk_metrics(df, benchmark_df)

        return df, risk_metrics

    except Exception as e:
        print(f"计算技术指标时出错: {str(e)}")
        raise


def calculate_score(df, risk_metrics):
    """
    计算评分（升级版，整合国际级指标）
    总分100分，分布如下：
    - 趋势分析: 20分
    - 动量指标: 20分
    - 成交量分析: 15分
    - 风险调整收益: 25分
    - 市场微观结构: 20分
    """
    try:
        score = 0
        latest = df.iloc[-1]

        # ========== 趋势分析 (20分) ==========
        # MA趋势
        if latest['MA5'] > latest['MA20'] > latest['MA60']:
            score += 10  # 完美多头排列
        elif latest['MA5'] > latest['MA20']:
            score += 5

        # ADX趋势强度
        if not pd.isna(latest['ADX']):
            if latest['ADX'] > 25:
                score += 10  # 强趋势
            elif latest['ADX'] > 20:
                score += 5

        # ========== 动量指标 (20分) ==========
        # RSI
        if not pd.isna(latest['RSI']):
            if 40 <= latest['RSI'] <= 60:
                score += 7  # 中性偏好
            elif 30 <= latest['RSI'] <= 70:
                score += 5
            elif latest['RSI'] < 30:
                score += 4  # 超卖有反弹机会

        # Stochastic
        if not pd.isna(latest['Stochastic_K']):
            if latest['Stochastic_K'] > latest['Stochastic_D'] and latest['Stochastic_K'] < 80:
                score += 7  # 金叉且未超买
            elif latest['Stochastic_K'] < 20:
                score += 4  # 超卖

        # MACD
        if not pd.isna(latest['MACD']) and not pd.isna(latest['Signal']):
            if latest['MACD'] > latest['Signal'] and latest['MACD_hist'] > 0:
                score += 6

        # ========== 成交量分析 (15分) ==========
        # 量比
        if not pd.isna(latest['Volume_Ratio']):
            if latest['Volume_Ratio'] > 2:
                score += 8  # 放量
            elif latest['Volume_Ratio'] > 1.5:
                score += 6
            elif latest['Volume_Ratio'] > 1:
                score += 3

        # OBV趋势
        if len(df) > 5:
            obv_trend = df['OBV'].iloc[-5:].diff().mean()
            if obv_trend > 0:
                score += 7  # OBV上升

        # ========== 风险调整收益 (25分) ==========
        # Sharpe Ratio
        if risk_metrics['sharpe_ratio'] > 2:
            score += 10  # 优秀
        elif risk_metrics['sharpe_ratio'] > 1:
            score += 7  # 良好
        elif risk_metrics['sharpe_ratio'] > 0:
            score += 4

        # Maximum Drawdown
        if risk_metrics['max_drawdown'] > -0.1:  # 回撤小于10%
            score += 8
        elif risk_metrics['max_drawdown'] > -0.2:  # 回撤小于20%
            score += 5
        elif risk_metrics['max_drawdown'] > -0.3:
            score += 2

        # Alpha (超额收益)
        if risk_metrics['alpha'] > 0.05:  # 年化超额收益>5%
            score += 7
        elif risk_metrics['alpha'] > 0:
            score += 4

        # ========== 市场微观结构 (20分) ==========
        # 价格相对VWAP位置
        if not pd.isna(latest['daily_vwap']):
            vwap_diff = (latest['close'] - latest['daily_vwap']) / latest['daily_vwap']
            if -0.02 < vwap_diff < 0.02:
                score += 10  # 价格在VWAP附近（公允价值）
            elif abs(vwap_diff) < 0.05:
                score += 5

        # MFI (资金流量)
        if not pd.isna(latest['MFI']):
            if 40 <= latest['MFI'] <= 60:
                score += 10  # 资金流中性
            elif 20 <= latest['MFI'] <= 80:
                score += 6
            elif latest['MFI'] < 20:
                score += 4  # 超卖

        return min(score, 100)  # 确保不超过100分

    except Exception as e:
        print(f"计算评分时出错: {str(e)}")
        raise


def get_recommendation(score):
    """根据得分给出建议（升级版）"""
    if score >= 80:
        return '强烈推荐买入 (Strong Buy)'
    elif score >= 65:
        return '推荐买入 (Buy)'
    elif score >= 50:
        return '谨慎买入 (Moderate Buy)'
    elif score >= 40:
        return '观望 (Hold)'
    elif score >= 25:
        return '谨慎卖出 (Moderate Sell)'
    else:
        return '推荐卖出 (Sell)'


def get_risk_level(risk_metrics):
    """根据风险指标评估风险等级"""
    max_dd = abs(risk_metrics['max_drawdown'])
    volatility = risk_metrics['annual_volatility']

    if max_dd < 0.1 and volatility < 0.2:
        return 'Low Risk (低风险)'
    elif max_dd < 0.2 and volatility < 0.3:
        return 'Medium Risk (中等风险)'
    elif max_dd < 0.3 and volatility < 0.4:
        return 'Medium-High Risk (中高风险)'
    else:
        return 'High Risk (高风险)'


def _truncate_json_for_logging(json_obj, max_length=500):
    """截断JSON对象用于日志记录，避免日志过大"""
    json_str = json.dumps(json_obj, ensure_ascii=False)
    if len(json_str) <= max_length:
        return json_str
    return json_str[:max_length] + f"... [截断，总长度: {len(json_str)}字符]"


@app.post("/analyze-stock/")
async def analyze_stock(request: StockAnalysisRequest, auth_token: str = Depends(verify_auth_token)):
    try:
        # 获取股票数据
        stock_data = get_stock_data(
            request.stock_code,
            request.market_type,
            request.start_date,
            request.end_date
        )

        # 获取基准数据（用于计算Alpha/Beta）
        benchmark_data = get_benchmark_data(
            request.market_type,
            request.start_date,
            request.end_date
        )

        print(f"股票数据记录数: {len(stock_data)}")
        if benchmark_data is not None:
            print(f"基准数据记录数: {len(benchmark_data)}")

        # 计算技术指标和风险指标
        stock_data, risk_metrics = calculate_indicators(stock_data, benchmark_data)

        # 计算评分
        score = calculate_score(stock_data, risk_metrics)

        # 获取最新数据
        latest = stock_data.iloc[-1]
        prev = stock_data.iloc[-2]

        # ========== 生成技术指标概要（升级版） ==========
        technical_summary = {
            'trend': 'upward' if latest['MA5'] > latest['MA20'] else 'downward',
            'volatility': f"{latest['Volatility']:.2f}%",
            'volume_trend': 'increasing' if latest['Volume_Ratio'] > 1 else 'decreasing',
            'rsi': float(latest['RSI']) if not pd.isna(latest['RSI']) else None,
            'adx': float(latest['ADX']) if not pd.isna(latest['ADX']) else None,
            'trend_strength': 'Strong' if (not pd.isna(latest['ADX']) and latest['ADX'] > 25) else 'Weak',
            'vwap_position': 'Above' if latest['close'] > latest['daily_vwap'] else 'Below',
            'stochastic_signal': 'Bullish' if (not pd.isna(latest['Stochastic_K']) and latest['Stochastic_K'] > latest['Stochastic_D']) else 'Bearish',
            'obv_trend': 'Rising' if len(stock_data) > 5 and stock_data['OBV'].iloc[-5:].diff().mean() > 0 else 'Falling',
            'mfi': float(latest['MFI']) if not pd.isna(latest['MFI']) else None
        }

        # ========== 风险调整指标摘要（新增） ==========
        risk_summary = {
            'sharpe_ratio': round(risk_metrics['sharpe_ratio'], 3),
            'sortino_ratio': round(risk_metrics['sortino_ratio'], 3),
            'max_drawdown': round(risk_metrics['max_drawdown'] * 100, 2),
            'calmar_ratio': round(risk_metrics['calmar_ratio'], 3),
            'alpha': round(risk_metrics['alpha'] * 100, 2),
            'beta': round(risk_metrics['beta'], 3),
            'annual_return': round(risk_metrics['annual_return'] * 100, 2),
            'annual_volatility': round(risk_metrics['annual_volatility'] * 100, 2),
            'risk_level': get_risk_level(risk_metrics)
        }

        # ========== 获取近14日交易数据（包含新指标） ==========
        recent_data = stock_data.tail(14).to_dict('records')

        # 清理NaN值并格式化
        for record in recent_data:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif isinstance(value, (np.integer, np.floating)):
                    record[key] = float(value)
                elif isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()

        # ========== 生成分析报告（升级版） ==========
        report = {
            'stock_code': request.stock_code,
            'market_type': request.market_type,
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'score': score,
            'recommendation': get_recommendation(score),
            'risk_level': risk_summary['risk_level'],
            'price': float(latest['close']),
            'price_change_pct': round((latest['close'] - prev['close']) / prev['close'] * 100, 2),
            'vwap': float(latest['daily_vwap']) if not pd.isna(latest['daily_vwap']) else None,
            'ma_trend': 'UP' if latest['MA5'] > latest['MA20'] else 'DOWN',
            'adx': float(latest['ADX']) if not pd.isna(latest['ADX']) else None,
            'trend_quality': 'Strong' if (not pd.isna(latest['ADX']) and latest['ADX'] > 25) else 'Weak',
            'rsi': float(latest['RSI']) if not pd.isna(latest['RSI']) else None,
            'stochastic_k': float(latest['Stochastic_K']) if not pd.isna(latest['Stochastic_K']) else None,
            'williams_r': float(latest['Williams_R']) if not pd.isna(latest['Williams_R']) else None,
            'macd_signal': 'BUY' if latest['MACD'] > latest['Signal'] else 'SELL',
            'volume_status': 'HIGH' if latest['Volume_Ratio'] > 1.5 else 'NORMAL' if latest['Volume_Ratio'] > 0.8 else 'LOW',
            'obv_signal': 'Bullish' if len(stock_data) > 5 and stock_data['OBV'].iloc[-5:].diff().mean() > 0 else 'Bearish',
            'mfi': float(latest['MFI']) if not pd.isna(latest['MFI']) else None,
            'sharpe_ratio': risk_summary['sharpe_ratio'],
            'max_drawdown_pct': risk_summary['max_drawdown'],
            'alpha_pct': risk_summary['alpha'],
            'beta': risk_summary['beta'],
            'annual_return_pct': risk_summary['annual_return']
        }

        # ========== 返回结果（保持原有结构，新增risk_summary） ==========
        return {
            "technical_summary": technical_summary,
            "risk_summary": risk_summary,  # 新增
            "recent_data": recent_data,
            "report": report
        }

    except Exception as e:
        print(f"分析过程出错: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# 健康检查路由
# Health Check Routes
# ============================================================================

@app.get("/health")
async def health_check():
    """
    健康检查接口
    Health check endpoint

    返回服务状态、版本信息和系统时间
    Returns service status, version info and system time
    """
    return {
        "status": "healthy",
        "service": "stock-analysis-api",
        "version": "2.0",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "markets_supported": ["A", "HK", "US", "ETF", "LOF"],
        "indicators_count": 20,
        "uptime": "running"
    }


@app.get("/health/ready")
async def readiness_check():
    """
    就绪检查接口 - 验证关键依赖是否可用
    Readiness check endpoint - verify critical dependencies
    """
    try:
        # 测试akshare是否可用
        import akshare as ak
        test_result = ak.__version__

        return {
            "status": "ready",
            "akshare_version": test_result,
            "dependencies": {
                "akshare": "available",
                "pandas": "available",
                "numpy": "available"
            },
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        return {
            "status": "not_ready",
            "error": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


@app.get("/health/live")
async def liveness_check():
    """
    存活检查接口 - 轻量级检查
    Liveness check endpoint - lightweight check
    """
    return {
        "status": "alive",
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8085)
