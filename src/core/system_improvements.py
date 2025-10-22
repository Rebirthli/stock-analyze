import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
import akshare as ak

logger = logging.getLogger(__name__)

def get_company_name(stock_code: str, market_type: str) -> str:
    """
    获取公司名称 - 增强版错误处理

    Args:
        stock_code: 股票代码
        market_type: 市场类型

    Returns:
        str: 公司名称
    """
    try:
        if market_type == 'A':
            # A股获取股票名称
            stock_info = ak.stock_individual_info_em(symbol=stock_code)
            if stock_info is not None and not stock_info.empty:
                name_item = stock_info[stock_info['item'] == '股票简称']
                if not name_item.empty:
                    return name_item.iloc[0]['value']

        elif market_type == 'HK':
            # 港股获取股票名称 - 改进版本
            try:
                # 方案1: 尝试使用个股信息查询
                stock_info = ak.stock_individual_info_em(symbol=f"HK{stock_code}")
                if stock_info is not None and not stock_info.empty:
                    name_item = stock_info[stock_info['item'] == '股票简称']
                    if not name_item.empty:
                        return name_item.iloc[0]['value']
            except Exception as e1:
                logger.debug(f"港股个股信息查询失败: {str(e1)[:50]}")

            try:
                # 方案2: 尝试使用港股实时行情
                stock_info = ak.stock_hk_spot_em()
                if stock_info is not None and not stock_info.empty:
                    # 尝试多种代码格式匹配
                    code_variants = [stock_code, f"0{stock_code}", f"HK{stock_code}"]
                    for code in code_variants:
                        stock_row = stock_info[stock_info['代码'] == code]
                        if not stock_row.empty:
                            return stock_row.iloc[0]['名称']
            except Exception as e2:
                logger.debug(f"港股实时行情查询失败: {str(e2)[:50]}")

            try:
                # 方案3: 尝试使用历史数据获取名称
                hist_data = ak.stock_hk_hist(symbol=stock_code, period="daily",
                                           start_date="20241001", end_date="20241031")
                if hist_data is not None and not hist_data.empty:
                    # 如果历史数据获取成功，但无法获取名称，返回标准格式
                    return f"HK股票{stock_code}"
            except Exception as e3:
                logger.debug(f"港股历史数据查询失败: {str(e3)[:50]}")

        elif market_type == 'US':
            # 美股获取股票名称
            stock_info = ak.stock_us_spot_em()
            if stock_info is not None and not stock_info.empty:
                stock_row = stock_info[stock_info['代码'] == stock_code]
                if not stock_row.empty:
                    return stock_row.iloc[0]['名称']

        elif market_type in ['ETF', 'LOF']:
            # ETF/LOF获取基金名称
            try:
                if market_type == 'ETF':
                    fund_info = ak.fund_etf_spot_em()
                else:
                    fund_info = ak.fund_lof_spot_em()

                if fund_info is not None and not fund_info.empty:
                    fund_row = fund_info[fund_info['代码'] == stock_code]
                    if not fund_row.empty:
                        return fund_row.iloc[0]['名称']
            except Exception:
                pass

        # 如果无法获取名称，返回默认值
        return f"{market_type}股票{stock_code}"

    except Exception as e:
        logger.warning(f"获取公司名称失败 {stock_code}({market_type}): {str(e)}")
        return f"{market_type}股票{stock_code}"

def safe_float_convert(value: Any, default: float = 0.0) -> float:
    """
    安全的浮点数转换

    Args:
        value: 要转换的值
        default: 默认值

    Returns:
        float: 转换后的浮点数
    """
    try:
        if pd.isna(value):
            return default
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return float(value)
        return default
    except (ValueError, TypeError):
        return default

def preprocess_recent_data(recent_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    预处理历史数据，确保数据类型正确

    Args:
        recent_df: 历史数据DataFrame

    Returns:
        List[Dict[str, Any]]: 处理后的历史数据列表
    """
    try:
        recent_data_list = []

        # 标准化列名映射
        column_mapping = {
            '股票代码': 'stock_code',
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '换手率': 'turnover_rate'
        }

        df_clean = recent_df.copy()
        df_clean.rename(columns=column_mapping, inplace=True)

        for _, row in df_clean.iterrows():
            row_dict = {}
            for col in df_clean.columns:
                value = row[col]

                if pd.isna(value):
                    row_dict[col] = None
                elif isinstance(value, (np.integer, np.floating)):
                    row_dict[col] = float(value)
                elif isinstance(value, pd.Timestamp):
                    row_dict[col] = value.isoformat()
                elif isinstance(value, (int, float)):
                    row_dict[col] = float(value)
                else:
                    row_dict[col] = str(value)
            recent_data_list.append(row_dict)

        return recent_data_list

    except Exception as e:
        logger.error(f"预处理历史数据失败: {str(e)}")
        return []

def validate_stock_data(df: pd.DataFrame) -> bool:
    """
    验证股票数据的有效性

    Args:
        df: 股票数据DataFrame

    Returns:
        bool: 数据是否有效
    """
    try:
        if df is None or df.empty:
            return False

        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"缺少必需列: {col}")
                return False

        # 检查数据完整性
        if len(df) < 10:  # 至少需要10条数据
            logger.warning(f"数据量过少: {len(df)} 条记录")
            return False

        # 检查价格数据合理性
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if (df[col] <= 0).any():
                logger.warning(f"发现非正价格数据在列: {col}")
                return False

        # 检查成交量合理性
        if (df['volume'] < 0).any():
            logger.warning("发现负成交量数据")
            return False

        return True

    except Exception as e:
        logger.error(f"验证股票数据失败: {str(e)}")
        return False

def validate_technical_indicators(indicators: Dict[str, Any]) -> Dict[str, Any]:
    """
    验证和清理技术指标数据

    Args:
        indicators: 原始技术指标字典

    Returns:
        Dict[str, Any]: 清理后的技术指标字典
    """
    try:
        validated_indicators = {}

        for key, value in indicators.items():
            if isinstance(value, (int, float, np.integer, np.floating)):
                if not np.isfinite(value):  # 处理inf和-nan
                    validated_indicators[key] = None
                else:
                    validated_indicators[key] = float(value)
            elif pd.isna(value):
                validated_indicators[key] = None
            else:
                validated_indicators[key] = value

        return validated_indicators

    except Exception as e:
        logger.error(f"验证技术指标失败: {str(e)}")
        return {}

def calculate_risk_level_safe(risk_metrics: Dict[str, Any]) -> str:
    """
    安全计算风险等级

    Args:
        risk_metrics: 风险指标字典

    Returns:
        str: 风险等级
    """
    try:
        max_dd = safe_float_convert(risk_metrics.get('max_drawdown', 0))
        volatility = safe_float_convert(risk_metrics.get('volatility', 0))

        max_dd_abs = abs(max_dd)

        if max_dd_abs < 0.1 and volatility < 0.2:
            return 'Low Risk (低风险)'
        elif max_dd_abs < 0.2 and volatility < 0.3:
            return 'Medium Risk (中等风险)'
        elif max_dd_abs < 0.3 and volatility < 0.4:
            return 'Medium-High Risk (中高风险)'
        else:
            return 'High Risk (高风险)'

    except Exception as e:
        logger.error(f"计算风险等级失败: {str(e)}")
        return 'Unknown Risk (风险未知)'

def enhanced_error_handler(func):
    """
    增强错误处理装饰器
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{func.__name__} 执行失败: {str(e)}")
            # 根据函数类型返回合适的默认值
            if func.__annotations__.get('return') == Dict[str, Any]:
                return {}
            elif func.__annotations__.get('return') == str:
                return 'Unknown (未知)'
            elif func.__annotations__.get('return') == float:
                return 0.0
            elif func.__annotations__.get('return') == bool:
                return False
            else:
                return None
    return wrapper

@enhanced_error_handler
def get_technical_summary_safe(technical_indicators: Dict[str, Any]) -> Dict[str, Any]:
    """
    安全生成技术指标概要

    Args:
        technical_indicators: 技术指标字典

    Returns:
        Dict[str, Any]: 技术指标概要
    """
    try:
        ma_short = safe_float_convert(technical_indicators.get('ma_short', 0))
        ma_medium = safe_float_convert(technical_indicators.get('ma_medium', 0))
        rsi = safe_float_convert(technical_indicators.get('rsi', 50))
        macd_histogram = safe_float_convert(technical_indicators.get('macd_histogram', 0))
        volume_ratio = safe_float_convert(technical_indicators.get('volume_ratio', 1))
        atr = safe_float_convert(technical_indicators.get('atr', 0))

        return {
            'trend': 'upward' if ma_short > ma_medium else 'downward',
            'rsi': rsi if rsi <= 100 else None,  # RSI应在0-100之间
            'macd_signal': 'bullish' if macd_histogram > 0 else 'bearish',
            'bollinger_position': safe_float_convert(technical_indicators.get('bollinger_position', 0.5)),
            'volume_ratio': max(0, volume_ratio),  # 成交量比不应为负
            'atr': max(0, atr),  # ATR不应为负
            'trend_strength': 'strong' if rsi > 50 else 'weak'
        }
    except Exception as e:
        logger.error(f"生成技术指标概要失败: {str(e)}")
        return {}

@enhanced_error_handler
def get_risk_summary_safe(risk_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    安全生成风险指标概要

    Args:
        risk_metrics: 风险指标字典

    Returns:
        Dict[str, Any]: 风险指标概要
    """
    try:
        volatility = safe_float_convert(risk_metrics.get('volatility', 0))
        sharpe_ratio = safe_float_convert(risk_metrics.get('sharpe_ratio', 0))
        max_drawdown = safe_float_convert(risk_metrics.get('max_drawdown', 0))
        var_95 = safe_float_convert(risk_metrics.get('var_95', 0))
        var_99 = safe_float_convert(risk_metrics.get('var_99', 0))

        return {
            'volatility': round(volatility * 100, 2),
            'sharpe_ratio': round(sharpe_ratio, 3),
            'max_drawdown': round(max_drawdown * 100, 2),
            'var_95': round(var_95 * 100, 2),
            'var_99': round(var_99 * 100, 2),
            'risk_level': calculate_risk_level_safe(risk_metrics)
        }
    except Exception as e:
        logger.error(f"生成风险指标概要失败: {str(e)}")
        return {}

@enhanced_error_handler
def get_trend_quality_safe(technical_indicators: Dict[str, Any]) -> str:
    """
    安全判断趋势质量

    Args:
        technical_indicators: 技术指标字典

    Returns:
        str: 趋势质量描述
    """
    try:
        rsi = safe_float_convert(technical_indicators.get('rsi', 50))
        macd_hist = safe_float_convert(technical_indicators.get('macd_histogram', 0))

        if rsi > 50 and macd_hist > 0:
            return 'Strong Up (强势上涨)'
        elif rsi < 50 and macd_hist < 0:
            return 'Strong Down (强势下跌)'
        elif rsi > 50:
            return 'Weak Up (弱势上涨)'
        else:
            return 'Weak Down (弱势下跌)'
    except Exception as e:
        logger.error(f"判断趋势质量失败: {str(e)}")
        return 'Unknown (未知)'

@enhanced_error_handler
def get_volume_status_safe(technical_indicators: Dict[str, Any]) -> str:
    """
    安全判断成交量状态

    Args:
        technical_indicators: 技术指标字典

    Returns:
        str: 成交量状态描述
    """
    try:
        volume_ratio = safe_float_convert(technical_indicators.get('volume_ratio', 1))

        if volume_ratio > 2:
            return 'High Volume (放量)'
        elif volume_ratio > 1.2:
            return 'Normal Volume (正常)'
        else:
            return 'Low Volume (缩量)'
    except Exception as e:
        logger.error(f"判断成交量状态失败: {str(e)}")
        return 'Unknown (未知)'