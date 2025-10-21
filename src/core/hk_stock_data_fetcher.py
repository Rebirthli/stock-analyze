#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
港股数据获取器 - 增强版
包含代码验证、格式化和多重数据源备选机制
"""

import akshare as ak
import pandas as pd
import time
import re
from typing import Optional, Dict, List, Tuple
from datetime import datetime


class HKStockDataFetcher:
    """增强版港股数据获取器"""

    def __init__(self):
        self.validator = HKStockCodeValidator()
        self.last_error = ""
        self.debug_mode = True

    def log_debug(self, message: str):
        """调试日志"""
        if self.debug_mode:
            print(f"[DEBUG] {message}")

    def format_hk_code(self, stock_code: str) -> str:
        """
        格式化港股代码为5位数字格式

        Args:
            stock_code: 输入的股票代码

        Returns:
            格式化的5位股票代码
        """
        if not stock_code or not isinstance(stock_code, str):
            return ""

        # 只提取数字部分
        digits = ''.join(filter(str.isdigit, stock_code))

        if not digits:
            return stock_code

        # 确保是5位数字，前面补0
        formatted_code = digits.zfill(5)

        self.log_debug(f"港股代码格式化: {stock_code} -> {formatted_code}")
        return formatted_code

    def validate_hk_code(self, stock_code: str) -> Dict[str, any]:
        """
        验证港股代码有效性

        Args:
            stock_code: 股票代码

        Returns:
            验证结果字典
        """
        formatted_code = self.format_hk_code(stock_code)

        result = {
            'original_code': stock_code,
            'formatted_code': formatted_code,
            'is_valid': False,
            'error_message': '',
            'stock_name': ''
        }

        # 基本格式验证
        if not formatted_code:
            result['error_message'] = '股票代码不能为空'
            return result

        if len(formatted_code) != 5:
            result['error_message'] = f'港股代码必须是5位数字，当前为{len(formatted_code)}位'
            return result

        if not formatted_code.isdigit():
            result['error_message'] = '港股代码必须全部是数字'
            return result

        # 简单验证代码范围（港股代码通常以0开头）
        if not formatted_code.startswith('0'):
            self.log_debug(f"警告：港股代码{formatted_code}不是以0开头")

        result['is_valid'] = True
        return result

    def try_stock_hk_hist(self, stock_code: str, period: str = "daily") -> Optional[pd.DataFrame]:
        """尝试使用stock_hk_hist获取数据"""
        try:
            self.log_debug(f"尝试使用stock_hk_hist获取数据: {stock_code}")
            df = ak.stock_hk_hist(symbol=stock_code, period=period)

            if df is not None and not df.empty:
                self.log_debug(f"stock_hk_hist成功获取{len(df)}条数据")
                return df
            else:
                self.log_debug("stock_hk_hist返回空数据")
                return None

        except Exception as e:
            self.log_debug(f"stock_hk_hist失败: {str(e)}")
            return None

    def try_stock_hk_daily(self, stock_code: str, adjust: str = "qfq") -> Optional[pd.DataFrame]:
        """尝试使用stock_hk_daily获取数据"""
        try:
            self.log_debug(f"尝试使用stock_hk_daily获取数据: {stock_code}")
            df = ak.stock_hk_daily(symbol=stock_code, adjust=adjust)

            if df is not None and not df.empty:
                self.log_debug(f"stock_hk_daily成功获取{len(df)}条数据")
                return df
            else:
                self.log_debug("stock_hk_daily返回空数据")
                return None

        except Exception as e:
            self.log_debug(f"stock_hk_daily失败: {str(e)}")
            return None

    def try_stock_hk_spot(self, stock_code: str) -> Optional[pd.DataFrame]:
        """尝试使用stock_hk_spot_em获取实时数据作为备选"""
        try:
            self.log_debug(f"尝试使用stock_hk_spot_em获取实时数据")
            df = ak.stock_hk_spot_em()

            if df is not None and not df.empty:
                # 查找指定股票代码
                code_col = None
                for col in ['代码', 'code', 'symbol']:
                    if col in df.columns:
                        code_col = col
                        break

                if code_col:
                    # 过滤指定股票
                    stock_df = df[df[code_col].astype(str) == stock_code]
                    if not stock_df.empty:
                        self.log_debug(f"stock_hk_spot_em找到{len(stock_df)}条实时数据")
                        return stock_df

            self.log_debug("stock_hk_spot_em未找到指定股票数据")
            return None

        except Exception as e:
            self.log_debug(f"stock_hk_spot_em失败: {str(e)}")
            return None

    def standardize_data_format(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        标准化数据格式

        Args:
            df: 原始数据DataFrame
            source: 数据源名称

        Returns:
            标准化后的DataFrame
        """
        self.log_debug(f"标准化数据格式，数据源: {source}")

        df_copy = df.copy()

        # 定义列名映射
        column_mappings = {
            'stock_hk_hist': {
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume"
            },
            'stock_hk_daily': {
                "date": "date",
                "open": "open",
                "close": "close",
                "high": "high",
                "low": "low",
                "volume": "volume"
            }
        }

        # 根据数据源进行列名映射
        if source in column_mappings:
            mapping = column_mappings[source]
            available_columns = {k: v for k, v in mapping.items() if k in df_copy.columns}
            if available_columns:
                df_copy = df_copy.rename(columns=available_columns)
                self.log_debug(f"应用列名映射: {available_columns}")

        # 确保必要的列存在
        required_columns = ['date', 'open', 'close', 'high', 'low', 'volume']
        missing_columns = [col for col in required_columns if col not in df_copy.columns]

        if missing_columns:
            self.log_debug(f"警告：缺少必要列: {missing_columns}")
            # 尝试从其他列推断
            fallback_mappings = {
                '成交': 'volume',
                '成交额': 'volume',
                '开盘价': 'open',
                '收盘价': 'close',
                '最高价': 'high',
                '最低价': 'low'
            }

            for missing_col in missing_columns:
                for alt_col, target_col in fallback_mappings.items():
                    if alt_col in df_copy.columns and target_col == missing_col:
                        df_copy = df_copy.rename(columns={alt_col: target_col})
                        self.log_debug(f"使用备选列名: {alt_col} -> {target_col}")
                        break

        # 标准化日期格式
        if 'date' in df_copy.columns:
            try:
                df_copy['date'] = pd.to_datetime(df_copy['date'])
            except Exception as e:
                self.log_debug(f"日期格式转换失败: {str(e)}")

        # 确保数值列为数值类型
        numeric_columns = ['open', 'close', 'high', 'low', 'volume']
        for col in numeric_columns:
            if col in df_copy.columns:
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

        # 只保留必要的列
        available_columns = [col for col in required_columns if col in df_copy.columns]
        if available_columns:
            df_copy = df_copy[available_columns]

        self.log_debug(f"标准化完成，可用列: {list(df_copy.columns)}")
        return df_copy

    def filter_by_date_range(self, df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        """按日期范围过滤数据"""
        if df.empty or 'date' not in df.columns:
            return df

        try:
            df['date'] = pd.to_datetime(df['date'])
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)

            filtered_df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]

            self.log_debug(f"日期过滤: {len(df)} -> {len(filtered_df)} 条数据")
            return filtered_df

        except Exception as e:
            self.log_debug(f"日期过滤失败: {str(e)}")
            return df

    def fetch_hk_stock_data(self, stock_code: str, start_date: str, end_date: str,
                           max_retries: int = 3, retry_delay: float = 1.0) -> Dict[str, any]:
        """
        获取港股数据的主方法

        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            max_retries: 最大重试次数
            retry_delay: 重试延迟

        Returns:
            包含数据获取结果的字典
        """
        result = {
            'success': False,
            'data': None,
            'data_source': '',
            'error_message': '',
            'records_count': 0,
            'code_used': ''
        }

        self.log_debug(f"开始获取港股数据: {stock_code}, 日期范围: {start_date} 至 {end_date}")

        # 1. 验证和格式化代码
        validation_result = self.validate_hk_code(stock_code)
        if not validation_result['is_valid']:
            result['error_message'] = validation_result['error_message']
            return result

        formatted_code = validation_result['formatted_code']
        result['code_used'] = formatted_code

        # 2. 尝试多个数据源
        data_sources = [
            ('stock_hk_hist', lambda: self.try_stock_hk_hist(formatted_code)),
            ('stock_hk_daily', lambda: self.try_stock_hk_daily(formatted_code)),
            ('stock_hk_spot', lambda: self.try_stock_hk_spot(formatted_code))
        ]

        for source_name, fetch_func in data_sources:
            self.log_debug(f"尝试数据源: {source_name}")

            for attempt in range(max_retries):
                try:
                    df = fetch_func()

                    if df is not None and not df.empty:
                        # 标准化数据格式
                        standardized_df = self.standardize_data_format(df, source_name)

                        # 过滤日期范围
                        filtered_df = self.filter_by_date_range(standardized_df, start_date, end_date)

                        if not filtered_df.empty:
                            result['success'] = True
                            result['data'] = filtered_df
                            result['data_source'] = source_name
                            result['records_count'] = len(filtered_df)

                            self.log_debug(f"成功从{source_name}获取{len(filtered_df)}条数据")
                            return result
                        else:
                            self.log_debug(f"{source_name}数据在指定日期范围内为空")

                    # 如果没有成功，等待后重试
                    if attempt < max_retries - 1:
                        self.log_debug(f"{source_name}第{attempt + 1}次尝试失败，等待{retry_delay}秒后重试")
                        time.sleep(retry_delay)

                except Exception as e:
                    self.log_debug(f"{source_name}第{attempt + 1}次尝试异常: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)

        # 所有数据源都失败
        result['error_message'] = f"所有数据源都无法获取股票{formatted_code}的数据"
        self.log_debug(result['error_message'])
        return result


class HKStockCodeValidator:
    """港股代码验证器（简化版）"""

    def validate_code(self, stock_code: str) -> Dict[str, any]:
        """验证港股代码"""
        result = {
            'original_code': stock_code,
            'formatted_code': '',
            'is_valid': False,
            'error_message': ''
        }

        if not stock_code or not isinstance(stock_code, str):
            result['error_message'] = '股票代码不能为空'
            return result

        # 只提取数字部分
        digits = ''.join(filter(str.isdigit, stock_code))

        if not digits:
            result['error_message'] = '股票代码必须包含数字'
            return result

        # 确保是5位数字，前面补0
        formatted_code = digits.zfill(5)

        if len(formatted_code) != 5:
            result['error_message'] = f'港股代码必须是5位数字，当前为{len(formatted_code)}位'
            return result

        result['formatted_code'] = formatted_code
        result['is_valid'] = True

        return result


def test_enhanced_hk_fetcher():
    """测试增强版港股数据获取器"""
    print("测试增强版港股数据获取器:")
    print("=" * 60)

    fetcher = HKStockDataFetcher()

    # 测试用例 - 包含之前失败的案例
    test_cases = [
        ("0992", "联想集团-错误格式"),
        ("992", "联想集团-超短格式"),
        ("00992", "联想集团-正确格式"),
        ("00700", "腾讯控股"),
        ("00005", "汇丰控股"),
        ("02318", "中国平安"),
        ("99999", "不存在的代码"),
    ]

    for code, description in test_cases:
        print(f"\n测试: {description}")
        print(f"输入代码: {code}")
        print("-" * 40)

        result = fetcher.fetch_hk_stock_data(
            stock_code=code,
            start_date="2024-01-01",
            end_date="2024-12-31"
        )

        print(f"成功: {result['success']}")
        print(f"使用代码: {result['code_used']}")
        print(f"数据源: {result['data_source']}")
        print(f"记录数: {result['records_count']}")

        if result['error_message']:
            print(f"错误: {result['error_message']}")

        if result['success'] and result['data'] is not None:
            print(f"数据列: {list(result['data'].columns)}")
            print(f"数据预览:")
            print(result['data'].head(3))

        # 等待一下，避免请求过快
        time.sleep(1)


if __name__ == "__main__":
    test_enhanced_hk_fetcher()