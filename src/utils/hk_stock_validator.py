#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
港股代码验证和格式化工具
"""

import akshare as ak
import pandas as pd
import time
from typing import Optional, List, Dict


class HKStockValidator:
    """港股代码验证器"""

    def __init__(self):
        self._valid_codes: Optional[List[str]] = None
        self._code_name_map: Optional[Dict[str, str]] = None
        self._last_update: Optional[float] = None
        self._cache_timeout = 3600  # 缓存1小时

    def _load_hk_stock_list(self) -> bool:
        """加载港股代码列表"""
        try:
            print("[DEBUG] 正在加载港股代码列表...")
            df = ak.stock_hk_spot_em()

            if df is not None and not df.empty:
                # 提取代码列
                if '代码' in df.columns:
                    self._valid_codes = df['代码'].astype(str).tolist()
                elif 'code' in df.columns:
                    self._valid_codes = df['code'].astype(str).tolist()
                else:
                    # 尝试第一列作为代码
                    self._valid_codes = df.iloc[:, 0].astype(str).tolist()

                # 构建代码-名称映射
                if '名称' in df.columns and '代码' in df.columns:
                    self._code_name_map = dict(zip(df['代码'].astype(str), df['名称']))
                elif 'name' in df.columns and 'code' in df.columns:
                    self._code_name_map = dict(zip(df['code'].astype(str), df['name']))

                self._last_update = time.time()
                print(f"[DEBUG] 成功加载 {len(self._valid_codes)} 个港股代码")
                return True
            else:
                print("[DEBUG] 未能获取港股代码列表")
                return False

        except Exception as e:
            print(f"[DEBUG] 加载港股代码列表失败: {str(e)}")
            return False

    def _ensure_cache_valid(self) -> bool:
        """确保缓存有效"""
        if (self._valid_codes is None or
            self._last_update is None or
            time.time() - self._last_update > self._cache_timeout):
            return self._load_hk_stock_list()
        return True

    def format_hk_code(self, stock_code: str) -> str:
        """
        格式化港股代码为5位数字格式

        Args:
            stock_code: 输入的股票代码，可以是各种格式

        Returns:
            格式化为5位的港股代码
        """
        if not stock_code or not isinstance(stock_code, str):
            return ""

        # 只提取数字部分
        digits = ''.join(filter(str.isdigit, stock_code))

        if not digits:
            return stock_code

        # 确保是5位数字，前面补0
        formatted_code = digits.zfill(5)

        return formatted_code

    def is_valid_hk_code(self, stock_code: str) -> bool:
        """
        验证港股代码是否有效

        Args:
            stock_code: 股票代码

        Returns:
            是否有效
        """
        formatted_code = self.format_hk_code(stock_code)

        # 确保缓存有效
        if not self._ensure_cache_valid():
            # 如果无法加载列表，只进行格式验证
            return len(formatted_code) == 5 and formatted_code.isdigit()

        # 检查代码是否在有效列表中
        return formatted_code in self._valid_codes

    def get_stock_name(self, stock_code: str) -> str:
        """
        获取股票名称

        Args:
            stock_code: 股票代码

        Returns:
            股票名称
        """
        formatted_code = self.format_hk_code(stock_code)

        if self._code_name_map and formatted_code in self._code_name_map:
            return self._code_name_map[formatted_code]

        return "未知股票"

    def get_similar_codes(self, stock_code: str, max_results: int = 5) -> List[str]:
        """
        获取相似的股票代码（用于错误提示）

        Args:
            stock_code: 输入的代码
            max_results: 最大返回数量

        Returns:
            相似的代码列表
        """
        formatted_code = self.format_hk_code(stock_code)

        if not self._ensure_cache_valid():
            return []

        # 简单的相似度匹配：数字相似
        similar_codes = []
        for valid_code in self._valid_codes:
            # 计算相似度（相同数字的数量）
            similarity = sum(1 for a, b in zip(formatted_code, valid_code) if a == b)
            if similarity >= 3:  # 至少3位相同
                similar_codes.append((valid_code, similarity))

        # 按相似度排序并返回前N个
        similar_codes.sort(key=lambda x: x[1], reverse=True)
        return [code for code, _ in similar_codes[:max_results]]

    def validate_and_fix(self, stock_code: str) -> Dict[str, any]:
        """
        验证并修复港股代码

        Args:
            stock_code: 输入的股票代码

        Returns:
            包含验证结果和修复建议的字典
        """
        result = {
            'original_code': stock_code,
            'formatted_code': '',
            'is_valid': False,
            'stock_name': '',
            'error_message': '',
            'suggestions': []
        }

        # 格式化代码
        formatted_code = self.format_hk_code(stock_code)
        result['formatted_code'] = formatted_code

        # 基本格式验证
        if not formatted_code:
            result['error_message'] = '股票代码不能为空'
            return result

        if len(formatted_code) != 5:
            result['error_message'] = f'港股代码必须是5位数字，当前为{len(formatted_code)}位'
            result['suggestions'].append(f'建议使用5位格式：{formatted_code}')

        # 验证代码是否存在
        if self.is_valid_hk_code(formatted_code):
            result['is_valid'] = True
            result['stock_name'] = self.get_stock_name(formatted_code)
        else:
            result['error_message'] = f'股票代码 {formatted_code} 不存在或无效'

            # 获取相似代码建议
            similar_codes = self.get_similar_codes(stock_code)
            if similar_codes:
                result['suggestions'].extend(similar_codes)

        return result


def test_validator():
    """测试验证器"""
    print("测试港股代码验证器:")
    print("=" * 50)

    validator = HKStockValidator()

    # 测试用例
    test_cases = [
        "0992",    # 联想集团错误格式
        "992",     # 联想集团超短格式
        "00992",   # 联想集团正确格式
        "00700",   # 腾讯控股
        "00005",   # 汇丰控股
        "02318",   # 中国平安
        "99999",   # 不存在的代码
        "ABC",     # 非数字代码
        "",        # 空代码
    ]

    for code in test_cases:
        print(f"\n测试代码: {code}")
        result = validator.validate_and_fix(code)

        print(f"格式化代码: {result['formatted_code']}")
        print(f"是否有效: {result['is_valid']}")
        print(f"股票名称: {result['stock_name']}")
        if result['error_message']:
            print(f"错误信息: {result['error_message']}")
        if result['suggestions']:
            print(f"建议: {result['suggestions']}")


if __name__ == "__main__":
    test_validator()