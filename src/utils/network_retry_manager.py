"""
网络重试管理器
处理数据源连接问题，提供备用数据源和重试机制
"""

import time
import random
import requests
from typing import Optional, Callable, Any, Dict
from functools import wraps
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NetworkRetryManager:
    """网络重试管理器"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 10.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def exponential_backoff(self, attempt: int) -> float:
        """计算指数退避延迟"""
        delay = min(self.base_delay * (2 ** attempt) + random.uniform(0, 1), self.max_delay)
        return delay

    def retry_with_backoff(self, func: Callable, *args, **kwargs) -> Any:
        """使用指数退避重试函数"""
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                logger.info(f"尝试 {func.__name__} 第 {attempt + 1} 次")
                result = func(*args, **kwargs)
                if result is not None:
                    logger.info(f"{func.__name__} 成功")
                    return result
                else:
                    logger.warning(f"{func.__name__} 返回 None，尝试备用方案")

            except Exception as e:
                last_exception = e
                logger.warning(f"{func.__name__} 第 {attempt + 1} 次失败: {str(e)}")

                if attempt < self.max_retries - 1:
                    delay = self.exponential_backoff(attempt)
                    logger.info(f"等待 {delay:.1f} 秒后重试")
                    time.sleep(delay)
                else:
                    logger.error(f"{func.__name__} 所有重试都失败")

        # 如果所有重试都失败，返回 None 或抛出异常
        if last_exception:
            logger.error(f"最终失败: {str(last_exception)}")
            return None
        return None


class DataSourceManager:
    """数据源管理器"""

    def __init__(self):
        self.retry_manager = NetworkRetryManager()

        # 备用数据源配置
        self.backup_sources = {
            'hk_stock': [
                {'name': 'stock_hk_hist', 'priority': 1},
                {'name': 'stock_hk_daily', 'priority': 2},
                {'name': 'stock_hk_spot', 'priority': 3}
            ],
            'a_stock': [
                {'name': 'stock_zh_a_hist', 'priority': 1},
                {'name': 'stock_zh_a_spot', 'priority': 2}
            ],
            'us_stock': [
                {'name': 'stock_us_daily', 'priority': 1},
                {'name': 'stock_us_spot', 'priority': 2}
            ],
            'etf': [
                {'name': 'fund_etf_hist_em', 'priority': 1},
                {'name': 'stock_zh_a_hist', 'priority': 2}  # 回退到股票接口
            ],
            'lof': [
                {'name': 'fund_lof_hist_em', 'priority': 1},
                {'name': 'stock_zh_a_hist', 'priority': 2}  # 回退到股票接口
            ]
        }

    def get_backup_sources(self, data_type: str) -> list:
        """获取指定数据类型的备用数据源"""
        return self.backup_sources.get(data_type, [])

    def test_network_connectivity(self, url: str, timeout: int = 5) -> bool:
        """测试网络连接"""
        try:
            response = requests.get(url, timeout=timeout)
            return response.status_code == 200
        except:
            return False

    def get_working_data_source(self, data_type: str, test_func: Callable) -> Optional[str]:
        """获取可用的数据源"""
        sources = self.get_backup_sources(data_type)

        for source in sorted(sources, key=lambda x: x['priority']):
            source_name = source['name']
            logger.info(f"测试数据源: {source_name}")

            try:
                # 使用重试管理器测试数据源
                result = self.retry_manager.retry_with_backoff(test_func, source_name)
                if result is not None:
                    logger.info(f"数据源 {source_name} 可用")
                    return source_name
                else:
                    logger.warning(f"数据源 {source_name} 不可用")
            except Exception as e:
                logger.error(f"数据源 {source_name} 测试失败: {str(e)}")

        logger.error(f"所有 {data_type} 数据源都不可用")
        return None


# 全局实例
network_retry_manager = NetworkRetryManager()
data_source_manager = DataSourceManager()


def with_network_retry(max_retries: int = 3, base_delay: float = 1.0):
    """网络重试装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_manager = NetworkRetryManager(max_retries, base_delay)
            return retry_manager.retry_with_backoff(func, *args, **kwargs)
        return wrapper
    return decorator


def test_network_environment():
    """测试网络环境"""
    test_urls = [
        'https://www.baidu.com',
        'https://cn.bing.com',
        'https://finance.sina.com.cn'
    ]

    logger.info("测试网络环境...")
    working_urls = []

    for url in test_urls:
        if data_source_manager.test_network_connectivity(url):
            working_urls.append(url)
            logger.info(f"✓ {url} 可访问")
        else:
            logger.warning(f"✗ {url} 不可访问")

    if working_urls:
        logger.info(f"网络环境正常，{len(working_urls)}个测试URL可访问")
        return True
    else:
        logger.error("网络环境异常，所有测试URL都不可访问")
        return False


if __name__ == "__main__":
    # 测试网络环境
    test_network_environment()