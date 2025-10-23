"""
缓存管理模块 - Redis集中式缓存支持
Cache Management Module - Centralized Redis Cache Support

版本: 1.0
作者: Stock Analysis Team
创建日期: 2025-10-22

主要功能:
    1. Redis连接管理
    2. 缓存读取和写入操作
    3. 不同类型数据的TTL管理
    4. 缓存键命名规范
    5. 异步缓存操作支持
"""

import json
import asyncio
import os
from datetime import datetime, timedelta
from typing import Optional, Any, Union, Dict
import redis.asyncio as redis
from redis.asyncio import Redis
import structlog

logger = structlog.get_logger(__name__)

class CacheManager:
    """Redis缓存管理器"""

    def __init__(self,
                 redis_url: str = None,
                 default_ttl: int = 300,
                 max_connections: int = 20):
        """
        初始化缓存管理器

        Args:
            redis_url: Redis连接URL (默认从环境变量读取)
            default_ttl: 默认TTL(秒)
            max_connections: 最大连接数
        """
        # 如果未指定redis_url,从环境变量构建
        if redis_url is None:
            redis_host = os.getenv('REDIS_HOST', 'redis')  # Docker环境默认使用服务名
            redis_port = os.getenv('REDIS_PORT', '6379')   # 内部端口始终是6379
            redis_db = os.getenv('REDIS_DB', '0')
            redis_password = os.getenv('REDIS_PASSWORD', '')

            if redis_password:
                redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
            else:
                redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

            logger.info("从环境变量构建Redis连接",
                       host=redis_host,
                       port=redis_port,
                       db=redis_db,
                       has_password=bool(redis_password))

        self.redis_url = redis_url
        self.default_ttl = default_ttl
        self.max_connections = max_connections
        self._redis_client: Optional[Redis] = None
        self._connection_pool = None

        # 不同类型数据的TTL配置
        self.ttl_config = {
            'daily_data': 24 * 3600,      # 日K线数据缓存24小时
            'minute_data': 5 * 60,        # 分钟级数据缓存5分钟
            'real_time_quote': 30,        # 实时行情缓存30秒
            'market_status': 60 * 60,     # 市场状态缓存1小时
            'source_status': 10 * 60,     # 数据源状态缓存10分钟
            'circuit_breaker': 60 * 60,   # 熔断器状态缓存1小时
        }

    async def connect(self) -> bool:
        """建立Redis连接"""
        try:
            if self._redis_client is None:
                # 创建连接池
                self._connection_pool = redis.ConnectionPool.from_url(
                    self.redis_url,
                    max_connections=self.max_connections,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                    socket_keepalive_options={}
                )

                # 创建Redis客户端
                self._redis_client = Redis(connection_pool=self._connection_pool)

                # 测试连接
                await self._redis_client.ping()
                logger.info("Redis连接建立成功", redis_url=self.redis_url)
                return True
            return True
        except Exception as e:
            logger.error("Redis连接失败", error=str(e), redis_url=self.redis_url)
            return False

    async def disconnect(self):
        """关闭Redis连接"""
        try:
            if self._redis_client:
                await self._redis_client.close()
                self._redis_client = None
            if self._connection_pool:
                await self._connection_pool.disconnect()
                self._connection_pool = None
            logger.info("Redis连接已关闭")
        except Exception as e:
            logger.error("关闭Redis连接失败", error=str(e))

    @property
    def client(self) -> Redis:
        """获取Redis客户端"""
        if self._redis_client is None:
            raise RuntimeError("Redis客户端未初始化，请先调用connect()方法")
        return self._redis_client

    def _generate_cache_key(self, prefix: str, **kwargs) -> str:
        """生成缓存键"""
        parts = [prefix]
        for key, value in sorted(kwargs.items()):
            parts.append(f"{key}:{value}")
        return ":".join(parts)

    async def get(self, key: str) -> Optional[Any]:
        """获取缓存数据"""
        try:
            value = await self.client.get(key)
            if value is None:
                logger.debug("缓存未命中", key=key)
                return None

            # 尝试解析JSON
            try:
                result = json.loads(value)
                logger.debug("缓存命中", key=key, data_type=type(result).__name__)
                return result
            except (json.JSONDecodeError, TypeError):
                # 如果不是JSON格式，直接返回原始值
                logger.debug("缓存命中(原始值)", key=key)
                return value.decode('utf-8') if isinstance(value, bytes) else value

        except Exception as e:
            logger.error("获取缓存失败", key=key, error=str(e))
            return None

    async def set(self,
                  key: str,
                  value: Any,
                  ttl: Optional[int] = None,
                  data_type: Optional[str] = None) -> bool:
        """设置缓存数据"""
        try:
            # 确定TTL
            if ttl is None and data_type:
                ttl = self.ttl_config.get(data_type, self.default_ttl)
            elif ttl is None:
                ttl = self.default_ttl

            # 序列化值
            if isinstance(value, (dict, list, tuple)):
                serialized_value = json.dumps(value, ensure_ascii=False, default=str)
            elif isinstance(value, (str, int, float, bool)):
                serialized_value = json.dumps(value)
            else:
                serialized_value = str(value)

            # 设置缓存
            result = await self.client.setex(key, ttl, serialized_value)

            if result:
                logger.debug("缓存设置成功", key=key, ttl=ttl, data_type=data_type)
                return True
            else:
                logger.warning("缓存设置失败", key=key)
                return False

        except Exception as e:
            logger.error("设置缓存失败", key=key, error=str(e))
            return False

    async def delete(self, key: str) -> bool:
        """删除缓存"""
        try:
            result = await self.client.delete(key)
            if result:
                logger.debug("缓存删除成功", key=key)
                return True
            else:
                logger.debug("缓存不存在或删除失败", key=key)
                return False
        except Exception as e:
            logger.error("删除缓存失败", key=key, error=str(e))
            return False

    async def exists(self, key: str) -> bool:
        """检查缓存是否存在"""
        try:
            result = await self.client.exists(key)
            return bool(result)
        except Exception as e:
            logger.error("检查缓存存在性失败", key=key, error=str(e))
            return False

    async def expire(self, key: str, ttl: int) -> bool:
        """设置缓存过期时间"""
        try:
            result = await self.client.expire(key, ttl)
            if result:
                logger.debug("缓存TTL设置成功", key=key, ttl=ttl)
                return True
            else:
                logger.warning("缓存TTL设置失败或缓存不存在", key=key)
                return False
        except Exception as e:
            logger.error("设置缓存TTL失败", key=key, ttl=ttl, error=str(e))
            return False

    async def ttl(self, key: str) -> int:
        """获取缓存剩余TTL"""
        try:
            result = await self.client.ttl(key)
            return result
        except Exception as e:
            logger.error("获取缓存TTL失败", key=key, error=str(e))
            return -1

    # ===== 股票数据专用缓存方法 =====

    async def get_stock_data(self,
                            stock_code: str,
                            market_type: str,
                            start_date: str,
                            end_date: str) -> Optional[Dict[str, Any]]:
        """获取股票数据缓存"""
        key = self._generate_cache_key(
            "stock_data",
            market=market_type,
            code=stock_code,
            start=start_date,
            end=end_date
        )
        return await self.get(key)

    async def set_stock_data(self,
                            stock_code: str,
                            market_type: str,
                            start_date: str,
                            end_date: str,
                            data: Dict[str, Any]) -> bool:
        """设置股票数据缓存"""
        key = self._generate_cache_key(
            "stock_data",
            market=market_type,
            code=stock_code,
            start=start_date,
            end=end_date
        )
        return await self.set(key, data, data_type='daily_data')

    async def get_real_time_quote(self, stock_code: str, market_type: str) -> Optional[Dict[str, Any]]:
        """获取实时行情缓存"""
        key = self._generate_cache_key(
            "real_time_quote",
            market=market_type,
            code=stock_code
        )
        return await self.get(key)

    async def set_real_time_quote(self,
                                 stock_code: str,
                                 market_type: str,
                                 quote_data: Dict[str, Any]) -> bool:
        """设置实时行情缓存"""
        key = self._generate_cache_key(
            "real_time_quote",
            market=market_type,
            code=stock_code
        )
        return await self.set(key, quote_data, data_type='real_time_quote')

    # ===== 熔断器状态缓存方法 =====

    async def get_circuit_breaker_status(self, source_name: str) -> Optional[Dict[str, Any]]:
        """获取熔断器状态"""
        key = self._generate_cache_key("circuit_breaker", source=source_name)
        return await self.get(key)

    async def set_circuit_breaker_status(self,
                                        source_name: str,
                                        status: Dict[str, Any]) -> bool:
        """设置熔断器状态"""
        key = self._generate_cache_key("circuit_breaker", source=source_name)
        return await self.set(key, status, data_type='circuit_breaker')

    async def delete_circuit_breaker_status(self, source_name: str) -> bool:
        """删除熔断器状态"""
        key = self._generate_cache_key("circuit_breaker", source=source_name)
        return await self.delete(key)

    # ===== 数据源状态缓存方法 =====

    async def get_source_status(self, source_name: str) -> Optional[Dict[str, Any]]:
        """获取数据源状态"""
        key = self._generate_cache_key("source_status", source=source_name)
        return await self.get(key)

    async def set_source_status(self,
                               source_name: str,
                               status: Dict[str, Any]) -> bool:
        """设置数据源状态"""
        key = self._generate_cache_key("source_status", source=source_name)
        return await self.set(key, status, data_type='source_status')

    # ===== 批量操作方法 =====

    async def mget(self, keys: list[str]) -> Dict[str, Any]:
        """批量获取缓存"""
        try:
            values = await self.client.mget(keys)
            result = {}
            for key, value in zip(keys, values):
                if value is not None:
                    try:
                        result[key] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        result[key] = value.decode('utf-8') if isinstance(value, bytes) else value
                else:
                    result[key] = None
            return result
        except Exception as e:
            logger.error("批量获取缓存失败", keys=keys, error=str(e))
            return {}

    async def mset(self, mapping: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """批量设置缓存"""
        try:
            # 序列化所有值
            serialized_mapping = {}
            for key, value in mapping.items():
                if isinstance(value, (dict, list, tuple)):
                    serialized_mapping[key] = json.dumps(value, ensure_ascii=False, default=str)
                else:
                    serialized_mapping[key] = json.dumps(value)

            # 使用pipeline提高性能
            pipe = self.client.pipeline()
            pipe.mset(serialized_mapping)

            # 如果指定了TTL，为每个键设置过期时间
            if ttl:
                for key in mapping.keys():
                    pipe.expire(key, ttl)

            await pipe.execute()
            logger.debug("批量设置缓存成功", keys=list(mapping.keys()), ttl=ttl)
            return True

        except Exception as e:
            logger.error("批量设置缓存失败", keys=list(mapping.keys()), error=str(e))
            return False

    # ===== 缓存统计和管理方法 =====

    async def get_cache_info(self) -> Dict[str, Any]:
        """获取缓存信息统计"""
        try:
            info = await self.client.info()
            return {
                'used_memory': info.get('used_memory_human', 'N/A'),
                'connected_clients': info.get('connected_clients', 'N/A'),
                'total_commands_processed': info.get('total_commands_processed', 'N/A'),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'hit_rate': (
                    info.get('keyspace_hits', 0) /
                    max(info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1)
                ) * 100
            }
        except Exception as e:
            logger.error("获取缓存信息失败", error=str(e))
            return {}

    async def clear_pattern(self, pattern: str) -> int:
        """按模式清除缓存"""
        try:
            keys = await self.client.keys(pattern)
            if keys:
                count = await self.client.delete(*keys)
                logger.info("按模式清除缓存成功", pattern=pattern, count=count)
                return count
            return 0
        except Exception as e:
            logger.error("按模式清除缓存失败", pattern=pattern, error=str(e))
            return 0

    async def health_check(self) -> Dict[str, Any]:
        """缓存健康检查"""
        try:
            start_time = datetime.now()
            await self.client.ping()
            end_time = datetime.now()

            response_time = (end_time - start_time).total_seconds() * 1000

            return {
                'status': 'healthy',
                'response_time_ms': round(response_time, 2),
                'timestamp': end_time.isoformat()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }


# 全局缓存管理器实例
cache_manager = CacheManager()

async def get_cache_manager() -> CacheManager:
    """获取缓存管理器实例"""
    await cache_manager.connect()
    return cache_manager

# 上下文管理器支持
class CacheContext:
    """缓存上下文管理器"""

    async def __aenter__(self) -> CacheManager:
        await cache_manager.connect()
        return cache_manager

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await cache_manager.disconnect()

# 使用示例:
# async with CacheContext() as cache:
#     await cache.set("test", {"data": "value"})
#     result = await cache.get("test")