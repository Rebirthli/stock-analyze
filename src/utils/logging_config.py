"""
结构化日志配置模块
Structured Logging Configuration Module

版本: 1.0
作者: Stock Analysis Team
创建日期: 2025-10-22

主要功能:
    1. 配置structlog输出JSON格式日志
    2. 设定基础处理器和格式化器
    3. 集成请求追踪和上下文信息
    4. 支持不同环境的日志配置
    5. 性能监控和错误追踪
"""

import sys
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
import structlog
from structlog.stdlib import LoggerFactory
from structlog.processors import JSONRenderer, TimeStamper, add_log_level, StackInfoRenderer

def configure_structured_logging(
    log_level: str = "INFO",
    json_logs: bool = True,
    service_name: str = "stock-analysis-api",
    environment: str = "development"
) -> None:
    """
    配置结构化日志

    Args:
        log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: 是否输出JSON格式日志
        service_name: 服务名称
        environment: 环境名称 (development, staging, production)
    """

    # 配置标准库logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper())
    )

    # 配置structlog处理器链
    processors = [
        # 添加时间戳
        TimeStamper(fmt="iso", key="timestamp"),

        # 添加日志级别
        add_log_level,

        # 添加堆栈信息（仅在DEBUG模式下）
        StackInfoRenderer() if log_level.upper() == "DEBUG" else None,

        # 添加服务信息
        structlog.processors.CallsiteParameterAdder(
            parameters=[structlog.processors.CallsiteParameter.FUNC_NAME,
                       structlog.processors.CallsiteParameter.LINENO]
        ),
    ]

    # 移除None值
    processors = [p for p in processors if p is not None]

    if json_logs:
        # JSON格式输出
        processors.extend([
            # 添加环境和服务信息
            structlog.processors.dict_tracebacks,
            lambda _, __, event_dict: {
                **event_dict,
                "service": service_name,
                "environment": environment,
                "version": "1.0.0"
            },
            JSONRenderer(serializer=json.dumps, ensure_ascii=False)
        ])
    else:
        # 可读格式输出
        processors.extend([
            structlog.dev.ConsoleRenderer(colors=True)
        ])

    # 配置structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )

def get_logger(name: Optional[str] = None, **context) -> structlog.stdlib.BoundLogger:
    """
    获取带上下文的日志记录器

    Args:
        name: 日志记录器名称
        **context: 额外的上下文信息

    Returns:
        绑定上下文的日志记录器
    """
    logger = structlog.get_logger(name)
    if context:
        logger = logger.bind(**context)
    return logger

class RequestLogger:
    """请求日志记录器"""

    def __init__(self, request_id: Optional[str] = None):
        """
        初始化请求日志记录器

        Args:
            request_id: 请求ID，用于追踪
        """
        self.request_id = request_id or self._generate_request_id()
        self.logger = get_logger(
            "request",
            request_id=self.request_id
        )

    @staticmethod
    def _generate_request_id() -> str:
        """生成请求ID"""
        return f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(datetime.now())}"

    def log_request_start(self,
                         method: str,
                         path: str,
                         **kwargs) -> None:
        """记录请求开始"""
        self.logger.info(
            "request_started",
            method=method,
            path=path,
            **kwargs
        )

    def log_request_success(self,
                           status_code: int,
                           duration_ms: float,
                           **kwargs) -> None:
        """记录请求成功"""
        self.logger.info(
            "request_completed",
            status_code=status_code,
            duration_ms=round(duration_ms, 2),
            **kwargs
        )

    def log_request_error(self,
                         error: Exception,
                         status_code: Optional[int] = None,
                         **kwargs) -> None:
        """记录请求错误"""
        self.logger.error(
            "request_failed",
            error_type=type(error).__name__,
            error_message=str(error),
            status_code=status_code,
            **kwargs,
            exc_info=True
        )

class PerformanceLogger:
    """性能日志记录器"""

    def __init__(self, component: str):
        """
        初始化性能日志记录器

        Args:
            component: 组件名称
        """
        self.logger = get_logger("performance", component=component)

    def log_execution_time(self,
                          operation: str,
                          duration_ms: float,
                          **kwargs) -> None:
        """记录执行时间"""
        self.logger.info(
            "execution_time",
            operation=operation,
            duration_ms=round(duration_ms, 2),
            **kwargs
        )

    def log_cache_hit(self,
                     cache_key: str,
                     **kwargs) -> None:
        """记录缓存命中"""
        self.logger.info(
            "cache_hit",
            cache_key=cache_key,
            **kwargs
        )

    def log_cache_miss(self,
                      cache_key: str,
                      **kwargs) -> None:
        """记录缓存未命中"""
        self.logger.info(
            "cache_miss",
            cache_key=cache_key,
            **kwargs
        )

class BusinessLogger:
    """业务日志记录器"""

    def __init__(self, service: str):
        """
        初始化业务日志记录器

        Args:
            service: 服务名称
        """
        self.logger = get_logger("business", service=service)

    def log_stock_data_request(self,
                              stock_code: str,
                              market_type: str,
                              **kwargs) -> None:
        """记录股票数据请求"""
        self.logger.info(
            "stock_data_requested",
            stock_code=stock_code,
            market_type=market_type,
            **kwargs
        )

    def log_data_source_used(self,
                            source_name: str,
                            success: bool,
                            duration_ms: float,
                            **kwargs) -> None:
        """记录数据源使用情况"""
        self.logger.info(
            "data_source_used",
            source_name=source_name,
            success=success,
            duration_ms=round(duration_ms, 2),
            **kwargs
        )

    def log_circuit_breaker_event(self,
                                 source_name: str,
                                 event_type: str,
                                 state: str,
                                 **kwargs) -> None:
        """记录熔断器事件"""
        self.logger.warning(
            "circuit_breaker_event",
            source_name=source_name,
            event_type=event_type,
            state=state,
            **kwargs
        )

# 日志装饰器
def log_execution(logger: Optional[structlog.stdlib.BoundLogger] = None):
    """执行时间日志装饰器"""
    def decorator(func):
        if not logger:
            func_logger = get_logger(func.__module__, function=func.__name__)
        else:
            func_logger = logger

        async def async_wrapper(*args, **kwargs):
            start_time = datetime.now()
            try:
                result = await func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds() * 1000

                func_logger.info(
                    "function_executed",
                    function=func.__name__,
                    duration_ms=round(duration, 2),
                    success=True
                )
                return result
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds() * 1000

                func_logger.error(
                    "function_failed",
                    function=func.__name__,
                    duration_ms=round(duration, 2),
                    error=str(e),
                    error_type=type(e).__name__,
                    exc_info=True
                )
                raise

        def sync_wrapper(*args, **kwargs):
            start_time = datetime.now()
            try:
                result = func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds() * 1000

                func_logger.info(
                    "function_executed",
                    function=func.__name__,
                    duration_ms=round(duration, 2),
                    success=True
                )
                return result
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds() * 1000

                func_logger.error(
                    "function_failed",
                    function=func.__name__,
                    duration_ms=round(duration, 2),
                    error=str(e),
                    error_type=type(e).__name__,
                    exc_info=True
                )
                raise

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator

def log_api_call():
    """API调用日志装饰器"""
    def decorator(func):
        logger = get_logger("api", endpoint=func.__name__)

        async def async_wrapper(*args, **kwargs):
            start_time = datetime.now()

            # 尝试提取请求参数
            try:
                if args and hasattr(args[0], '__dict__'):
                    # 如果是类方法，尝试提取有用信息
                    request_info = {
                        'args_count': len(args),
                        'kwargs_keys': list(kwargs.keys())
                    }
                else:
                    request_info = {}
            except Exception:
                request_info = {}

            try:
                result = await func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds() * 1000

                logger.info(
                    "api_call_success",
                    function=func.__name__,
                    duration_ms=round(duration, 2),
                    request_info=request_info
                )
                return result
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds() * 1000

                logger.error(
                    "api_call_failed",
                    function=func.__name__,
                    duration_ms=round(duration, 2),
                    error=str(e),
                    error_type=type(e).__name__,
                    request_info=request_info,
                    exc_info=True
                )
                raise

        def sync_wrapper(*args, **kwargs):
            return asyncio.run(async_wrapper(*args, **kwargs))

        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator

# 默认配置
if __name__ == "__main__":
    # 配置日志
    configure_structured_logging(
        log_level="INFO",
        json_logs=True,
        service_name="stock-analysis-api",
        environment="development"
    )

    # 测试日志
    logger = get_logger("test")

    logger.info("测试信息日志", key="value")
    logger.warning("测试警告日志", warning_level="medium")
    logger.error("测试错误日志", error_code=500)

    # 测试不同类型的日志记录器
    req_logger = RequestLogger("test-request-123")
    req_logger.log_request_start("GET", "/api/stocks/600271")
    req_logger.log_request_success(200, 150.5)

    perf_logger = PerformanceLogger("data-fetcher")
    perf_logger.log_execution_time("fetch_stock_data", 250.75)

    biz_logger = BusinessLogger("stock-service")
    biz_logger.log_stock_data_request("600271", "A")
    biz_logger.log_data_source_used("akshare_api", True, 120.3)