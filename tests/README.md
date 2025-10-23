# 测试文档
# Test Documentation

## 概述

本项目包含全面的单元测试和集成测试，覆盖了系统的核心功能模块。

## 测试结构

```
tests/
├── __init__.py                    # 测试套件初始化
├── test_cache_manager.py          # 缓存管理器测试
├── test_circuit_breaker.py        # 熔断器测试
└── test_async_fetcher.py          # 异步数据获取器测试
```

## 测试覆盖范围

### 1. 缓存管理器测试 (test_cache_manager.py)

#### 单元测试
- ✅ Redis连接管理
- ✅ 基本操作 (GET/SET/DELETE/EXISTS)
- ✅ 缓存命中和未命中场景
- ✅ TTL管理
- ✅ 批量操作 (MGET/MSET)
- ✅ 股票数据专用缓存方法
- ✅ 实时行情缓存
- ✅ 错误处理

#### 集成测试
- ✅ 真实Redis工作流程
- ✅ 缓存过期机制

**测试类:**
- `TestCacheManager` - 基础功能测试
- `TestCacheManagerErrorHandling` - 错误处理测试
- `TestCacheIntegration` - 集成测试 (需要Redis)

### 2. 熔断器测试 (test_circuit_breaker.py)

#### 单元测试
- ✅ 熔断器初始化
- ✅ 初始状态(关闭)
- ✅ 记录成功/失败请求
- ✅ 连续失败触发熔断 (CLOSED → OPEN)
- ✅ 熔断状态检查
- ✅ 超时自动转换为半开放 (OPEN → HALF_OPEN)
- ✅ 半开放状态下恢复 (HALF_OPEN → CLOSED)
- ✅ 半开放状态下再次失败 (HALF_OPEN → OPEN)
- ✅ 统计信息获取
- ✅ 熔断器重置

#### 状态转换测试
- ✅ 完整状态转换周期
- ✅ 多数据源独立管理

#### 集成测试
- ✅ 真实Redis环境下的熔断器工作流程

**测试类:**
- `TestCircuitBreakerManager` - 基础功能测试
- `TestCircuitBreakerStateTransitions` - 状态转换测试
- `TestCircuitBreakerIntegration` - 集成测试 (需要Redis)

### 3. 异步数据获取器测试 (test_async_fetcher.py)

#### 基础测试
- ✅ 数据获取器初始化
- ✅ aiohttp session创建和关闭
- ✅ 请求间隔控制
- ✅ A股数据获取 (Mock)
- ✅ 数据获取失败处理
- ✅ 实时数据转历史格式转换

#### 并发数据获取测试
- ✅ 单个数据源获取成功
- ✅ 单个数据源获取失败
- ✅ 并发获取采纳最快响应 ⚠️
- ✅ 所有数据源失败场景

#### 缓存集成测试
- ✅ 缓存命中场景
- ✅ 缓存未命中后设置缓存

#### 熔断器集成测试
- ✅ 熔断器阻止访问数据源
- ✅ 熔断器记录成功

#### 上下文管理器测试
- ✅ 生命周期管理
- ✅ 带缓存的上下文管理器 ⚠️

**测试类:**
- `TestAsyncStockDataFetcher` - 基础功能测试
- `TestConcurrentFetching` - 并发获取测试
- `TestCacheIntegration` - 缓存集成测试
- `TestCircuitBreakerIntegration` - 熔断器集成测试
- `TestContextManager` - 上下文管理器测试

## 测试统计

### 当前测试结果

```
异步数据获取器测试:
- ✅ 通过: 13个
- ❌ 失败: 2个
- ⚠️ 错误: 1个
- 总计: 16个测试

通过率: 81.25%
```

### 失败的测试 (待修复)

1. **test_concurrent_fetch_fastest_wins** - Mock协程对象处理问题
2. **test_context_manager_with_cache** - 模块属性引用问题
3. **test_circuit_breaker_records_success** - Fixture问题

## 运行测试

### 运行所有测试

```bash
pytest tests/ -v
```

### 运行特定测试文件

```bash
# 缓存管理器测试
pytest tests/test_cache_manager.py -v

# 熔断器测试
pytest tests/test_circuit_breaker.py -v

# 异步数据获取器测试
pytest tests/test_async_fetcher.py -v
```

### 按标记运行测试

```bash
# 只运行单元测试
pytest -m unit

# 只运行集成测试
pytest -m integration

# 只运行缓存相关测试
pytest -m cache

# 只运行熔断器测试
pytest -m circuit_breaker

# 只运行异步测试
pytest -m asyncio
```

### 运行特定测试类

```bash
pytest tests/test_async_fetcher.py::TestAsyncStockDataFetcher -v
```

### 运行特定测试方法

```bash
pytest tests/test_async_fetcher.py::TestAsyncStockDataFetcher::test_fetcher_initialization -v
```

## 测试标记说明

- `@pytest.mark.unit` - 单元测试
- `@pytest.mark.integration` - 集成测试
- `@pytest.mark.cache` - 缓存相关测试
- `@pytest.mark.circuit_breaker` - 熔断器相关测试
- `@pytest.mark.asyncio` - 异步测试
- `@pytest.mark.redis` - 需要Redis的测试
- `@pytest.mark.network` - 需要网络的测试
- `@pytest.mark.slow` - 慢速测试

## Mock使用示例

### Mock akshare调用

```python
def test_fetch_zh_a_hist_async_mock(self, fetcher, sample_stock_data, mocker):
    # Mock akshare调用
    mock_ak = mocker.patch('akshare.stock_zh_a_hist')
    mock_ak.return_value = sample_stock_data

    result = await fetcher._fetch_zh_a_hist_async(
        code="600271",
        start_date="20240101",
        end_date="20240110"
    )

    assert not result.empty
    mock_ak.assert_called_once()
```

### Mock Redis客户端

```python
@pytest.fixture
async def mock_redis(self):
    mock_redis = AsyncMock()
    mock_redis.ping.return_value = True
    mock_redis.get.return_value = None
    mock_redis.setex.return_value = True
    return mock_redis
```

## 测试覆盖率

要生成测试覆盖率报告:

```bash
pytest --cov=src --cov-report=html --cov-report=term-missing
```

覆盖率报告将生成在 `htmlcov/index.html`

## 集成测试要求

某些集成测试需要真实的Redis实例:

```bash
# 使用Docker启动Redis
docker run -d -p 6379:6379 redis:latest

# 或使用docker-compose
docker-compose up -d redis
```

## 持续集成

测试可以集成到CI/CD流程中:

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      redis:
        image: redis
        ports:
          - 6379:6379

    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-asyncio pytest-mock pytest-cov

      - name: Run tests
        run: pytest tests/ -v --cov=src
```

## 最佳实践

1. **使用Fixtures** - 复用测试数据和Mock对象
2. **使用pytest-mock** - Mock外部依赖
3. **异步测试** - 使用 `@pytest.mark.asyncio` 标记
4. **测试隔离** - 每个测试应该独立运行
5. **清理资源** - 在fixture中正确清理资源
6. **命名规范** - 测试函数以 `test_` 开头
7. **文档化** - 在测试函数中添加docstring说明测试目的

## 下一步改进

1. 提高测试覆盖率到90%以上
2. 修复失败的测试用例
3. 添加性能测试
4. 添加端到端测试
5. 集成到CI/CD流程
6. 添加测试覆盖率徽章

## 参考资料

- [pytest文档](https://docs.pytest.org/)
- [pytest-asyncio](https://pytest-asyncio.readthedocs.io/)
- [pytest-mock](https://pytest-mock.readthedocs.io/)
