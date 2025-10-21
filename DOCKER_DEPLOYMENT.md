# Docker部署指南
## Stock Analysis API - Docker Deployment Guide

---

## 📋 部署准备

### 系统要求
```
操作系统: Linux/Windows/MacOS
Docker版本: >= 20.10
Docker Compose版本: >= 2.0
内存: >= 2GB
磁盘空间: >= 5GB
```

### 文件清单
```
stock-analysis-api/
├── Dockerfile                  # Docker镜像构建文件
├── docker-compose.yml          # Docker Compose编排文件
├── .dockerignore              # Docker构建忽略文件
├── .env.example               # 环境变量示例
├── requirements.txt           # Python依赖
└── stock_analysis_api.py      # API服务代码
```

---

## 🚀 快速部署

### 第一步: 配置环境变量

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑配置（重要）
vi .env  # 或使用任何文本编辑器

# 必须修改的配置：
# VALID_TOKENS=your-secret-token-here
```

### 第二步: 构建并启动服务

```bash
# 构建镜像并启动服务
docker-compose up -d --build

# 查看服务状态
docker-compose ps

# 查看服务日志
docker-compose logs -f stock-analysis-api
```

### 第三步: 验证服务

```bash
# 方式1: 访问API文档
# 在浏览器打开: http://localhost:8085/docs

# 方式2: 使用curl测试
curl -X POST http://localhost:8085/analyze-stock/ \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-xykj-001" \
  -d '{
    "stock_code": "600271",
    "market_type": "A"
  }'

# 方式3: 健康检查
curl http://localhost:8085/docs
```

---

## 🔧 常用命令

### 服务管理

```bash
# 启动服务
docker-compose up -d

# 停止服务
docker-compose stop

# 重启服务
docker-compose restart

# 停止并删除容器
docker-compose down

# 停止并删除容器+volumes
docker-compose down -v
```

### 日志查看

```bash
# 实时查看日志
docker-compose logs -f

# 查看最近100行日志
docker-compose logs --tail=100

# 查看特定服务日志
docker-compose logs -f stock-analysis-api
```

### 进入容器

```bash
# 进入容器shell
docker-compose exec stock-analysis-api /bin/bash

# 执行Python命令
docker-compose exec stock-analysis-api python -c "print('Hello')"
```

### 更新服务

```bash
# 拉取最新代码后重新构建
docker-compose up -d --build

# 仅重新构建镜像
docker-compose build --no-cache
```

---

## 📊 性能优化

### 资源限制配置

在`docker-compose.yml`中已配置：

```yaml
deploy:
  resources:
    limits:
      cpus: '2'      # 最多使用2个CPU核心
      memory: 2G     # 最多使用2GB内存
    reservations:
      cpus: '1'      # 保留1个CPU核心
      memory: 1G     # 保留1GB内存
```

### 调整资源限制

```yaml
# 编辑docker-compose.yml
# 根据服务器配置调整以下值：

# 高性能服务器
cpus: '4'
memory: 4G

# 低配置服务器
cpus: '1'
memory: 512M
```

---

## 🔒 安全配置

### 修改默认Token

```bash
# 编辑.env文件
VALID_TOKENS=your-super-secret-token-1,your-super-secret-token-2

# 重启服务使配置生效
docker-compose restart
```

### 限制访问IP

```yaml
# 在docker-compose.yml中添加
ports:
  - "127.0.0.1:8085:8085"  # 仅本地访问

# 或使用防火墙
iptables -A INPUT -p tcp --dport 8085 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 8085 -j DROP
```

### 启用HTTPS（使用Nginx反向代理）

```bash
# 安装nginx
apt-get install nginx

# 配置nginx
vi /etc/nginx/sites-available/stock-api
```

Nginx配置示例：

```nginx
server {
    listen 443 ssl http2;
    server_name api.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://localhost:8085;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## 📁 持久化数据

### 目录结构

```
.
├── logs/                  # 日志文件目录
│   └── stock_analysis.log
├── analysis_results/      # 分析结果保存目录
│   └── analysis_*.json
└── data/                  # 数据库目录（可选）
    └── stock_analysis.db
```

### 备份数据

```bash
# 备份日志
tar -czf logs-backup-$(date +%Y%m%d).tar.gz ./logs

# 备份分析结果
tar -czf results-backup-$(date +%Y%m%d).tar.gz ./analysis_results

# 完整备份
docker-compose exec stock-analysis-api tar -czf /app/backup.tar.gz /app/logs /app/analysis_results
docker cp stock-analysis-api:/app/backup.tar.gz ./backup-$(date +%Y%m%d).tar.gz
```

---

## 🐛 故障排查

### 问题1: 容器无法启动

```bash
# 查看详细日志
docker-compose logs stock-analysis-api

# 检查端口是否被占用
netstat -tuln | grep 8085

# 检查磁盘空间
df -h
```

### 问题2: API响应缓慢

```bash
# 检查容器资源使用
docker stats stock-analysis-api

# 增加内存限制
# 编辑docker-compose.yml，增加memory值

# 查看进程
docker-compose exec stock-analysis-api ps aux
```

### 问题3: 数据获取失败

```bash
# 进入容器测试网络
docker-compose exec stock-analysis-api ping -c 3 baidu.com

# 测试akshare
docker-compose exec stock-analysis-api python -c "import akshare as ak; print(ak.stock_zh_a_hist('600271'))"

# 检查DNS
docker-compose exec stock-analysis-api cat /etc/resolv.conf
```

### 问题4: Token认证失败

```bash
# 检查环境变量
docker-compose exec stock-analysis-api env | grep TOKEN

# 查看当前有效Token
docker-compose exec stock-analysis-api python -c "
from stock_analysis_api import valid_tokens
print(valid_tokens)
"
```

---

## 📈 监控与维护

### 健康检查

```bash
# 查看健康状态
docker inspect --format='{{.State.Health.Status}}' stock-analysis-api

# 手动触发健康检查
docker-compose exec stock-analysis-api curl -f http://localhost:8085/docs || exit 1
```

### 日志轮转

```yaml
# 已在docker-compose.yml中配置
logging:
  driver: "json-file"
  options:
    max-size: "10m"    # 单个日志文件最大10MB
    max-file: "3"      # 保留3个日志文件
```

### 自动重启

```yaml
# 已在docker-compose.yml中配置
restart: unless-stopped  # 除非手动停止，否则自动重启
```

---

## 🔄 升级部署

### 滚动更新

```bash
# 1. 备份当前版本
docker-compose exec stock-analysis-api tar -czf /app/backup.tar.gz /app

# 2. 拉取新代码
git pull origin main

# 3. 构建新镜像
docker-compose build --no-cache

# 4. 启动新版本
docker-compose up -d

# 5. 验证服务
curl http://localhost:8085/docs
```

### 回滚操作

```bash
# 1. 停止当前容器
docker-compose stop

# 2. 恢复旧版本代码
git checkout <previous-commit>

# 3. 重新构建
docker-compose up -d --build
```

---

## 🌐 生产环境部署建议

### 高可用配置

1. **使用负载均衡**
```yaml
# 启动多个实例
docker-compose up -d --scale stock-analysis-api=3

# 配置Nginx负载均衡
upstream stock_api {
    server 127.0.0.1:8085;
    server 127.0.0.1:8086;
    server 127.0.0.1:8087;
}
```

2. **数据库分离**
```yaml
# 使用外部MySQL/PostgreSQL
environment:
  - DB_TYPE=mysql
  - MYSQL_HOST=db.example.com
  - MYSQL_PORT=3306
```

3. **缓存层**
```yaml
# 添加Redis缓存
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"

  stock-analysis-api:
    environment:
      - REDIS_HOST=redis
      - REDIS_PORT=6379
```

### 监控接入

```yaml
# 添加Prometheus监控
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
```

---

## 📞 技术支持

- **文档**: 查看 `UPGRADE_DOCUMENTATION.md`
- **快速开始**: 查看 `QUICK_START.md`
- **问题反馈**: 提交GitHub Issue

---

**部署版本**: 2.0
**更新时间**: 2025-10-20
**部署状态**: ✅ 生产就绪
