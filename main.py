#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票分析API启动文件
Stock Analysis API Entry Point
"""

import uvicorn
import os
import sys

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.core.app import app

def main():
    """主函数"""
    print("=" * 60)
    print("股票分析API服务 - Stock Analysis API Service")
    print("版本: 2.0")
    print("=" * 60)
    print("服务启动中...")

    # 启动服务
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8085,
        reload=True,
        log_level="info"
    )

if __name__ == "__main__":
    main()