import requests
import json
import pandas as pd
from datetime import datetime
from prettytable import PrettyTable

def call_stock_analysis_api(stock_code, market_type='A', start_date=None, end_date=None, api_url="http://localhost:8085", auth_token=None):
    """
    调用股票分析API接口

    参数:
        stock_code (str): 股票代码
        market_type (str): 市场类型，默认为'A'
        start_date (str): 开始日期，格式为'YYYYMMDD'，默认为None
        end_date (str): 结束日期，格式为'YYYYMMDD'，默认为None
        api_url (str): API服务器地址，默认为"http://localhost:8085"
        auth_token (str): 鉴权Token，默认为None

    返回:
        dict: API返回的结果
    """
    # 构建请求URL
    url = f"{api_url}/analyze-stock/"

    # 构建请求数据
    data = {
        "stock_code": stock_code,
        "market_type": market_type
    }

    if start_date:
        data["start_date"] = start_date
    if end_date:
        data["end_date"] = end_date

    # 构建请求头
    headers = {
        "Content-Type": "application/json"
    }

    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
    else:
        print("警告: 未提供鉴权Token，可能会导致请求失败")

    try:
        # 发送POST请求
        response = requests.post(url, json=data, headers=headers)

        # 检查响应状态
        if response.status_code == 200:
            return response.json()
        else:
            print(f"请求失败，状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            return None
    except Exception as e:
        print(f"请求异常: {str(e)}")
        return None

def display_technical_summary(technical_summary):
    """显示技术指标概要（升级版）"""
    print("\n" + "="*80)
    print("技术指标概要 (Technical Summary)".center(80))
    print("="*80)

    # 基础指标
    print(f"\n【趋势分析 Trend Analysis】")
    print(f"  趋势方向: {'上升 (Upward)' if technical_summary['trend'] == 'upward' else '下降 (Downward)'}")
    print(f"  趋势强度: {technical_summary.get('trend_strength', 'N/A')}")
    if technical_summary.get('adx_strength'):
        print(f"  ADX指标: {technical_summary['adx_strength']:.2f} ", end="")
        if technical_summary['adx_strength'] > 25:
            print("(强趋势 Strong Trend)")
        elif technical_summary['adx_strength'] > 20:
            print("(中等趋势 Moderate Trend)")
        else:
            print("(弱趋势/震荡 Weak/Ranging)")

    # 波动率和成交量
    print(f"\n【波动性与成交量 Volatility & Volume】")
    print(f"  波动率: {technical_summary['volatility']}")
    print(f"  成交量趋势: {'增加 (Increasing)' if technical_summary['volume_trend'] == 'increasing' else '减少 (Decreasing)'}")
    print(f"  OBV能量潮: {technical_summary.get('obv_trend', 'N/A')}")

    # 动量指标
    print(f"\n【动量指标 Momentum Indicators】")
    if technical_summary.get('rsi_level'):
        rsi = technical_summary['rsi_level']
        print(f"  RSI指标: {rsi:.2f} ", end="")
        if rsi > 70:
            print("(超买 Overbought)")
        elif rsi < 30:
            print("(超卖 Oversold)")
        else:
            print("(中性 Neutral)")

    print(f"  Stochastic信号: {technical_summary.get('stochastic_signal', 'N/A')}")

    if technical_summary.get('mfi_level'):
        mfi = technical_summary['mfi_level']
        print(f"  MFI资金流: {mfi:.2f} ", end="")
        if mfi > 80:
            print("(资金流入强 Strong Inflow)")
        elif mfi < 20:
            print("(资金流出强 Strong Outflow)")
        else:
            print("(中性 Neutral)")

    # 市场微观结构
    print(f"\n【市场微观结构 Market Microstructure】")
    print(f"  VWAP位置: {technical_summary.get('vwap_position', 'N/A')}")

def display_risk_summary(risk_summary):
    """显示风险调整指标摘要（新增）"""
    print("\n" + "="*80)
    print("风险调整指标 (Risk-Adjusted Metrics)".center(80))
    print("="*80)

    # 收益指标
    print(f"\n【收益指标 Return Metrics】")
    print(f"  年化收益率: {risk_summary['annual_return']:.2f}%")
    print(f"  年化波动率: {risk_summary['annual_volatility']:.2f}%")

    # 风险调整收益
    print(f"\n【风险调整收益 Risk-Adjusted Returns】")
    sharpe = risk_summary['sharpe_ratio']
    print(f"  Sharpe Ratio: {sharpe:.3f} ", end="")
    if sharpe > 2:
        print("(优秀 Excellent)")
    elif sharpe > 1:
        print("(良好 Good)")
    elif sharpe > 0:
        print("(一般 Fair)")
    else:
        print("(差 Poor)")

    sortino = risk_summary['sortino_ratio']
    print(f"  Sortino Ratio: {sortino:.3f} ", end="")
    if sortino > 2:
        print("(优秀 Excellent)")
    elif sortino > 1:
        print("(良好 Good)")
    else:
        print("(一般 Fair)")

    # 风险指标
    print(f"\n【风险指标 Risk Metrics】")
    max_dd = risk_summary['max_drawdown']
    print(f"  最大回撤: {max_dd:.2f}% ", end="")
    if abs(max_dd) < 10:
        print("(低风险 Low Risk)")
    elif abs(max_dd) < 20:
        print("(中等风险 Medium Risk)")
    else:
        print("(高风险 High Risk)")

    print(f"  Calmar Ratio: {risk_summary['calmar_ratio']:.3f}")
    print(f"  风险等级: {risk_summary['risk_level']}")

    # Alpha和Beta
    print(f"\n【因子分析 Factor Analysis】")
    alpha = risk_summary['alpha']
    print(f"  Alpha (超额收益): {alpha:.2f}% ", end="")
    if alpha > 5:
        print("(显著跑赢市场 Significantly Outperforming)")
    elif alpha > 0:
        print("(跑赢市场 Outperforming)")
    elif alpha > -5:
        print("(略低于市场 Slightly Underperforming)")
    else:
        print("(显著跑输市场 Significantly Underperforming)")

    beta = risk_summary['beta']
    print(f"  Beta (系统风险): {beta:.3f} ", end="")
    if beta > 1.2:
        print("(高波动 High Volatility)")
    elif beta > 0.8:
        print("(与市场同步 Market Aligned)")
    else:
        print("(低波动 Low Volatility)")

def display_recent_data(recent_data):
    """显示近期交易数据（升级版，包含新指标）"""
    print("\n" + "="*80)
    print("近14日交易数据 (Recent 14-Day Trading Data)".center(80))
    print("="*80)

    # 创建表格
    table = PrettyTable()

    # 设置表头（包含新指标）
    table.field_names = [
        "日期", "收盘", "VWAP", "RSI", "MACD",
        "ADX", "Stoch K", "MFI", "成交量"
    ]

    # 添加数据行
    for day in recent_data:
        date_str = day['date'].split('T')[0] if 'T' in day['date'] else day['date']

        table.add_row([
            date_str,
            f"{day['close']:.2f}" if day.get('close') else "N/A",
            f"{day['daily_vwap']:.2f}" if day.get('daily_vwap') else "N/A",
            f"{day['RSI']:.1f}" if day.get('RSI') else "N/A",
            f"{day['MACD']:.4f}" if day.get('MACD') else "N/A",
            f"{day['ADX']:.1f}" if day.get('ADX') else "N/A",
            f"{day['Stochastic_K']:.1f}" if day.get('Stochastic_K') else "N/A",
            f"{day['MFI']:.1f}" if day.get('MFI') else "N/A",
            f"{int(day['volume'])}" if day.get('volume') else "N/A"
        ])

    # 打印表格
    print(table)

def display_report(report):
    """显示分析报告（升级版）"""
    print("\n" + "="*80)
    print("股票分析报告 (Stock Analysis Report)".center(80))
    print("="*80)

    # 基本信息
    print(f"\n【基本信息 Basic Information】")
    print(f"  股票代码: {report['stock_code']}")
    print(f"  市场类型: {report['market_type']}")
    print(f"  分析时间: {report['analysis_date']}")

    # 评分与建议
    print(f"\n【评分与建议 Score & Recommendation】")
    score = report['score']
    print(f"  综合评分: {score}/100 ", end="")
    if score >= 80:
        print("⭐⭐⭐⭐⭐")
    elif score >= 65:
        print("⭐⭐⭐⭐")
    elif score >= 50:
        print("⭐⭐⭐")
    elif score >= 40:
        print("⭐⭐")
    else:
        print("⭐")

    print(f"  投资建议: {report['recommendation']}")
    print(f"  风险等级: {report['risk_level']}")

    # 价格信息
    print(f"\n【价格信息 Price Information】")
    print(f"  当前价格: {report['price']:.2f}")
    print(f"  价格变动: {report['price_change']:.2f}%")
    if report.get('vwap'):
        vwap_diff = (report['price'] - report['vwap']) / report['vwap'] * 100
        print(f"  VWAP: {report['vwap']:.2f} (偏离度: {vwap_diff:+.2f}%)")

    # 趋势分析
    print(f"\n【趋势分析 Trend Analysis】")
    print(f"  MA趋势: {report['ma_trend']}")
    if report.get('adx'):
        print(f"  ADX: {report['adx']:.2f}")
    print(f"  趋势质量: {report['trend_quality']}")

    # 动量指标
    print(f"\n【动量指标 Momentum Indicators】")
    if report.get('rsi'):
        print(f"  RSI: {report['rsi']:.2f}")
    if report.get('stochastic_k'):
        print(f"  Stochastic K: {report['stochastic_k']:.2f}")
    if report.get('williams_r'):
        print(f"  Williams %R: {report['williams_r']:.2f}")
    print(f"  MACD信号: {report['macd_signal']}")

    # 成交量分析
    print(f"\n【成交量分析 Volume Analysis】")
    print(f"  成交量状态: {report['volume_status']}")
    print(f"  OBV信号: {report['obv_signal']}")
    if report.get('mfi'):
        print(f"  MFI: {report['mfi']:.2f}")

    # 风险收益指标
    print(f"\n【风险收益指标 Risk-Return Metrics】")
    print(f"  Sharpe Ratio: {report['sharpe_ratio']:.3f}")
    print(f"  最大回撤: {report['max_drawdown_pct']:.2f}%")
    print(f"  Alpha: {report['alpha_pct']:.2f}%")
    print(f"  Beta: {report['beta']:.3f}")
    print(f"  年化收益: {report['annual_return_pct']:.2f}%")

def save_to_file(result, stock_code):
    """保存结果到文件"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"analysis_{stock_code}_{timestamp}.json"

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

    print(f"\n分析结果已保存到: {filename}")

def main():
    """主函数"""
    print("="*80)
    print("股票分析客户端 (Stock Analysis Client)".center(80))
    print("国际级技术指标分析系统 (International-Grade Technical Analysis System)".center(80))
    print("="*80)

    # 获取用户输入（可以取消注释使用交互模式）
    # stock_code = input("请输入股票代码: ")
    # market_type = input("请输入市场类型 [A/HK/US/ETF/LOF] (默认A): ") or 'A'
    # start_date = input("请输入开始日期 (YYYYMMDD) (可选): ")
    # end_date = input("请输入结束日期 (YYYYMMDD) (可选): ")
    # auth_token = input("请输入鉴权Token: ")

    # 测试配置
    stock_code = "600271"
    market_type = "A"
    start_date = ""
    end_date = ""
    auth_token = "sk-xykj-tykj-001"  # 替换为实际的Token

    # 调用API
    print(f"\n正在分析股票 {stock_code}...")
    result = call_stock_analysis_api(stock_code, market_type, start_date, end_date, auth_token=auth_token)

    if result:
        # 显示技术指标概要
        display_technical_summary(result["technical_summary"])

        # 显示风险调整指标（新增）
        if "risk_summary" in result:
            display_risk_summary(result["risk_summary"])

        # 显示近期交易数据
        display_recent_data(result["recent_data"])

        # 显示分析报告
        display_report(result["report"])

        # 保存结果到文件
        save_to_file(result, stock_code)

        print("\n" + "="*80)
        print("分析完成！Analysis Complete!".center(80))
        print("="*80)
    else:
        print("获取股票分析数据失败")

if __name__ == "__main__":
    main()
