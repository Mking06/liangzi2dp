"""Liangzidp - OpenAI/Anthropic 兼容 API 网关 for Liangzidp Web

启动方式:
  uv run granian --interface asgi --host 0.0.0.0 --port 8000 app.main:app

需要 curl-cffi 来模拟浏览器 TLS 指纹绕过 Cloudflare WAF。
POW 验证需要从浏览器获取 X-Ds-Pow-Response header 值。
"""
