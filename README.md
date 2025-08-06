# Yahoo Finance 新闻爬虫 - GitHub Actions自动化

自动化Yahoo Finance新闻爬取系统，支持多URL爬取、智能去重和Supabase数据库存储。

## 🚀 功能特点

- **多URL爬取**: 同时爬取Yahoo Finance的最新新闻、一般新闻和科技新闻
- **智能去重**: 基于URL和标题的精确匹配去重，避免重复存储
- **完整内容提取**: 提取文章完整内容，包括隐藏的read-more部分
- **并发处理**: 5线程并发获取文章详细内容
- **数据库存储**: 自动存储到Supabase数据库
- **GitHub Actions自动化**: 每小时自动运行

## 🛠 技术栈

- **Python 3.11**: 主要编程语言
- **Crawl4AI**: 现代网页爬取，支持动态内容和虚拟滚动
- **BeautifulSoup4**: HTML解析和内容提取
- **Supabase**: 云数据库存储
- **GitHub Actions**: 自动化运行

## 📋 爬取流程

1. **阶段1: 获取文章列表** - 从3个Yahoo Finance URL爬取基础信息
2. **阶段2: 数据库查重检查** - 与Supabase中现有文章进行去重
3. **阶段3: 获取新文章详细内容** - 5线程并发获取完整文章内容
4. **阶段4: 保存到数据库** - 批量插入新文章到Supabase

## ⚙️ GitHub Actions设置

### 1. 配置Supabase密钥

在GitHub仓库的 **Settings → Secrets and variables → Actions** 中添加以下Repository secrets：

| 变量名 | 说明 | 示例值 |
|--------|------|--------|
| `SUPABASE_URL` | Supabase项目URL | `https://your-project.supabase.co` |
| `SUPABASE_ANON_KEY` | Supabase密钥（使用Service Role密钥） | `eyJhbGci...` |
| `SUPABASE_TABLE_NAME` | 表名（可选） | `news_items` |

⚠️ **重要**: 虽然变量名是 `SUPABASE_ANON_KEY`，但请使用 `service_role` 密钥以获得数据库读写权限。

### 2. 工作流配置

工作流文件：`.github/workflows/crawler.yml`

- **运行频率**: 每小时一次 (`cron: '0 * * * *'`)
- **手动触发**: 支持手动运行和参数配置
- **环境**: Ubuntu Latest + Python 3.11
- **浏览器**: 自动安装Chromium

### 3. 支持的参数

手动运行时可配置：
- `max_hours`: 爬取多少小时内的新闻（默认2小时）

## 📊 数据源

- `https://finance.yahoo.com/topic/latest-news/` - 最新新闻
- `https://finance.yahoo.com/news/` - 一般财经新闻  
- `https://finance.yahoo.com/topic/tech/` - 科技新闻

## 🗃 数据库结构

Supabase `news_items` 表字段：
- `id`: UUID主键
- `title`: 文章标题
- `url`: 文章链接
- `content`: 文章完整内容
- `published_at`: 发布时间（ISO格式）
- `source`: 数据来源（Yahoo Finance）
- `created_at`: 创建时间
- 其他字段...

## 🔧 本地开发

### 安装依赖
```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 配置环境变量
```bash
# 复制环境变量模板
cp .env.example .env

# 编辑 .env 文件，填入真实的Supabase配置
# SUPABASE_URL=https://your-project.supabase.co
# SUPABASE_ANON_KEY=your_service_role_key
# SUPABASE_TABLE_NAME=news_items
```

### 运行爬虫
```bash
python yahoo_news_crawler.py
```

本地运行时会额外保存JSON和CSV文件作为备份。

## 📝 日志监控

在GitHub Actions中可查看详细运行日志：
- Actions → Yahoo Finance News Crawler → 选择运行记录

日志包含：
- 系统资源信息
- 爬取进度详情
- 数据库操作结果
- 错误处理信息

## ⚡ 性能优化

- **并发控制**: 5个线程同时处理文章内容获取
- **超时保护**: 单篇文章最多45秒超时
- **智能回退**: Crawl4AI失败时自动使用requests备用方法
- **内存优化**: GitHub Actions环境下跳过本地文件保存

## 🔍 故障排除

常见问题：
1. **网络超时**: 自动重试机制和备用方法
2. **内容获取失败**: 只保存成功获取内容的文章
3. **数据库连接**: 检查Supabase配置和网络连接
4. **重复文章**: 自动去重，不会重复存储

## 📈 监控指标

每次运行输出：
- 处理文章总数
- 去重后新文章数量
- 成功获取内容的文章数
- 数据库总文章数统计