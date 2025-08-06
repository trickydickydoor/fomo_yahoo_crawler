#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo Finance News 爬虫
使用 Crawl4AI 处理动态内容和滚动加载
"""

import asyncio
import time
import json
import csv
import re
import os
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from asyncio import Semaphore

# Web scraping
import requests
from bs4 import BeautifulSoup

# Crawl4AI
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, VirtualScrollConfig

# Supabase
from supabase_manager import SupabaseManager, create_supabase_manager

# 尝试加载.env文件（本地开发使用）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # GitHub Actions环境不需要dotenv
    pass


class AntiDetection:
    """反反爬虫工具类"""
    
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ]
    
    @staticmethod
    def get_random_headers():
        """获取随机请求头"""
        return {
            'User-Agent': random.choice(AntiDetection.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
    
    @staticmethod
    async def random_delay(min_seconds=1.0, max_seconds=3.0):
        """随机延迟"""
        delay = random.uniform(min_seconds, max_seconds)
        await asyncio.sleep(delay)


class YahooNewsCrawl4AICrawler:
    """基于Crawl4AI的Yahoo Finance新闻爬虫 - 支持多URL爬取"""
    
    def __init__(self, supabase_config=None):
        self.articles = []
        self.urls = [
            "https://finance.yahoo.com/topic/latest-news/",
            "https://finance.yahoo.com/news/", 
            "https://finance.yahoo.com/topic/tech/"
        ]
        self.is_ci_environment = self._detect_ci_environment()
        
        # 初始化Supabase管理器
        self.supabase_manager = None
        if supabase_config:
            self.supabase_manager = create_supabase_manager(supabase_config)
            if self.supabase_manager:
                print("🗄️ Supabase数据库集成已启用")
            else:
                print("⚠️ Supabase连接失败，将只保存本地文件")
        
    def _detect_ci_environment(self):
        """检测是否在CI/CD环境中运行"""
        ci_indicators = [
            'GITHUB_ACTIONS',
            'CI', 
            'CONTINUOUS_INTEGRATION',
            'GITLAB_CI',
            'JENKINS_URL'
        ]
        return any(os.getenv(indicator) for indicator in ci_indicators)
        
    def is_within_hours(self, time_text, max_hours=2):
        """检查时间是否在指定小时内"""
        if not time_text:
            return True
        
        time_text = str(time_text).lower().strip()
        
        # 匹配"X分钟前" 或 "X minutes ago"
        if 'minute' in time_text or '分钟' in time_text:
            return True
        
        # 匹配"X小时前" 或 "X hours ago"  
        if 'hour' in time_text or '小时' in time_text:
            hours_match = re.search(r'(\d+)', time_text)
            if hours_match:
                hours = int(hours_match.group(1))
                return hours <= max_hours
            return True  # "an hour ago" 或 "1小时前"
        
        # 匹配"刚刚"、"just now"等
        if any(word in time_text for word in ['just now', 'now', '刚刚', '刚才']):
            return True
            
        # 如果无法解析，默认包含（假设是最新的）
        return True
    
    async def get_article_content(self, article_url):
        """获取文章的完整内容和准确时间 - 使用requests替代Crawl4AI"""
        try:
            # 随机延迟，避免请求过于频繁
            await AntiDetection.random_delay(0.5, 2.0)
            
            # 使用随机请求头
            headers = AntiDetection.get_random_headers()
            
            # 设置会话
            timeout = 15 if self.is_ci_environment else 10
            
            # 发起请求
            response = requests.get(article_url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            # 解析内容
            return self._extract_article_details(response.text, article_url)
            
        except requests.exceptions.Timeout:
            print(f"⚠️ 请求超时: {article_url}")
            return {"content": "", "full_time": ""}
        except requests.exceptions.RequestException as e:
            print(f"⚠️ 请求失败 {article_url}: {e}")
            return {"content": "", "full_time": ""}
        except Exception as e:
            print(f"⚠️ 获取文章内容出错 {article_url}: {e}")
            return {"content": "", "full_time": ""}
    
    def _extract_article_details(self, html_content, article_url):
        """从文章页面HTML中提取内容和时间"""
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # 提取文章内容
            content_parts = []
            
            # 1. 获取主要内容区域
            main_content_selectors = [
                'div[data-testid="caas-body"]',
                '.caas-body',
                'div.caas-body',
                'div[class*="body"]'
            ]
            
            main_content = None
            for selector in main_content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break
            
            if main_content:
                # 2. 获取可见的段落内容
                visible_paragraphs = main_content.find_all('p', class_=lambda x: x and 'yf-' in str(x))
                for p in visible_paragraphs:
                    text = p.get_text(strip=True)
                    if text and len(text) > 10:  # 过滤太短的段落
                        content_parts.append(text)
                
                # 3. 获取隐藏的read-more内容
                read_more_wrapper = main_content.find('div', class_='read-more-wrapper')
                if read_more_wrapper:
                    hidden_paragraphs = read_more_wrapper.find_all('p')
                    for p in hidden_paragraphs:
                        # 获取HTML_TAG_START到HTML_TAG_END之间的内容
                        text = p.get_text(strip=True)
                        if text and len(text) > 10 and 'Read the original article' not in text:
                            content_parts.append(text)
            
            # 提取准确的发布时间
            full_time = ""
            time_selectors = [
                'time[datetime]',
                '[data-testid="timestamp"]',
                'div[data-testid="caas-attr-time-style"]',
                '.caas-attr-time-style'
            ]
            
            for selector in time_selectors:
                time_element = soup.select_one(selector)
                if time_element:
                    full_time = time_element.get('datetime') or time_element.get_text(strip=True)
                    if full_time:
                        break
            
            # 组合完整内容
            full_content = '\n\n'.join(content_parts) if content_parts else ""
            
            return {
                "content": full_content,
                "full_time": full_time or "Recent"
            }
            
        except Exception as e:
            print(f"⚠️ 解析文章详情出错 {article_url}: {e}")
            return {"content": "", "full_time": ""}
    
    
    def parse_html_content(self, html_content, max_hours=2):
        """解析HTML内容，提取新闻数据"""
        print("解析HTML内容...")
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # 调试：查看页面中有哪些相关的CSS类
            all_lis = soup.find_all('li')
            print(f"页面中总共有 {len(all_lis)} 个li元素")
            
            # 查找包含"stream"或"item"的li元素
            stream_items = soup.find_all('li', class_=lambda x: x and ('stream' in str(x) or 'item' in str(x)))
            print(f"找到 {len(stream_items)} 个包含stream或item的li元素")
            
            # 显示前几个和最后几个的class名称
            for i, item in enumerate(stream_items[:3]):
                classes = item.get('class')
                print(f"  开始li元素 {i+1}: class='{classes}'")
            
            if len(stream_items) > 6:
                print("  ...")
                for i, item in enumerate(stream_items[-3:], len(stream_items)-2):
                    classes = item.get('class')
                    print(f"  结尾li元素 {i}: class='{classes}'")
            
            # 查找所有新闻项目（排除广告） - 使用更宽松的匹配
            story_items = []
            
            # 方法1：严格匹配
            story_items_strict = soup.find_all('li', class_=lambda x: x and 'stream-item' in str(x) and 'story-item' in str(x) and 'ad-item' not in str(x))
            print(f"严格匹配找到 {len(story_items_strict)} 个story-item")
            
            # 方法2：宽松匹配
            story_items_loose = soup.find_all('li', class_=lambda x: x and 'item' in str(x) and 'ad' not in str(x))
            print(f"宽松匹配找到 {len(story_items_loose)} 个item（非广告）")
            
            # 使用有结果的方法
            if len(story_items_strict) > 0:
                story_items = story_items_strict
                print("使用严格匹配结果")
            elif len(story_items_loose) > 0:
                story_items = story_items_loose
                print("使用宽松匹配结果")
            else:
                # 方法3：最宽松匹配 - 查找任何包含链接的li
                story_items = [item for item in all_lis if item.find('a', href=True)]
                print(f"最宽松匹配找到 {len(story_items)} 个包含链接的li元素")
            
            found_count = 0
            for item in story_items:  # 处理所有story_items
                try:
                    # 从a标签的aria-label属性获取标题
                    title_link = item.find('a', {'aria-label': True})
                    if not title_link:
                        # 备用：查找任何有href的a标签
                        title_link = item.find('a', href=True)
                        if not title_link:
                            continue
                    
                    # 提取标题 - 优先使用aria-label
                    title = title_link.get('aria-label')
                    if not title:
                        title = title_link.get_text(strip=True)
                    
                    if not title or len(title) < 10:
                        continue
                    
                    # 提取链接
                    link = title_link.get('href')
                    if not link:
                        continue
                        
                    # 处理相对链接
                    if link.startswith('/'):
                        link = f"https://finance.yahoo.com{link}"
                    elif not link.startswith('http'):
                        continue
                    
                    # 暂时不过滤时间，先获取数据
                    time_text = 'Recent'
                    
                    # 去重检查 - 基于title和link
                    if any(existing['link'] == link or existing['title'] == title for existing in self.articles):
                        continue
                    
                    article = {
                        'title': title,
                        'link': link, 
                        'time': time_text,
                        'source': 'Yahoo Finance',
                        'content': '',  # 稍后填充
                        'full_time': ''  # 稍后填充准确时间
                    }
                    
                    self.articles.append(article)
                    found_count += 1
                    print(f"[{found_count:2}] {title[:60]}... ({time_text})")
                    
                except Exception as e:
                    print(f"解析单个新闻项出错: {e}")
                    continue
            
            print(f"\n成功解析 {len(self.articles)} 篇最近{max_hours}小时的新闻")
            return self.articles
            
        except Exception as e:
            print(f"HTML解析出错: {e}")
            return []
    
    async def crawl_single_url(self, url, max_hours=2):
        """爬取单个URL"""
        print(f"爬取URL: {url}")
        
        
        try:
            # 配置浏览器 - 针对CI环境优化
            browser_config = BrowserConfig(
                browser_type="chromium", 
                headless=True,
                verbose=False,  # CI环境中减少日志
                extra_args=[
                    "--no-sandbox",  # CI环境必需
                    "--disable-dev-shm-usage",  # 避免内存问题
                    "--disable-gpu",  # 禁用GPU
                    "--disable-web-security",  # 禁用Web安全限制
                    "--disable-features=VizDisplayCompositor"  # 减少资源使用
                ] if self.is_ci_environment else []
            )
            
            # 配置虚拟滚动 - 使用html容器，15次滚动
            virtual_scroll_config = VirtualScrollConfig(
                container_selector="html",  # html容器最稳定
                scroll_count=15,  # 减少到15次滚动
                scroll_by="page_height",  # 按页面高度滚动
                wait_after_scroll=1.5  # 等待1.5秒
            )
            
            # 配置爬取参数 - 不等待DOM，直接获取内容
            crawl_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=45000,  # 45秒超时
                wait_for=None,  # 不等待特定事件，滚动完成后直接获取内容
                virtual_scroll_config=virtual_scroll_config
            )
            
            # 创建爬虫
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(
                    url=url,
                    config=crawl_config
                )
                
                if result.success:
                    print(f"✅ {url} - Crawl4AI爬取成功")
                    print(f"页面内容长度: {len(result.html)} 字符")
                    
                    # 解析HTML内容
                    articles = self.parse_html_content(result.html, max_hours)
                    return articles
                else:
                    print(f"❌ {url} - Crawl4AI爬取失败: {result.error_message}")
                    return []
                    
        except Exception as e:
            print(f"❌ {url} - Crawl4AI爬取出错: {e}")
            return []

    async def crawl_with_crawl4ai(self, max_hours=2, max_articles=None):
        """使用Crawl4AI虚拟滚动爬取所有URL的新闻 - 新增Supabase集成流程"""
        print("使用Crawl4AI虚拟滚动爬取多个URL...")
        
        # 阶段1: 获取所有URL的基础文章列表（title, link, time）
        print("\n=== 阶段1: 获取文章列表 ===")
        all_articles = []
        
        for url in self.urls:
            try:
                articles = await self.crawl_single_url(url, max_hours)
                if articles:
                    print(f"从 {url} 获取到 {len(articles)} 篇新闻")
                    all_articles.extend(articles)
                else:
                    print(f"从 {url} 未获取到新闻，尝试备用方法...")
                    fallback_articles = await self.crawl_single_url_fallback(url, max_hours)
                    if fallback_articles:
                        print(f"备用方法从 {url} 获取到 {len(fallback_articles)} 篇新闻")
                        all_articles.extend(fallback_articles)
                        
            except Exception as e:
                print(f"处理URL {url} 时出错: {e}")
                continue
        
        # 内部去重（基于title和link）
        self.articles = []
        for article in all_articles:
            if not any(existing['link'] == article['link'] or existing['title'] == article['title'] for existing in self.articles):
                self.articles.append(article)
        
        print(f"\n从所有URL获取 {len(all_articles)} 篇新闻，内部去重后保留 {len(self.articles)} 篇")
        
        # 阶段2: Supabase查重检查
        print("\n=== 阶段2: 数据库查重检查 ===")
        new_articles = self.articles  # 默认都是新文章
        
        if self.supabase_manager and self.supabase_manager.is_connected():
            try:
                # 获取数据库中现有的URL和Title
                existing_urls, existing_titles = self.supabase_manager.get_existing_articles()
                
                # 进行查重检查
                new_articles = self.supabase_manager.check_duplicates(self.articles, existing_urls, existing_titles)
                
            except Exception as e:
                print(f"⚠️ 数据库查重失败，将处理所有文章: {e}")
                new_articles = self.articles
        else:
            print("⚠️ 未连接数据库，跳过数据库查重")
        
        if not new_articles:
            print("✅ 没有新文章需要处理")
            return self.articles
        
        # 阶段3: 只对新文章获取详细内容（保持5线程并发）
        print(f"\n=== 阶段3: 获取新文章详细内容 ===")
        print(f"需要获取详细内容的新文章: {len(new_articles)} 篇")
        
        # 限制处理数量（用于测试）
        articles_to_process = new_articles[:max_articles] if max_articles else new_articles
        if max_articles and len(new_articles) > max_articles:
            print(f"📝 限制处理文章数量: {max_articles} 篇（总共有 {len(new_articles)} 篇新文章）")
        
        # 获取详细内容 - 降低并发数以减少被检测的风险
        concurrent_limit = 3 if self.is_ci_environment else 5
        await self.fetch_articles_content_for_articles(articles_to_process, max_concurrent=concurrent_limit)
        
        # 阶段4: 保存到数据库
        if self.supabase_manager and self.supabase_manager.is_connected():
            print(f"\n=== 阶段4: 保存到数据库 ===")
            try:
                # 只保存有内容的文章
                articles_with_content = [article for article in articles_to_process if article.get('content', '').strip()]
                
                if articles_with_content:
                    success = self.supabase_manager.insert_articles(articles_with_content)
                    if success:
                        print(f"✅ 成功将 {len(articles_with_content)} 篇文章保存到数据库")
                    else:
                        print(f"⚠️ 数据库保存失败")
                else:
                    print("⚠️ 没有包含内容的文章可以保存")
                    
            except Exception as e:
                print(f"❌ 数据库保存过程出错: {e}")
        
        # 更新self.articles为处理过的文章
        self.articles = articles_to_process
        
        return self.articles
    
    async def fetch_articles_content_for_articles(self, articles, max_concurrent=5):
        """批量并发获取指定文章列表的详细内容和时间"""
        if not articles:
            return
        
        total_articles = len(articles)
        print(f"\n开始并发获取 {total_articles} 篇文章的详细内容（{max_concurrent}并发）...")
        
        # 创建信号量控制并发数量
        semaphore = Semaphore(max_concurrent)
        
        # 创建任务列表
        tasks = []
        for i, article in enumerate(articles):
            task = self.fetch_single_article_content(semaphore, i+1, total_articles, article)
            tasks.append(task)
        
        # 并发执行所有任务
        await asyncio.gather(*tasks, return_exceptions=True)
        
        print(f"\n文章内容获取完成!")
    
    async def fetch_articles_content(self, max_articles=None, max_concurrent=5):
        """批量并发获取文章的详细内容和时间（兼容旧接口）"""
        if not self.articles:
            return
        
        # 限制文章数量以避免过长时间
        articles_to_process = self.articles[:max_articles] if max_articles else self.articles
        await self.fetch_articles_content_for_articles(articles_to_process, max_concurrent)
    
    async def fetch_single_article_content(self, semaphore, index, total, article):
        """获取单篇文章内容（带并发控制）"""
        async with semaphore:  # 限制并发数量
            try:
                print(f"[{index:2}/{total}] 获取内容: {article['title'][:50]}...")
                
                # 添加总体超时保护 - requests比Crawl4AI快，缩短超时时间
                timeout_limit = 20.0 if self.is_ci_environment else 15.0
                try:
                    details = await asyncio.wait_for(
                        self.get_article_content(article['link']),
                        timeout=timeout_limit
                    )
                except asyncio.TimeoutError:
                    print(f"    ⚠️ [{index:2}] 获取超时，跳过")
                    details = {"content": "", "full_time": ""}
                
                # 更新文章信息
                article['content'] = details['content']
                article['full_time'] = details['full_time']
                
                if details['content']:
                    content_length = len(details['content'])
                    print(f"    ✅ [{index:2}] 获取到 {content_length} 字符内容")
                else:
                    print(f"    ⚠️ [{index:2}] 未获取到内容")
                    
            except Exception as e:
                print(f"    ❌ [{index:2}] 获取内容失败: {e}")
                # 确保失败时也有默认值
                article['content'] = ''
                article['full_time'] = ''
    
    async def crawl_single_url_fallback(self, url, max_hours=2):
        """单URL备用爬取方法（使用requests + 反反爬虫）"""
        print(f"备用方法爬取: {url}")
        
        try:
            # 添加随机延迟
            await AntiDetection.random_delay(1.0, 3.0)
            
            # 使用随机请求头
            headers = AntiDetection.get_random_headers()
            
            timeout = 20 if self.is_ci_environment else 15
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            print(f"✅ {url} - 备用方法：页面内容长度 {len(response.text)} 字符")
            articles = self.parse_html_content(response.text, max_hours)
            return articles
            
        except Exception as e:
            print(f"❌ {url} - 备用爬取方法失败: {e}")
            return []

    async def crawl_fallback(self, max_hours=2, max_articles=None):
        """备用爬取方法（不使用Crawl4AI）- 爬取所有URL，集成Supabase流程"""
        print("使用备用方法爬取所有URL...")
        
        # 阶段1: 获取文章列表
        print("\n=== 阶段1: 获取文章列表（备用方法）===")
        all_articles = []
        
        for url in self.urls:
            try:
                articles = await self.crawl_single_url_fallback(url, max_hours)
                if articles:
                    print(f"备用方法从 {url} 获取到 {len(articles)} 篇新闻")
                    all_articles.extend(articles)
                    
            except Exception as e:
                print(f"备用方法处理URL {url} 时出错: {e}")
                continue
        
        # 内部去重
        self.articles = []
        for article in all_articles:
            if not any(existing['link'] == article['link'] or existing['title'] == article['title'] for existing in self.articles):
                self.articles.append(article)
        
        print(f"\n备用方法获取 {len(all_articles)} 篇新闻，内部去重后保留 {len(self.articles)} 篇")
        
        # 阶段2: Supabase查重检查
        print("\n=== 阶段2: 数据库查重检查 ===")
        new_articles = self.articles
        
        if self.supabase_manager and self.supabase_manager.is_connected():
            try:
                existing_urls, existing_titles = self.supabase_manager.get_existing_articles()
                new_articles = self.supabase_manager.check_duplicates(self.articles, existing_urls, existing_titles)
            except Exception as e:
                print(f"⚠️ 数据库查重失败，将处理所有文章: {e}")
                new_articles = self.articles
        else:
            print("⚠️ 未连接数据库，跳过数据库查重")
        
        if not new_articles:
            print("✅ 没有新文章需要处理")
            return self.articles
        
        # 阶段3: 获取新文章详细内容
        print(f"\n=== 阶段3: 获取新文章详细内容 ===")
        articles_to_process = new_articles[:max_articles] if max_articles else new_articles
        if max_articles and len(new_articles) > max_articles:
            print(f"📝 限制处理文章数量: {max_articles} 篇（总共有 {len(new_articles)} 篇新文章）")
        
        # 获取详细内容 - 降低并发数以减少被检测的风险
        concurrent_limit = 3 if self.is_ci_environment else 5
        await self.fetch_articles_content_for_articles(articles_to_process, max_concurrent=concurrent_limit)
        
        # 阶段4: 保存到数据库
        if self.supabase_manager and self.supabase_manager.is_connected():
            print(f"\n=== 阶段4: 保存到数据库 ===")
            try:
                articles_with_content = [article for article in articles_to_process if article.get('content', '').strip()]
                if articles_with_content:
                    success = self.supabase_manager.insert_articles(articles_with_content)
                    if success:
                        print(f"✅ 成功将 {len(articles_with_content)} 篇文章保存到数据库")
            except Exception as e:
                print(f"❌ 数据库保存过程出错: {e}")
        
        self.articles = articles_to_process
        return self.articles
    
    def save_json(self, filename=None):
        """保存为JSON格式"""
        if not self.articles:
            print("没有数据可保存")
            return None
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"yahoo_multi_news_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.articles, f, ensure_ascii=False, indent=2)
        
        print(f"JSON已保存: {filename}")
        return filename
    
    def save_csv(self, filename=None):
        """保存为CSV格式"""
        if not self.articles:
            return None
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"yahoo_multi_news_{timestamp}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['title', 'link', 'time', 'source', 'content', 'full_time']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.articles)
        
        print(f"CSV已保存: {filename}")
        return filename
    
    def show_summary(self):
        """显示爬取结果摘要"""
        if not self.articles:
            print("没有找到新闻")
            return
        
        print(f"\n{'='*60}")
        print(f"多URL爬取结果汇总")
        print(f"{'='*60}")
        print(f"总计: {len(self.articles)} 篇新闻（已去重）")
        print(f"数据源:")
        for url in self.urls:
            print(f"  - {url}")
        
        # 按时间分类统计
        recent_count = sum(1 for a in self.articles if 'minute' in a['time'].lower())
        hour_count = sum(1 for a in self.articles if 'hour' in a['time'].lower())
        
        if recent_count > 0 or hour_count > 0:
            print(f"\n时间分布:")
            if recent_count > 0:
                print(f"  分钟内: {recent_count} 篇")
            if hour_count > 0:
                print(f"  小时内: {hour_count} 篇")
        
        print(f"\n最新的10篇新闻:")
        print("-" * 60)
        for i, article in enumerate(self.articles[:10], 1):
            print(f"{i:2}. {article['title'][:65]}...")
            print(f"    时间: {article['time']}")
            print()


async def main():
    """主函数"""
    print("=" * 65)
    print("Yahoo Finance News 多URL爬虫 - Crawl4AI + Supabase集成")
    print("=" * 65)
    
    # 第一步：立即检查环境变量配置
    print("\n=== 第1步: 环境变量检查 ===")
    is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
    print(f"运行环境: {'GitHub Actions' if is_github_actions else 'Local'}")
    
    # Supabase配置 - 从环境变量读取
    supabase_config = {
        'url': os.getenv('SUPABASE_URL'),
        'anon_key': os.getenv('SUPABASE_ANON_KEY'),
        'table_name': os.getenv('SUPABASE_TABLE_NAME', 'news_items')
    }
    
    print(f"SUPABASE_URL: {supabase_config['url'] or '未设置'}")
    print(f"SUPABASE_TABLE_NAME: {supabase_config['table_name']}")
    print(f"SUPABASE_ANON_KEY: {'已设置' if supabase_config['anon_key'] else '未设置'}")
    if supabase_config['anon_key']:
        print(f"SUPABASE_ANON_KEY长度: {len(supabase_config['anon_key'])}")
        print(f"SUPABASE_ANON_KEY前缀: {supabase_config['anon_key'][:20]}...")
    
    # 检查必要的环境变量
    if not supabase_config['url'] or not supabase_config['anon_key']:
        print("\n❌ 缺少必要的环境变量:")
        print("   - SUPABASE_URL")
        print("   - SUPABASE_ANON_KEY")
        if is_github_actions:
            print("请在GitHub仓库设置中配置这些Secrets")
            exit(1)
        else:
            print("请在本地环境中设置这些变量")
            exit(1)
    
    # 第二步：立即测试Supabase连接
    print("\n=== 第2步: Supabase连接测试 ===")
    test_manager = create_supabase_manager(supabase_config)
    if not test_manager or not test_manager.is_connected():
        print("❌ Supabase连接测试失败，程序终止")
        exit(1)
    else:
        print("✅ Supabase连接测试成功")
        # 立即测试数据库查询
        print("\n=== 第3步: 数据库查询测试 ===")
        try:
            existing_urls, existing_titles = test_manager.get_existing_articles()
            print(f"✅ 数据库查询成功 - 现有文章: {len(existing_urls)} 个URL, {len(existing_titles)} 个标题")
        except Exception as e:
            print(f"❌ 数据库查询测试失败: {e}")
            exit(1)
    
    # 创建爬虫（带Supabase配置）
    crawler = YahooNewsCrawl4AICrawler(supabase_config=supabase_config)
    
    max_hours = int(os.getenv('INPUT_MAX_HOURS', '2'))  # 支持工作流参数
    print(f"\n爬取时间范围: 最近{max_hours}小时")
    
    # 尝试使用Crawl4AI爬取（生产环境无文章数量限制）
    print("尝试使用Crawl4AI爬取...")
    articles = await crawler.crawl_with_crawl4ai(max_hours=max_hours)
    
    # 如果Crawl4AI失败，使用备用方法
    if not articles:
        print("\nCrawl4AI爬取失败，尝试备用方法...")
        articles = await crawler.crawl_fallback(max_hours=max_hours)
    
    if articles:
        # 显示结果
        crawler.show_summary()
        
        # 显示数据库统计
        if crawler.supabase_manager and crawler.supabase_manager.is_connected():
            print("\n=== 数据库统计信息 ===")
            stats = crawler.supabase_manager.get_stats()
            if 'error' not in stats:
                print(f"数据库总文章数: {stats['total_articles']}")
                print(f"数据来源分布: {stats['sources']}")
            else:
                print(f"获取统计信息失败: {stats['error']}")
        
        print(f"\n🎉 爬取完成!")
        print(f"✅ 处理文章数: {len(articles)}")
        print(f"📊 数据已保存到Supabase数据库")
        
        # GitHub Actions环境下不保存本地文件
        if not is_github_actions:
            print(f"\n保存本地备份文件...")
            json_file = crawler.save_json()
            csv_file = crawler.save_csv()
            print(f"本地备份: JSON: {json_file}, CSV: {csv_file}")
        
    else:
        print("\n❌ 爬取失败，没有获取到新闻数据")
        print("可能原因:")
        print("1. 网络连接问题")
        print("2. 页面结构发生变化") 
        print("3. Crawl4AI配置问题")
        print("4. Supabase连接问题")
        
        # 在GitHub Actions中返回非零退出码
        if is_github_actions:
            exit(1)


if __name__ == "__main__":
    asyncio.run(main())