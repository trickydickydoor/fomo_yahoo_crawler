#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase数据库管理器
处理news_items表的所有操作
"""

import json
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set, Tuple
from supabase import create_client, Client


class SupabaseManager:
    """Supabase数据库管理器"""
    
    def __init__(self, url: str, key: str, table_name: str = "news_items"):
        """
        初始化Supabase连接
        
        Args:
            url: Supabase项目URL
            key: Supabase服务密钥
            table_name: 表名，默认为news_items
        """
        self.url = url
        self.key = key
        self.table_name = table_name
        self.client: Optional[Client] = None
        self._connect()
    
    def _connect(self):
        """建立Supabase连接"""
        try:
            self.client = create_client(self.url, self.key)
            print("✅ Supabase连接成功")
        except Exception as e:
            print(f"❌ Supabase连接失败: {e}")
            self.client = None
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.client is not None
    
    async def get_existing_articles(self) -> Tuple[Set[str], Set[str]]:
        """
        获取现有文章的URL和Title集合，用于查重
        
        Returns:
            Tuple[Set[str], Set[str]]: (existing_urls, existing_titles)
        """
        if not self.client:
            print("⚠️ 数据库未连接，跳过查重")
            return set(), set()
        
        try:
            print("🔍 查询现有文章进行查重...")
            
            # 尝试查询，处理可能的错误
            try:
                # 只查询需要的字段以提高性能
                result = self.client.table(self.table_name).select("url,title").execute()
                
                # 检查是否有数据返回
                if hasattr(result, 'data') and result.data is not None:
                    existing_urls = {item['url'] for item in result.data if item.get('url')}
                    existing_titles = {item['title'] for item in result.data if item.get('title')}
                    
                    print(f"📊 数据库中现有文章: {len(result.data)} 篇")
                    print(f"   - 现有URL: {len(existing_urls)} 个")
                    print(f"   - 现有标题: {len(existing_titles)} 个")
                    
                    return existing_urls, existing_titles
                else:
                    # 如果没有数据或表为空
                    print("📊 数据库表为空或无数据")
                    return set(), set()
                    
            except Exception as query_error:
                # 如果是权限或表不存在的问题
                error_str = str(query_error)
                if 'swagger' in error_str.lower() or 'openapi' in error_str.lower():
                    print(f"⚠️ API返回了文档而不是数据，可能是权限问题")
                    print("   请检查：")
                    print("   1. Service Role密钥是否正确")
                    print("   2. 表名是否正确")
                    print("   3. RLS策略是否限制了访问")
                elif 'not found' in error_str.lower():
                    print(f"⚠️ 表 '{self.table_name}' 不存在")
                else:
                    print(f"⚠️ 查询错误: {error_str[:200]}")
                return set(), set()
            
        except Exception as e:
            print(f"❌ 查询现有文章失败: {str(e)[:200]}")
            return set(), set()
    
    def check_duplicates(self, articles: List[Dict], existing_urls: Set[str], existing_titles: Set[str]) -> List[Dict]:
        """
        检查重复文章（基于URL和Title）
        
        Args:
            articles: 待检查的文章列表
            existing_urls: 现有的URL集合
            existing_titles: 现有的标题集合
            
        Returns:
            List[Dict]: 去重后的新文章列表
        """
        print(f"🔍 开始查重检查...")
        
        new_articles = []
        duplicate_count = 0
        
        for article in articles:
            url = article.get('link', '')
            title = article.get('title', '')
            
            # 检查URL和Title是否已存在
            if url in existing_urls:
                print(f"   ⚠️ 重复URL: {title[:50]}...")
                duplicate_count += 1
                continue
            
            if title in existing_titles:
                print(f"   ⚠️ 重复标题: {title[:50]}...")
                duplicate_count += 1
                continue
            
            # 通过查重检查的新文章
            new_articles.append(article)
        
        print(f"📊 查重结果:")
        print(f"   - 原始文章: {len(articles)} 篇")
        print(f"   - 重复文章: {duplicate_count} 篇")
        print(f"   - 新文章: {len(new_articles)} 篇")
        
        return new_articles
    
    def convert_to_supabase_format(self, article: Dict) -> Dict:
        """
        将爬虫数据格式转换为Supabase格式
        
        Args:
            article: 爬虫文章数据
            
        Returns:
            Dict: Supabase格式的文章数据
        """
        # 处理时间格式
        published_at = self._convert_time_to_iso(article.get('full_time', ''))
        
        # 生成UUID作为主键
        article_id = str(uuid.uuid4())
        
        return {
            'id': article_id,
            'title': article.get('title', ''),
            'url': article.get('link', ''),
            'published_at': published_at,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'content': article.get('content', ''),
            'source': article.get('source', 'Yahoo Finance'),
            'companies': [],  # 待实现公司提取
            'industries': ['Financial News'],  # 默认分类
            'embedding_status': 'pending',  # 待向量化
            'embedding_vector_id': None,
            'embedded_at': None,
            'embedding_model': None
        }
    
    def _convert_time_to_iso(self, time_str: str) -> str:
        """
        将时间字符串转换为ISO格式
        
        Args:
            time_str: 原始时间字符串
            
        Returns:
            str: ISO格式的时间字符串
        """
        if not time_str or time_str == 'Recent':
            # 如果没有具体时间，使用当前时间
            return datetime.now(timezone.utc).isoformat()
        
        try:
            # 尝试解析常见的时间格式
            if 'T' in time_str and ('Z' in time_str or '+' in time_str):
                # 已经是ISO格式
                return time_str
            
            # 其他格式可以在这里添加解析逻辑
            # 暂时使用当前时间
            return datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            print(f"⚠️ 时间格式转换失败 ({time_str}): {e}")
            return datetime.now(timezone.utc).isoformat()
    
    async def insert_articles(self, articles: List[Dict]) -> bool:
        """
        批量插入文章到数据库
        
        Args:
            articles: 要插入的文章列表
            
        Returns:
            bool: 是否插入成功
        """
        if not articles:
            print("📝 没有新文章需要插入")
            return True
        
        if not self.client:
            print("❌ 数据库未连接，无法插入数据")
            return False
        
        try:
            print(f"📝 开始插入 {len(articles)} 篇新文章到数据库...")
            
            # 转换数据格式
            supabase_articles = [self.convert_to_supabase_format(article) for article in articles]
            
            # 批量插入
            result = self.client.table(self.table_name).insert(supabase_articles).execute()
            
            if result.data:
                print(f"✅ 成功插入 {len(result.data)} 篇文章到数据库")
                
                # 显示插入的文章标题
                for i, article in enumerate(result.data[:5], 1):
                    print(f"   {i}. {article['title'][:50]}...")
                
                if len(result.data) > 5:
                    print(f"   ... 还有 {len(result.data) - 5} 篇文章")
                
                return True
            else:
                print("⚠️ 插入结果为空")
                return False
                
        except Exception as e:
            print(f"❌ 插入文章失败: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """
        获取数据库统计信息
        
        Returns:
            Dict: 统计信息
        """
        if not self.client:
            return {"error": "数据库未连接"}
        
        try:
            # 获取总数
            result = self.client.table(self.table_name).select("*", count="exact").execute()
            total_count = result.count
            
            # 获取来源统计
            source_result = self.client.table(self.table_name).select("source").execute()
            sources = {}
            for item in source_result.data:
                source = item.get('source', 'Unknown')
                sources[source] = sources.get(source, 0) + 1
            
            return {
                "total_articles": total_count,
                "sources": sources,
                "table_name": self.table_name
            }
            
        except Exception as e:
            return {"error": f"获取统计信息失败: {e}"}


def create_supabase_manager(config: Dict) -> Optional[SupabaseManager]:
    """
    创建Supabase管理器实例
    
    Args:
        config: Supabase配置字典
        
    Returns:
        SupabaseManager: 管理器实例，连接失败时返回None
    """
    try:
        manager = SupabaseManager(
            url=config['url'],
            key=config['anon_key'],
            table_name=config.get('table_name', 'news_items')
        )
        return manager if manager.is_connected() else None
        
    except Exception as e:
        print(f"❌ 创建Supabase管理器失败: {e}")
        return None