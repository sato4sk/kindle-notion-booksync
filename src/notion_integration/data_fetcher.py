import os
import json
import pandas as pd
import requests
import google.generativeai as genai
from notion_client import Client
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type, wait_fixed
import time
from notion_client.errors import APIResponseError
from .client import _notion_query_with_retry

def get_existing_asins(notion_client, database_id):
    """Notionデータベースから既存のすべてのASINを取得する。"""
    existing_asins = set()
    has_more = True
    start_cursor = None
    while has_more:
        try:
            response = _notion_query_with_retry(notion_client, database_id, start_cursor)
            for page in response.get("results", []):
                asin_property = page.get("properties", {}).get("ASIN", {})
                rich_text = asin_property.get("rich_text", [])
                if rich_text:
                    existing_asins.add(rich_text[0].get("plain_text"))
            
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")

        except APIResponseError as e:
            print(f"Notionから既存ASINの取得中にAPIエラーが発生しました: {e}")
            if e.code == "rate_limited":
                retry_after = e.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                    print(f"レート制限に達しました。{wait_time}秒待機します。")
                    time.sleep(wait_time)
            break
        except Exception as e:
            print(f"Notionから既存ASINの取得中に予期せぬエラーが発生しました: {e}")
            break
    return existing_asins

def get_existing_titles(notion_client, database_id):
    """Notionデータベースから既存のすべての書籍タイトルを取得する。"""
    existing_titles = set()
    has_more = True
    start_cursor = None
    while has_more:
        try:
            response = _notion_query_with_retry(notion_client, database_id, start_cursor)
            for page in response.get("results", []):
                title_property = page.get("properties", {}).get("書籍名", {})
                title_list = title_property.get("title", [])
                if title_list:
                    existing_titles.add(title_list[0].get("plain_text"))
            
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")

        except APIResponseError as e:
            print(f"Notionから既存タイトルの取得中にAPIエラーが発生しました: {e}")
            if e.code == "rate_limited":
                retry_after = e.headers.get("Retry-After")
                if retry_after:
                    wait_time = int(retry_after)
                    print(f"レート制限に達しました。{wait_time}秒待機します。")
                    time.sleep(wait_time)
            break
        except Exception as e:
            print(f"Notionから既存タイトルの取得中に予期せぬエラーが発生しました: {e}")
            break
    return existing_titles

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIResponseError)
)
def get_notion_select_options(notion_client, database_id, property_name):
    """Notionデータベースから指定されたプロパティの選択肢を取得する。"""
    try:
        db_info = notion_client.databases.retrieve(database_id=database_id)
        prop = db_info['properties'].get(property_name)
        if prop and prop['type'] in ['multi_select', 'select']:
            return [option['name'] for option in prop[prop['type']]['options']]
    except APIResponseError as e:
        print(f"Notionから'{property_name}'の選択肢取得中にAPIエラーが発生しました: {e}")
        if e.code == "rate_limited":
            retry_after = e.headers.get("Retry-After")
            if retry_after:
                wait_time = int(retry_after)
                print(f"レート制限に達しました。{wait_time}秒待機します。")
                time.sleep(wait_time)
        return []
    except Exception as e:
        print(f"Notionから'{property_name}'の選択肢取得中に予期せぬエラーが発生しました: {e}")
    return []