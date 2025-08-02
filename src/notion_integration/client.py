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

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIResponseError)
)
def _notion_query_with_retry(notion_client, database_id, start_cursor):
    return notion_client.databases.query(
        database_id=database_id,
        start_cursor=start_cursor,
        page_size=100,
    )

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIResponseError)
)
def register_book_to_notion_page(notion_client, db_id, book_data, description, tags, book_type):
    """書籍データをNotionに登録する。"""
    properties = {
        "タイトル": {"title": [{"text": {"content": str(book_data.get("title", ""))}}]},
        "著者": {"rich_text": [{"text": {"content": str(book_data.get("author", ""))}}]},
        "出版社": {"rich_text": [{"text": {"content": str(book_data.get("publisher", ""))}}]},
        "ASIN": {"rich_text": [{"text": {"content": str(book_data.get("asin", ""))}}]}
    }
    if pd.notna(book_data.get("purchase_date")):
        properties["購入日"] = {"date": {"start": str(book_data["purchase_date"])}};
    if tags:
        properties["タグ"] = {"multi_select": [{"name": tag} for tag in tags]}
    if book_type:
        properties["種別"] = {"select": {"name": book_type}}

    page_content = []
    if description:
        page_content.append({
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": description}}]}
        })

    try:
        request_payload = {"parent": {"database_id": db_id}, "properties": properties}
        if page_content:
            request_payload["children"] = page_content
        notion_client.pages.create(**request_payload)
        print(f"-> '{book_data['title']}' をNotionに登録しました。")
    except APIResponseError as e:
        print(f"-> '{book_data['title']}' の登録中にAPIエラー: {e}")
        if e.code == "rate_limited":
            retry_after = e.headers.get("Retry-After")
            if retry_after:
                wait_time = int(retry_after)
                print(f"レート制限に達しました。{wait_time}秒待機します。")
                time.sleep(wait_time)
        raise # tenacityでリトライさせるために再raise
    except Exception as e:
        print(f"-> '{book_data['title']}' の登録中に予期せぬエラー: {e}")
        raise # tenacityでリトライさせるために再raise

