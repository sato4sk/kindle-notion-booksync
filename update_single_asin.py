

import os
import sys
from notion_client import Client
from dotenv import load_dotenv

def update_single_asin(target_title, new_asin):
    """
    指定されたタイトルのNotionページのASINを更新する。
    """
    load_dotenv(override=True)

    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')

    if not all([notion_token, database_id, target_title, new_asin]):
        print("エラー: 必要な情報（環境変数、タイトル、ASIN）が不足しています。")
        return

    notion = Client(auth=notion_token)

    # --- 指定されたタイトルのページを検索 ---
    try:
        response = notion.databases.query(
            database_id=database_id,
            filter={
                "property": "タイトル",
                "title": {
                    "equals": target_title
                }
            }
        )
        pages = response.get("results", [])
        if not pages:
            print(f"エラー: '{target_title}' というタイトルの書籍が見つかりませんでした。")
            return
        
        page_id = pages[0]["id"]

        # --- ASINを更新 ---
        notion.pages.update(
            page_id=page_id,
            properties={
                "ASIN": {
                    "rich_text": [{
                        "text": {"content": new_asin}
                    }]
                }
            }
        )
        print(f"'{target_title}' のASINを '{new_asin}' に更新しました。")

    except Exception as e:
        print(f"処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使用方法: python update_single_asin.py \"書籍のタイトル\" \"新しいASIN\"")
    else:
        title_arg = sys.argv[1]
        asin_arg = sys.argv[2]
        update_single_asin(title_arg, asin_arg)

