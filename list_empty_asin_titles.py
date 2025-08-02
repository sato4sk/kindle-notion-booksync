
import os
from notion_client import Client
from dotenv import load_dotenv

def list_empty_asin_titles():
    """
    NotionデータベースからASINが空のページのタイトルをリストアップする。
    """
    load_dotenv(override=True)

    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')

    if not notion_token or not database_id:
        print("エラー: 環境変数が設定されていません。")
        return

    notion = Client(auth=notion_token)

    print("NotionデータベースからASINが空の書籍を取得しています...")
    empty_asin_titles = []
    has_more = True
    start_cursor = None
    while has_more:
        try:
            response = notion.databases.query(
                database_id=database_id,
                filter={
                    "property": "ASIN",
                    "rich_text": {
                        "is_empty": True
                    }
                },
                start_cursor=start_cursor,
                page_size=100
            )
            results = response.get("results", [])
            for page in results:
                title_property = page.get('properties', {}).get('タイトル', {}).get('title', [])
                if title_property:
                    empty_asin_titles.append(title_property[0].get('plain_text', '（無題）'))
            
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")

        except Exception as e:
            print(f"Notionからのデータ取得中にエラーが発生しました: {e}")
            return

    if not empty_asin_titles:
        print("-> ASINが空の書籍は見つかりませんでした。")
    else:
        print("--- ASINが空の書籍タイトル一覧 ---")
        for title in empty_asin_titles:
            print(f"- {title}")
        print("----------------------------------")
        print(f"合計: {len(empty_asin_titles)}件")

if __name__ == "__main__":
    list_empty_asin_titles()
