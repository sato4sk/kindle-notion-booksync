

import os
from notion_client import Client
from dotenv import load_dotenv

def debug_notion_fetch():
    """
    Notionデータベースの接続情報をデバッグ表示する。
    DB ID、DB名、取得できた全ページのタイトルをコンソールに出力する。
    """
    # --- 0. .envファイルの内容を直接読み込んで表示 ---
    print("--- .envファイルの内容を直接読み込み ---")
    try:
        with open(".env", "r") as f:
            print(f.read())
        print("-------------------------------------")
    except Exception as e:
        print(f".envファイルの読み込みに失敗: {e}")
        print("-------------------------------------")

    load_dotenv(override=True)

    # --- 1. 環境変数の読み込みとクライアントの初期化 ---
    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')

    if not notion_token or not database_id:
        print("エラー: NOTION_API_TOKEN または NOTION_DB_ID が.envファイルに設定されていません。")
        return

    notion = Client(auth=notion_token)

    # --- 2. データベースIDとデータベース名の表示 ---
    print(f"\n使用しているNotionデータベースID (os.getenv): {database_id}")

    try:
        db_info = notion.databases.retrieve(database_id=database_id)
        db_title = db_info.get('title', [{}])[0].get('plain_text', '（タイトル取得失敗）')
        print(f"接続中のデータベース名: {db_title}")
    except Exception as e:
        print(f"データベース情報の取得中にエラーが発生しました: {e}")
        print("-> DB IDが間違っているか、インテグレーションにDBへのアクセス権がありません。")
        return

    # --- 3. Notionの全ページを取得してタイトルを表示 ---
    print("\nデータベースから全ページのタイトルを取得します...")
    all_notion_pages = []
    has_more = True
    start_cursor = None
    while has_more:
        try:
            response = notion.databases.query(
                database_id=database_id,
                start_cursor=start_cursor,
                page_size=100
            )
            all_notion_pages.extend(response.get("results", []))
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")
        except Exception as e:
            print(f"ページ取得中にエラーが発生しました: {e}")
            return

    if not all_notion_pages:
        print("-> ページは1件も見つかりませんでした。")
    else:
        print("--- 取得したページタイトル一覧 ---")
        for i, page in enumerate(all_notion_pages):
            title_property = page.get('properties', {}).get('タイトル', {}).get('title', [])
            if title_property:
                page_title = title_property[0].get('plain_text', '（無題）')
                print(f"{i+1}: {page_title}")
        print("----------------------------------")

    print(f"\n取得した合計ページ数: {len(all_notion_pages)}")

if __name__ == "__main__":
    debug_notion_fetch()

