
import os
import json
from notion_client import Client
from dotenv import load_dotenv

def inspect_notion_db():
    """
    Notionデータベースのプロパティ情報を取得して表示する。
    """
    load_dotenv()

    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')

    if not notion_token or not database_id:
        print("エラー: NOTION_API_TOKEN または NOTION_DB_ID が.envファイルに設定されていません。")
        return

    notion = Client(auth=notion_token)

    try:
        db_info = notion.databases.retrieve(database_id=database_id)
        # プロパティ情報のみをJSON形式で出力
        print(json.dumps(db_info['properties'], indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Notionデータベースの情報の取得中にエラーが発生しました: {e}")

if __name__ == "__main__":
    inspect_notion_db()
