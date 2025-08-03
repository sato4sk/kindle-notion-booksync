import os
import pandas as pd
from notion_client import Client
from dotenv import load_dotenv
from .client import register_book_to_notion_page
from .data_fetcher import get_existing_asins, get_notion_select_options
from .api_integrations import get_book_info_from_google_books, select_properties_with_gemini

def register_kindle_data_to_notion(kindle_df: pd.DataFrame, limit=None):
    """書籍データを処理し、Notionへの登録を行う。"""
    load_dotenv(override=True)

    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')
    google_api_key = os.getenv('GOOGLE_BOOKS_API_KEY')
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    

    if not notion_token:
        print("エラー: NOTION_API_TOKEN が設定されていません。")
        return

    notion = Client(auth=notion_token)

    if not database_id:
        print("エラー: NOTION_DB_ID が設定されていません。")
        return

    if not all([google_api_key, gemini_api_key]):
        print("エラー: GOOGLE_BOOKS_API_KEY または GEMINI_API_KEY が設定されていません。")
        return
    
    print("Notionから既存の書籍情報を取得しています...")
    existing_asins = get_existing_asins(notion, database_id)
    print(f"{len(existing_asins)}件の既存書籍が見つかりました。")

    print("Notionからプロパティ情報を取得しています...\n")
    tags_list = get_notion_select_options(notion, database_id, 'タグ')
    types_list = get_notion_select_options(notion, database_id, '種別')
    
    if not tags_list:
        print("警告: 'タグ' プロパティの選択肢が取得できませんでした。データベースが新規作成されたばかりの場合、これは正常です。")
    if not types_list:
        print("警告: '種別' プロパティの選択肢が取得できませんでした。データベースが新規作成されたばかりの場合、これは正常です。")

    if limit is not None:
        df = kindle_df.head(limit)
    else:
        df = kindle_df

    print("\n書籍情報を処理し、Notionに登録します...")
    for _, row in df.iterrows():
        title = row['title']
        asin = row.get('asin')
        print(f"\n--- 処理中の書籍: {title} (ASIN: {asin}) ---")

        if asin in existing_asins:
            print("-> この書籍は既にNotionに存在するため、スキップします。")
            continue

        book_description = get_book_info_from_google_books(google_api_key, title)

        selected_tags, selected_type = [], None
        print("書籍情報からタグと種別を選定中...")
        if book_description:
            print("  - 書籍概要が見つかりました。概要を基に選定します。")
        else:
            print("  - 書籍概要が見つかりませんでした。タイトルを基にWeb検索で選定します。")

        if tags_list and types_list:
            selected_tags, selected_type = select_properties_with_gemini(
                api_key=gemini_api_key,
                title=title,
                tags_list=tags_list,
                types_list=types_list,
                description=book_description
            )
            print(f"  - 選定されたタグ: {selected_tags}")
            print(f"  - 選定された種別: {selected_type}")
        else:
            print("  - タグまたは種別の選択肢が利用できないため、選定をスキップします。")

        register_book_to_notion_page(notion, database_id, row, book_description, selected_tags, selected_type)


if __name__ == "__main__":
    # テスト用のダミーデータフレームを作成
    dummy_data = {
        'title': ['テスト書籍1', 'テスト書籍2', 'テスト書籍3'],
        'author': ['著者A', '著者B', '著者C'],
        'publisher': ['出版社X', '出版社Y', '出版社Z'],
        'asin': ['ASIN001', 'ASIN002', 'ASIN003'],
        'content_tag': ['小説', '技術書', 'ビジネス'],
        'purchase_date': ['2023-01-01', '2023-02-15', '2023-03-20']
    }
    dummy_df = pd.DataFrame(dummy_data)
    
    # 環境変数をロード
    load_dotenv()

    # register_kindle_data_to_notion 関数を実行
    register_kindle_data_to_notion(dummy_df, limit=2)

    print("\nスクリプトの実行が完了しました。")