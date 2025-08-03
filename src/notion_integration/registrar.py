import os
import pandas as pd
from notion_client import Client
from dotenv import load_dotenv
from .client import register_book_to_notion_page
from .data_fetcher import get_existing_asins, get_notion_select_options
from .api_integrations import get_book_info_from_google_books, select_properties_with_gemini

def setup_notion_client_and_get_context():
    """環境変数を読み込み、Notionクライアントと登録に必要なコンテキスト情報を準備する。"""
    load_dotenv(override=True)

    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')
    google_api_key = os.getenv('GOOGLE_BOOKS_API_KEY')
    gemini_api_key = os.getenv('GEMINI_API_KEY')

    if not all([notion_token, database_id, google_api_key, gemini_api_key]):
        raise ValueError("必要な環境変数 (NOTION_API_TOKEN, NOTION_DB_ID, GOOGLE_BOOKS_API_KEY, GEMINI_API_KEY) が設定されていません。")

    notion = Client(auth=notion_token)

    print("Notionから既存の書籍情報を取得しています...")
    existing_asins = get_existing_asins(notion, database_id)
    print(f"{len(existing_asins)}件の既存書籍が見つかりました。")

    print("Notionからプロパティ情報を取得しています...")
    tags_list = get_notion_select_options(notion, database_id, 'タグ')
    types_list = get_notion_select_options(notion, database_id, '種別')
    if not tags_list or not types_list:
        print("警告: 'タグ' または '種別' の選択肢が取得できませんでした。")

    api_keys = {'google': google_api_key, 'gemini': gemini_api_key}
    property_options = {'tags': tags_list, 'types': types_list}

    return notion, database_id, api_keys, property_options, existing_asins

def process_and_register_book(notion, database_id, book_data, api_keys, property_options):
    """（重複チェックなし）一冊の書籍データを処理し、Notionに登録する。"""
    title = book_data['title']
    print(f"\n--- 処理中の書籍: {title} ---")

    # Google Books APIから情報を取得
    volume_info = get_book_info_from_google_books(api_keys['google'], title)
    book_description = None

    if volume_info:
        print("  - Google Books APIから書籍情報を取得しました。")
        book_description = volume_info.get('description')
        if book_description:
            print(f"    - 概要: {book_description[:100]}...")

        # 入力されなかった情報をAPIからの情報で補完（エンリッチ）
        if not book_data.get('author') and volume_info.get('authors'):
            book_data['author'] = ", ".join(volume_info['authors'])
            print(f"    - 著者を補完しました: {book_data['author']}")
        if not book_data.get('publisher') and volume_info.get('publisher'):
            book_data['publisher'] = volume_info['publisher']
            print(f"    - 出版社を補完しました: {book_data['publisher']}")
    else:
        print("  - Google Books APIで書籍情報が見つかりませんでした。")

    print("\n書籍情報からタグと種別を選定中...")
    if property_options['tags'] and property_options['types']:
        selected_tags, selected_type = select_properties_with_gemini(
            api_key=api_keys['gemini'],
            title=title,
            tags_list=property_options['tags'],
            types_list=property_options['types'],
            description=book_description
        )
        print(f"  - 選定されたタグ: {selected_tags}")
        print(f"  - 選定された種別: {selected_type}")
    else:
        selected_tags, selected_type = [], None
        print("  - タグまたは種別の選択肢が利用できないため、選定をスキップします。")

    # 補完された可能性のあるbook_dataを渡す
    register_book_to_notion_page(notion, database_id, book_data, book_description, selected_tags, selected_type)

def register_kindle_data_to_notion(kindle_df: pd.DataFrame, limit=None):
    """Kindleの書籍データフレームを処理し、Notionへの一括登録を行う。"""
    try:
        (
            notion,
            database_id,
            api_keys,
            property_options,
            existing_asins
        ) = setup_notion_client_and_get_context()

        if limit is not None:
            df = kindle_df.head(limit)
        else:
            df = kindle_df

        print("\n書籍情報を一括処理し、Notionに登録します...")
        for _, row in df.iterrows():
            book_data = row.to_dict()
            asin = book_data.get('asin')

            # ASINでの重複チェックをここで行う
            if asin and asin in existing_asins:
                print(f"-> 書籍「{book_data['title']}」(ASIN: {asin})は既に存在するため、スキップします。")
                continue

            process_and_register_book(
                notion,
                database_id,
                book_data,
                api_keys,
                property_options
            )
        print("\n一括登録処理が完了しました。")

    except (ValueError, Exception) as e:
        print(f"\nエラーが発生しました: {e}")


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
    
    register_kindle_data_to_notion(dummy_df, limit=2)
