
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

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def get_book_info_from_google_books(api_key, title):
    """Google Books APIから書籍の概要を取得する。"""
    if not api_key:
        return None
    url = f"https://www.googleapis.com/books/v1/volumes?q=intitle:{title}&key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data.get("totalItems", 0) > 0:
            return data["items"][0].get("volumeInfo", {}).get("description")
    except requests.exceptions.RequestException as e:
        print(f"Google Books APIへのリクエスト中にエラー: {e}")
        raise # tenacityでリトライさせるために再raise
    except Exception as e:
        print(f"Google Books APIからのデータ処理中に予期せぬエラー: {e}")
        return None

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(Exception) # Gemini APIの特定のエラータイプがないため、一般的なExceptionを捕捉
)
def select_properties_with_gemini(api_key, description, tags_list, types_list):
    """Gemini APIを使用して、書籍概要に最適なタグと種別を選定する。"""
    if not api_key:
        return [], None

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-flash')
    prompt = f"""
    以下の書籍概要を分析し、2つのタスクを実行してください。
    1. 「タグリスト」の中から、概要に最も関連性の高いタグを0個から最大2個まで選んでください。
    2. 「種別リスト」の中から、概要に最も当てはまる種別を1つだけ選んでください。
    回答は、必ず以下のJSON形式で出力してください。
    {{"tags": ["選んだタグ1", "選んだタグ2"], "type": "選んだ種別"}}
    もし適切なタグがない場合は、"tags"を空のリスト `[]` にしてください。
    --- START OF DATA ---
    [書籍概要]
    {description}
    [タグリスト]
    {tags_list}
    [種別リスト]
    {types_list}
    --- END OF DATA ---
    """
    try:
        response = model.generate_content(prompt)
        json_response_str = response.text.strip().replace("```json", "").replace("```", "").strip()
        result = json.loads(json_response_str)
        return result.get("tags", []), result.get("type")
    except Exception as e:
        print(f"Gemini APIでのプロパティ選定中にエラー: {e}")
        raise # tenacityでリトライさせるために再raise

@retry(
    wait=wait_exponential(multiplier=1, min=4, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(APIResponseError)
)
def register_book_to_notion(notion_client, db_id, book_data, description, tags, book_type):
    """書籍データをNotionに登録する。"""
    properties = {
        "タイトル": {"title": [{"text": {"content": str(book_data.get("title", ""))}}]},
        "著者": {"rich_text": [{"text": {"content": str(book_data.get("author", ""))}}]},
        "出版社": {"rich_text": [{"text": {"content": str(book_data.get("publisher", ""))}}]},
        "ASIN": {"rich_text": [{"text": {"content": str(book_data.get("asin", ""))}}]}
    }
    if pd.notna(book_data.get("purchase_date")):
        properties["購入日"] = {"date": {"start": str(book_data["purchase_date"])}}
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

def create_notion_database(notion_client, parent_page_id, title="Kindle Books"):
    """指定されたプロパティを持つ新しいNotionデータベースを作成する。"""
    properties = {
        "タイトル": {"title": {}},
        "著者": {"rich_text": {}},
        "出版社": {"rich_text": {}},
        "ASIN": {"rich_text": {}},
        "購入日": {"date": {}},
        "タグ": {"multi_select": {"options": [
            {"name": "読書法", "color": "green"},
            {"name": "テスト", "color": "purple"},
            {"name": "データ分析", "color": "brown"},
            {"name": "デザイン", "color": "brown"},
            {"name": "web", "color": "blue"},
            {"name": "開発手法", "color": "pink"},
            {"name": "ネットワーク", "color": "gray"},
            {"name": "コーディング", "color": "orange"},
            {"name": "設計", "color": "red"},
            {"name": "DB", "color": "red"},
            {"name": "AWS", "color": "default"},
            {"name": "マネジメント", "color": "yellow"},
            {"name": "セキュリティ", "color": "orange"},
            {"name": "フロントエンド", "color": "orange"},
            {"name": "Javascript", "color": "brown"},
            {"name": "Python", "color": "purple"},
            {"name": "運用保守", "color": "orange"},
            {"name": "DevOps", "color": "blue"},
            {"name": "AI", "color": "blue"},
            {"name": "React", "color": "purple"},
            {"name": "資格", "color": "yellow"},
            {"name": "Java", "color": "red"},
            {"name": "git", "color": "pink"},
            {"name": "インフラ", "color": "brown"}
        ]}},
        "種別": {"select": {"options": [
            {"name": "ビジネス書", "color": "red"},
            {"name": "応用書", "color": "yellow"},
            {"name": "入門書", "color": "purple"},
            {"name": "教科書", "color": "blue"},
            {"name": "その他書籍", "color": "default"},
            {"name": "ポッドキャスト", "color": "default"},
            {"name": "学術誌", "color": "default"},
            {"name": "資格", "color": "green"}
        ]}},
        "評価": {"select": {"options": [
            {"name": "⭐️⭐️⭐️⭐️⭐️", "color": "default"},
            {"name": "⭐️⭐️⭐️⭐️", "color": "default"},
            {"name": "⭐️⭐️⭐️", "color": "default"},
            {"name": "⭐️⭐️", "color": "default"},
            {"name": "⭐️", "color": "default"},
            {"name": "TBD", "color": "default"}
        ]}},
        "ステータス": {"status": {}},
        "リンク": {"url": {}},
        "読了日": {"date": {}},
        "開始日": {"date": {}},
    }
    try:
        new_database = notion_client.databases.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            title=[{"type": "text", "text": {"content": title}}],
            properties=properties
        )
        print(f"新しいデータベース '{title}' が作成されました。ID: {new_database['id']}")
        return new_database['id']
    except APIResponseError as e:
        print(f"Notionデータベースの作成中にAPIエラーが発生しました: {e}")
        raise
    except Exception as e:
        print(f"Notionデータベースの作成中に予期せぬエラーが発生しました: {e}")
        raise

def register_kindle_data_to_notion(kindle_df: pd.DataFrame, limit=3):
    """書籍データを処理し、Notionへの登録を行う。"""
    load_dotenv(override=True)

    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')
    google_api_key = os.getenv('GOOGLE_BOOKS_API_KEY')
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    parent_page_id = os.getenv('NOTION_PARENT_PAGE_ID') # 親ページIDを取得

    if not notion_token:
        print("エラー: NOTION_API_TOKEN が設定されていません。")
        return

    notion = Client(auth=notion_token)

    if not database_id or database_id == "":
        print("NOTION_DB_ID が設定されていないため、新しいデータベースを作成します。")
        if not parent_page_id:
            print("エラー: 新しいデータベースを作成するには、NOTION_PARENT_PAGE_ID が.envファイルに設定されている必要があります。")
            print("    .envファイルに NOTION_PARENT_PAGE_ID を追加し、親となるページのIDを設定してください。")
            print("    例: NOTION_PARENT_PAGE_ID=\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"")
            return
        try:
            database_id = create_notion_database(notion, parent_page_id)
            print(f"新しいデータベースID: {database_id} を.envファイルの NOTION_DB_ID に設定してください。")
            return # 新規作成後は一度終了し、ユーザーにID設定を促す
        except Exception as e:
            print(f"データベースの新規作成に失敗しました: {e}")
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

    df = kindle_df.head(limit)

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
        if book_description:
            print("書籍概要からタグと種別を選定中...")
            if tags_list and types_list:
                selected_tags, selected_type = select_properties_with_gemini(gemini_api_key, book_description, tags_list, types_list)
                print(f"  - 選定されたタグ: {selected_tags}")
                print(f"  - 選定された種別: {selected_type}")
            else:
                print("タグまたは種別の選択肢が利用できないため、選定をスキップします。")
        else:
            print("書籍概要が取得できなかったため、タグと種別の選定はスキップします。")

        register_book_to_notion(notion, database_id, row, book_description, selected_tags, selected_type)


