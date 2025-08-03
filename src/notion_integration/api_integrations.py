import os
import json
import requests
import google.generativeai as genai
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

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
def select_properties_with_gemini(api_key, title, tags_list, types_list, description=None):
    """Gemini APIを使用して、書籍のタグと種別を選定する。概要がない場合はWeb検索を利用する。"""
    if not api_key:
        return [], None

    genai.configure(api_key=api_key)

    if description:
        # 概要がある場合は、Web検索を使わない
        model = genai.GenerativeModel(model_name='gemini-2.5-flash')
        prompt = f"""
        以下の書籍概要を分析し、2つのタスクを実行してください。
        1. 「タグリスト」の中から、概要に最も関連性の高いタグを0個から最大2個まで選んでください。
        2. 「種別リスト」の中から、概要に最も当てはまる種別を1つだけ選んでください。
        回答は、必ず以下のJSON形式で出力してください。
        {{"tags": ["選んだタグ1", "選んだタグ2"], "type": "選んだ種別"}}
        もし適切なタグがない場合は、"tags"を空のリスト `[]` にしてください。
        --- START OF DATA ---
        [書籍タイトル]
        {title}
        [書籍概要]
        {description}
        [タグリスト]
        {tags_list}
        [種別リスト]
        {types_list}
        --- END OF DATA ---
        """
    else:
        # 概要がない場合は、Web検索を有効にする
        model = genai.GenerativeModel(
            model_name='gemini-2.5-flash',
            tools=['google_search']
        )
        prompt = f"""
        以下の書籍情報に基づき、2つのタスクを実行してください。
        書籍の概要が提供されていない、または情報が不十分な場合は、書籍のタイトルを基にWebで検索して内容を把握してください。

        1. 「タグリスト」の中から、書籍の内容に最も関連性の高いタグを0個から最大2個まで選んでください。
        2. 「種別リスト」の中から、書籍の内容に最も当てはまる種別を1つだけ選んでください。

        回答は、必ず以下のJSON形式で出力してください。
        {{"tags": ["選んだタグ1", "選んだタグ2"], "type": "選んだ種別"}}

        もし適切なタグがない場合は、"tags"を空のリスト `[]` にしてください。

        --- START OF DATA ---
        [書籍タイトル]
        {title}
        [書籍概要]
        提供されていません。Webで検索してください。
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