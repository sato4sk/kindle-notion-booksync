

import os
import pandas as pd
from notion_client import Client
from dotenv import load_dotenv

def update_asins_in_notion():
    """
    Notionデータベースの既存の書籍データに、ローカルのCSVファイルを元にASINを追記する。
    タイトルで書籍をマッチングし、ASINが空の場合のみ更新する。
    """
    load_dotenv(override=True)

    # --- 1. 環境変数の読み込みとクライアントの初期化 ---
    notion_token = os.getenv('NOTION_API_TOKEN')
    database_id = os.getenv('NOTION_DB_ID')

    if not notion_token or not database_id:
        print("エラー: NOTION_API_TOKEN または NOTION_DB_ID が.envファイルに設定されていません。")
        return

    notion = Client(auth=notion_token)

    # --- 2. ローカルデータの読み込みとASIN対応表の作成 ---
    print("ローカルの書籍データ(cleaned_result.csv)を読み込んでいます...")
    try:
        df = pd.read_csv("cleaned_result.csv")
        # ASINが空でないデータのみで、{タイトル: ASIN} の辞書を作成
        title_to_asin_map = df.dropna(subset=['asin']).set_index('title')['asin'].to_dict()
        print(f"{len(title_to_asin_map)}件の書籍データをローカルから読み込みました。")
    except FileNotFoundError:
        print("エラー: cleaned_result.csv が見つかりません。処理を中断します。")
        return

    # --- 3. Notionの全ページを取得 ---
    print("Notionデータベースから既存の全書籍データを取得しています...")
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
            print(f"Notionからのデータ取得中にエラーが発生しました: {e}")
            return
    print(f"{len(all_notion_pages)}件の書籍データをNotionから取得しました。")

    # --- 4. タイトルでのマッチングとASINの更新処理 ---
    print("\nASINの更新処理を開始します...")
    updated_count = 0
    for page in all_notion_pages:
        page_id = page['id']
        properties = page.get("properties", {})
        
        # Notionページのタイトルを取得
        title_property = properties.get('タイトル', {}).get('title', [])
        if not title_property:
            continue
        notion_title = title_property[0].get('plain_text')

        # NotionページのASINが空かどうかを確認
        asin_property = properties.get('ASIN', {}).get('rich_text', [])
        is_asin_empty = not asin_property

        # タイトルがローカルデータに存在し、かつNotionのASINが空の場合のみ更新
        if notion_title in title_to_asin_map and is_asin_empty:
            asin_to_update = str(title_to_asin_map[notion_title])
            print(f"- '{notion_title}': ASINが空です。ローカルのASIN '{asin_to_update}' で更新します。")
            
            try:
                notion.pages.update(
                    page_id=page_id,
                    properties={
                        "ASIN": {
                            "rich_text": [{
                                "text": {"content": asin_to_update}
                            }]
                        }
                    }
                )
                updated_count += 1
            except Exception as e:
                print(f"  -> 更新中にエラーが発生しました: {e}")
        else:
            if not is_asin_empty:
                # print(f"- '{notion_title}': 既にASINが登録済みのためスキップします。")
                pass
            else:
                # print(f"- '{notion_title}': ローカルに一致する書籍が見つからないためスキップします。")
                pass

    print(f"\n処理が完了しました。{updated_count}件の書籍のASINを更新しました。")

if __name__ == "__main__":
    update_asins_in_notion()

