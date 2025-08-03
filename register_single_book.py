import argparse
from src.notion_integration.registrar import (
    setup_notion_client_and_get_context,
    process_and_register_book
)
from src.notion_integration.data_fetcher import get_existing_titles

def main():
    """単一の書籍情報をコマンドライン引数から受け取り、Notionに登録する。"""
    parser = argparse.ArgumentParser(description="単一の書籍をNotionデータベースに登録します。")
    parser.add_argument("--title", type=str, required=True, help="書籍のタイトル")
    parser.add_argument("--author", type=str, help="書籍の著者")
    parser.add_argument("--asin", type=str, help="書籍のASIN")
    parser.add_argument("--publisher", type=str, help="出版社")
    parser.add_argument("--purchase_date", type=str, help="購入日 (YYYY-MM-DD)")

    args = parser.parse_args()

    try:
        # 1. 共通のセットアップ処理を呼び出す
        (
            notion,
            database_id,
            api_keys,
            property_options,
            _ # existing_asins はここでは不要
        ) = setup_notion_client_and_get_context()

        # 2. タイトルでの重複チェック
        print("Notionから既存の書籍タイトルを取得して重複を確認しています...")
        existing_titles = get_existing_titles(notion, database_id)
        if args.title in existing_titles:
            print(f"-> 書籍「{args.title}」は既にNotionに存在するため、処理を中断します。")
            return
        print("-> この書籍は新規登録対象です。")

        # 3. コマンドライン引数を辞書にまとめる
        book_data = {
            "title": args.title,
            "author": args.author,
            "asin": args.asin,
            "publisher": args.publisher,
            "purchase_date": args.purchase_date,
            "content_tag": None,
        }

        # 4. コアロジックを呼び出す（重複チェックは責務外）
        process_and_register_book(
            notion=notion,
            database_id=database_id,
            book_data=book_data,
            api_keys=api_keys,
            property_options=property_options
        )

        print("\n登録処理が正常に完了しました。")

    except (ValueError, Exception) as e:
        print(f"\nエラーが発生しました: {e}")

if __name__ == "__main__":
    main()
