from extract_kindle_data import get_cleaned_kindle_data
from register_to_notion import register_kindle_data_to_notion

def main():
    print("Kindleデータ抽出とNotion登録を開始します。")
    
    # Kindleデータの抽出とクリーンアップ
    kindle_df = get_cleaned_kindle_data()

    if kindle_df is not None:
        # Notionへの登録
        register_kindle_data_to_notion(kindle_df)
    else:
        print("Kindleデータの取得に失敗したため、Notionへの登録をスキップします。")

if __name__ == "__main__":
    main()