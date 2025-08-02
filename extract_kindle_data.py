import sqlite3
import pandas as pd
import os
import plistlib
import numpy as np
from typing import Any, Dict, Union

def resolve_ns_keyed_archive_fully(data: bytes) -> Any:
    """
    ZSYNCMETADATAATTRIBUTESカラムのplistデータを解析する。

    Args:
        data: バイナリ形式のplistデータ

    Returns:
        Any: 解析されたデータ
    """
    if pd.isna(data):
        return np.nan

    try:
        root = plistlib.loads(data)
        objects = root["$objects"]
        top_uid = root["$top"]["root"]

        # クラスID → クラス名
        class_map = {
            idx: obj["$classname"] for idx, obj in enumerate(objects) if isinstance(obj, dict) and "$classname" in obj
        }

        def resolve(obj: Any, memo: Dict[int, Any]) -> Any:
            if isinstance(obj, plistlib.UID):
                idx = obj.data
                if idx in memo:
                    return memo[idx]
                raw = objects[idx]
                resolved = resolve(raw, memo)
                memo[idx] = resolved
                return resolved

            elif isinstance(obj, list):
                return [resolve(item, memo) for item in obj]

            elif isinstance(obj, dict):
                # クラスID に基づいて判定
                class_id = obj.get("$class")
                class_name = class_map.get(class_id.data) if isinstance(class_id, plistlib.UID) else None

                # NSMutableArray / NSArray の展開
                if class_name in ("NSMutableArray", "NSArray") and "NS.objects" in obj:
                    return resolve(obj["NS.objects"], memo)

                # NSMutableDictionary / NSDictionary の展開
                if class_name in ("NSMutableDictionary", "NSDictionary") and "NS.keys" in obj and "NS.objects" in obj:
                    keys = resolve(obj["NS.keys"], memo)
                    vals = resolve(obj["NS.objects"], memo)
                    return dict(zip(keys, vals))

                # 通常の辞書展開
                return {
                    resolve(k, memo): resolve(v, memo)
                    for k, v in obj.items()
                    if not (isinstance(k, str) and k.startswith("$"))
                }

            else:
                return obj

        return resolve(top_uid, {})
    except Exception as e:
        # print(f"Error parsing plist data: {e}") # デバッグ用
        return np.nan

def extract_metadata_attributes(book_df: pd.DataFrame) -> pd.DataFrame:
    """
    ZSYNCMETADATAATTRIBUTESカラムからメタデータを抽出する。

    Args:
        book_df: 書籍データのDataFrame

    Returns:
        pd.DataFrame: メタデータを含む拡張されたDataFrame
    """
    # ZSYNCMETADATAATTRIBUTESカラムがない場合は元のDataFrameを返す
    if "ZSYNCMETADATAATTRIBUTES" not in book_df.columns:
        print("ZSYNCMETADATAATTRIBUTES column not found in the DataFrame")
        return book_df

    # メタデータを解析
    book_df["metadata"] = book_df["ZSYNCMETADATAATTRIBUTES"].apply(resolve_ns_keyed_archive_fully)

    # 必要な情報を抽出
    def extract_attribute(row, attr_path):
        if pd.isna(row["metadata"]):
            return np.nan

        try:
            value = row["metadata"]
            for key in attr_path:
                if key in value:
                    value = value[key]
                else:
                    return np.nan

            # リスト型の値を文字列に変換
            if isinstance(value, list):
                return ", ".join(str(item) for item in value)

            return value
        except (KeyError, TypeError, ValueError, AttributeError):
            return np.nan

    # 著者情報を抽出
    book_df["author"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "authors", "author"]), axis=1)

    # 出版社情報を抽出
    book_df["publisher"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "publishers", "publisher"]), axis=1)

    # タイトル情報を抽出
    book_df["title_from_metadata"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "title"]), axis=1)

    # ASIN情報を抽出
    book_df["asin"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "ASIN"]), axis=1)

    # コンテンツタグ情報を抽出
    book_df["content_tag"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "content_tags", "tag"]), axis=1)

    # 購入日情報を抽出
    book_df["purchase_date"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "purchase_date"]), axis=1)

    # 出版日情報を抽出
    book_df["publication_date"] = book_df.apply(lambda row: extract_attribute(row, ["attributes", "publication_date"]), axis=1)

    return book_df

def extract_kindle_data(db_path):
    """
    KindleのSQLiteデータベースからZBOOKテーブルのレコードを抽出し、メタデータを抽出します。
    """
    if not os.path.exists(db_path):
        print(f"エラー: データベースファイルが見つかりません: {db_path}")
        return None

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        query = "SELECT * FROM ZBOOK"
        df = pd.read_sql_query(query, conn)
        df = extract_metadata_attributes(df) # メタデータ抽出を追加
        return df
    except sqlite3.Error as e:
        print(f"SQLiteエラーが発生しました: {e}")
        return None
    finally:
        if conn:
            conn.close()

from dotenv import load_dotenv

def get_cleaned_kindle_data():
    load_dotenv()

    db_file = "data/BookData.sqlite"
    script_dir = os.path.dirname(__file__)
    db_absolute_path = os.path.join(script_dir, db_file)

    print(f"データベースパス: {db_absolute_path}")

    kindle_df = extract_kindle_data(db_absolute_path)

    if kindle_df is None:
        print("データの取得に失敗しました。")
        return None

    exclude_tags_str = os.getenv('EXCLUDE_CONTENT_TAGS', '')
    exclude_tags = [tag.strip() for tag in exclude_tags_str.split(',') if tag.strip()]
    purchase_date_since_str = os.getenv('PURCHASE_DATE_SINCE')

    columns_to_drop = ['ZDISPLAYAUTHOR', 'title_from_metadata', 'ZAUTHOR', 'ZPUBLISHER', 'metadata']
    df_cleaned = kindle_df.drop(columns=[col for col in columns_to_drop if col in kindle_df.columns], errors='ignore')

    df_cleaned = df_cleaned.rename(columns={
        'ZDISPLAYTITLE': 'title'
    })

    final_columns = [
        'title', 
        'author', 
        'publisher', 
        'asin', 
        'content_tag', 
        'purchase_date', 
        'publication_date'
    ]
    
    existing_final_columns = [col for col in final_columns if col in df_cleaned.columns]
    result_df = df_cleaned[existing_final_columns]
    
    if exclude_tags and 'content_tag' in result_df.columns:
        result_df = result_df[~result_df['content_tag'].fillna('').str.contains('|'.join(exclude_tags), na=False)]

    if purchase_date_since_str and 'purchase_date' in result_df.columns:
        result_df['purchase_date'] = pd.to_datetime(result_df['purchase_date'], errors='coerce')
        purchase_date_since = pd.to_datetime(purchase_date_since_str, utc=True)
        result_df = result_df[result_df['purchase_date'] >= purchase_date_since]

    print("フィルタリング・クリーンアップ後のKindle蔵書データ:")
    print(result_df)
    print(f"合計レコード数: {len(result_df)}")
    
    return result_df

if __name__ == "__main__":
    # このスクリプトを直接実行した場合のテスト用
    cleaned_data = get_cleaned_kindle_data()
    if cleaned_data is not None:
        output_csv_path = "cleaned_result.csv"
        cleaned_data.to_csv(output_csv_path, index=False)
        print(f"\nクリーンアップされたデータを'{output_csv_path}'に保存しました。")
