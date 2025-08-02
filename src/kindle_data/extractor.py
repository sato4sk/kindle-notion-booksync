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
        return df
    except sqlite3.Error as e:
        print(f"SQLiteエラーが発生しました: {e}")
        return None
    finally:
        if conn:
            conn.close()