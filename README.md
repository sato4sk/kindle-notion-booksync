# Kindle to Notion Sync

このリポジトリは、Kindleの蔵書データを抽出し、整形してNotionデータベースに登録するためのPythonスクリプトを提供します。Google Books APIとGemini APIを利用して、書籍の概要取得とNotionプロパティの自動選定を行います。

## 役割

*   `data/BookData.sqlite`に保存されているKindleの蔵書データを読み込みます。
*   書籍データから著者、出版社、ASINなどのメタデータを抽出・整形します。
*   Google Books APIを使用して書籍の概要を取得します。
*   Gemini APIを使用して、書籍の概要に基づきNotionの「タグ」と「種別」プロパティを自動で選定します。
*   整形された書籍データをNotionデータベースに登録します。
*   既にNotionに登録されている書籍はスキップし、重複登録を防ぎます。

## 使用方法

### 1. Kindleデータの準備

本スクリプトは、Kindleアプリのデータベースファイル `BookData.sqlite` を読み込みます。このファイルはGitリポジトリには含まれていないため、ご自身で取得し、プロジェクトルートの `data/` ディレクトリに配置する必要があります。

1.  **`BookData.sqlite` の場所**:
    macOSの場合、通常以下のパスに存在します。
    `/Users/{YOUR USER}/Library/Containers/Kindle/Data/Library/Protected/BookData.sqlite`

2.  **ファイルの配置**:
    上記パスから `BookData.sqlite` ファイルをコピーし、本プロジェクトのルートディレクトリ直下にある `data/` ディレクトリに配置してください。
    例: `your_project_root/data/BookData.sqlite`

### 2. notionデータベースページの準備

本リポジトリはnotionにデータベースページが作成済であることが前提です。
以下のnotionページを複製して、ご自身のnotionにデータベースを準備してください。

https://www.notion.so/2435064144e780d5a3f6f0d29dfa4ea8?v=2435064144e78125a4a8000c4cbc05c3&source=copy_link

### 3. 環境設定

必要なAPIキーと設定値を `.env` ファイルに設定する必要があります。プロジェクトのルートディレクトリに `.env` ファイルを作成し、以下の情報を記述してください。

```
NOTION_API_TOKEN="your_notion_api_token"
NOTION_DB_ID="your_notion_database_id"
GOOGLE_BOOKS_API_KEY="your_google_books_api_key"
GEMINI_API_KEY="your_gemini_api_key"
EXCLUDE_CONTENT_TAGS="tag1,tag2" # オプション: 除外したいコンテンツタグをカンマ区切りで指定
PURCHASE_DATE_SINCE="YYYY-MM-DDTHH:MM:SSZ" # オプション: 指定した日付以降の購入日を持つ書籍のみを処理 (例: 2023-01-01T00:00:00Z)
```

*   **`NOTION_API_TOKEN`**: Notion APIと連携するためのトークンです。Notionのインテグレーション設定で取得できます。
*   **`NOTION_DB_ID`**: 書籍データを登録するNotionデータベースのIDです。NotionデータベースのURLから取得できます。
*   **`GOOGLE_BOOKS_API_KEY`**: Google Books APIを利用するためのAPIキーです。Google Cloud Consoleで取得できます。
*   **`GEMINI_API_KEY`**: Gemini APIを利用するためのAPIキーです。Google AI Studioで取得できます。
*   **`EXCLUDE_CONTENT_TAGS` (オプション)**: Kindleデータから除外したいコンテンツタグをカンマ区切りで指定します。
*   **`PURCHASE_DATE_SINCE` (オプション)**: 指定した日付以降に購入された書籍のみを処理する場合に設定します。ISO 8601形式 (`YYYY-MM-DDTHH:MM:SSZ`) で指定してください。

### 4. Pythonパッケージのインストール

このプロジェクトでは、Pythonパッケージマネージャーとして `uv` を使用しています。以下のコマンドで必要なパッケージをインストールしてください。

```bash
uv sync
```

### 5. スクリプトの実行

すべての設定が完了したら、以下のコマンドでスクリプトを実行します。

```bash
uv run main.py
```

このコマンドを実行すると、`data/BookData.sqlite` からKindleデータが抽出・整形され、Notionデータベースに登録されます。

### 注意事項

*   Notion APIのレート制限やGemini APIのクォータ制限に注意してください。特にGemini APIは無料枠に制限があるため、大量の書籍を一度に処理するとエラーになる可能性があります。その場合は、時間をおいて再試行するか、APIの利用状況を確認してください。

### 参考

Kindle蔵書リストのメタデータ解析コードは、以下のリポジトリを参考とさせていただきました。
https://github.com/karaage0703/kindle-analyzer?tab=readme-ov-file