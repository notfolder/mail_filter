#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import pandas as pd
import requests
from tqdm import tqdm
import sys

# — 設定 —  
LM_STUDIO_URL   = os.getenv("LM_STUDIO_URL", "http://127.0.0.1:1234")
LM_STUDIO_MODEL = os.getenv("LM_STUDIO_MODEL", "qwen3-30b-a3b-mlx")
INPUT_CSV       = "emails.csv"
OUTPUT_JSONL    = "labeled_emails_lmstudio.jsonl"
CHECKPOINT_FILE = "annotation_checkpoint.json"
BATCH_SIZE      = 100  # チェックポイント保存間隔（処理件数）
ERROR_WAIT_TIME = 10   # エラー発生時の待機秒数

PROMPT_HEADER = """\
Read the following email body and return an importance score (1–5) and the reason \
(within 50 characters) in JSON format.

Classification criteria:
* 5: Urgent + financial matters
* 4: Urgent or financial matters
* 3: Important but not urgent communication
* 2: Informational or promotional content
* 1: Spam, phishing, or unknown sender

Output format (JSON only):
{"importance":<1–5>,"reason":"<reason within 50 characters>"}

Email body:
"""

def save_checkpoint(processed_ids):
    """処理済みIDをチェックポイントファイルに保存"""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'processed_ids': processed_ids,
            'count': len(processed_ids),
            'timestamp': time.time()
        }, f, ensure_ascii=False)
    print(f"チェックポイントを保存しました: {len(processed_ids)} 件処理済み")

def load_checkpoint():
    """チェックポイントファイルから処理済みIDを読み込む"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"チェックポイントを読み込みました: {data['count']} 件処理済み")
                return set(data['processed_ids'])
        except Exception as e:
            print(f"チェックポイントの読み込みに失敗しました: {e}")
    return set()

def annotate():
    # CSVの読み込み
    print(f"{INPUT_CSV} を読み込んでいます...")
    df = pd.read_csv(INPUT_CSV)
    if "body" not in df.columns or "message_id" not in df.columns:
        raise ValueError("CSV に必要なカラム (body, message_id) が存在しません")
    
    total_emails = len(df)
    print(f"合計 {total_emails} 件のメールを処理します")
    
    # チェックポイントの読み込み
    processed_ids = load_checkpoint()
    
    # 出力ファイルが存在しなければ新規作成、存在すれば追記モード
    file_mode = 'a' if os.path.exists(OUTPUT_JSONL) and processed_ids else 'w'
    
    # APIエンドポイントの設定
    endpoint = f"{LM_STUDIO_URL}/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    
    # 処理対象のレコードを特定（未処理のもののみ）
    remaining_df = df[~df['message_id'].astype(str).isin(processed_ids)]
    remaining_count = len(remaining_df)
    
    if remaining_count == 0:
        print("すべてのメールは既に処理済みです。")
        return
    
    print(f"残り {remaining_count} 件のメールを処理します")
    
    # 処理カウンタとバッチカウンタの初期化
    batch_counter = 0
    
    with open(OUTPUT_JSONL, file_mode, encoding='utf-8') as fout:
        # ヘッダーが必要な場合は書き込む（新規作成時のみ）
        if file_mode == 'w':
            print("新規ファイルを作成します")
        else:
            print("既存ファイルに追記します")
        
        # tqdmで進捗バーを表示
        with tqdm(total=remaining_count, desc="アノテーション中") as pbar:
            for idx, row in remaining_df.iterrows():
                try:
                    message_id = str(row['message_id'])
                    body = row['body']
                    
                    if not isinstance(body, str):
                        print(f"警告: message_id={message_id} の本文が文字列ではありません。スキップします。")
                        processed_ids.add(message_id)
                        pbar.update(1)
                        continue
                    
                    # API呼び出し
                    payload = {
                        "model": LM_STUDIO_MODEL,
                        "messages": [
                            {"role": "system", "content": ""},
                            {"role": "user",   "content": PROMPT_HEADER + body.strip()}
                        ],
                        "temperature": 0.0,
                        "max_tokens": 60
                    }
                    
                    # リトライ機能付きAPIリクエスト
                    max_retries = 3
                    retry_count = 0
                    while retry_count < max_retries:
                        try:
                            resp = requests.post(endpoint, headers=headers, json=payload, timeout=100)
                            resp.raise_for_status()
                            break
                        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                            retry_count += 1
                            if retry_count >= max_retries:
                                raise
                            wait_time = ERROR_WAIT_TIME * retry_count
                            print(f"API呼び出しエラー: {e}. {wait_time}秒後に再試行 ({retry_count}/{max_retries})")
                            time.sleep(wait_time)
                    
                    data = resp.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    
                    # JSON形式にパース（エラーハンドリング付き）
                    try:
                        obj = json.loads(text)
                    except json.JSONDecodeError:
                        # JSON解析エラー時のリトライ処理
                        parse_retries = 0
                        max_parse_retries = 3
                        parse_success = False
                        
                        while parse_retries < max_parse_retries:
                            try:
                                # 改行を削除して再試行
                                cleaned_text = text.replace("\n", "")
                                # 先頭と末尾の不要な文字を削除して再試行
                                cleaned_text = cleaned_text.strip('`').strip()
                                # JSONの前後にある不要なテキストを削除
                                if '{' in cleaned_text and '}' in cleaned_text:
                                    start_idx = cleaned_text.find('{')
                                    end_idx = cleaned_text.rfind('}') + 1
                                    cleaned_text = cleaned_text[start_idx:end_idx]
                                
                                obj = json.loads(cleaned_text)
                                parse_success = True
                                print(f"JSON解析リトライ成功 ({parse_retries+1}回目)")
                                break
                            except json.JSONDecodeError:
                                parse_retries += 1
                                print(f"JSON解析リトライ失敗 ({parse_retries}/{max_parse_retries}): {text[:50]}...")
                                time.sleep(1)  # 短い待機
                        
                        # すべてのリトライが失敗した場合
                        if not parse_success:
                            print(f"警告: JSON解析がすべて失敗しました: {text[:100]}...")
                            obj = {"importance": -1, "reason": "JSONパース失敗"}
                    
                    # レコードの作成と書き込み
                    record = {
                        "message_id": message_id,
                        "email_body": body[:500] + ("..." if len(body) > 500 else ""),  # 長すぎるボディを切り詰め
                        "importance": obj.get("importance", 0),
                        "reason": obj.get("reason", "不明")
                    }
                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                    fout.flush()  # 確実にディスクに書き込む
                    
                    # 処理済みIDを記録
                    processed_ids.add(message_id)
                    
                    # 進捗バーを更新
                    pbar.update(1)
                    pbar.set_postfix(importance=record['importance'])
                    
                    # バッチカウンタを更新
                    batch_counter += 1
                    
                    # BATCH_SIZE毎にチェックポイントを保存
                    if batch_counter >= BATCH_SIZE:
                        save_checkpoint(list(processed_ids))
                        batch_counter = 0
                    
                    # APIのレート制限対策に短い待機を入れる（必要に応じて調整）
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"\nエラー発生 (message_id={row.get('message_id', 'unknown')}): {e}")
                    print(f"{ERROR_WAIT_TIME}秒後に次のメールの処理を続行します...")
                    time.sleep(ERROR_WAIT_TIME)
                    pbar.update(1)
    
    # 最終チェックポイントの保存
    save_checkpoint(list(processed_ids))
    
    print(f"アノテーション完了：{len(processed_ids)} 件を {OUTPUT_JSONL} に保存しました")

if __name__ == "__main__":
    try:
        annotate()
    except KeyboardInterrupt:
        print("\n処理が中断されました。次回実行時にチェックポイントから再開できます。")
        sys.exit(1)
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        sys.exit(1)
