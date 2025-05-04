#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import imaplib, base64, random, os, sys, json, time
from email import message_from_bytes
import pandas as pd
from tqdm import tqdm
import csv

# 環境変数から認証情報を取得（環境変数がない場合はデフォルト値を使用）
GMAIL_USER = os.environ.get('GMAIL_USER', 'notfolder@gmail.com')
APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', 'your_app_password')
NUM_SAMPLES = 50000  # 抽出するメール件数
BATCH_SIZE = 500  # 一度に処理するバッチサイズ
CHECKPOINT_FILE = 'email_checkpoint.json'  # チェックポイントファイル
OUTPUT_FILE = 'emails.csv'  # 出力ファイル

def save_checkpoint(processed_ids, sampled_ids):
    """進捗状況をチェックポイントファイルに保存"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed_ids': [id.decode() if isinstance(id, bytes) else id for id in processed_ids],
            'sampled_ids': [id.decode() if isinstance(id, bytes) else id for id in sampled_ids],
            'timestamp': time.time()
        }, f)
    print(f"チェックポイントを保存しました: {len(processed_ids)} / {len(sampled_ids)} 件処理済み")

def load_checkpoint():
    """チェックポイントファイルから進捗状況を読み込む"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                print(f"チェックポイントを読み込みました: {len(data['processed_ids'])} 件処理済み")
                # バイト列に戻す
                processed_ids = [id.encode() if isinstance(id, str) else id for id in data['processed_ids']]
                sampled_ids = [id.encode() if isinstance(id, str) else id for id in data['sampled_ids']]
                return processed_ids, sampled_ids
        except Exception as e:
            print(f"チェックポイントの読み込みに失敗しました: {e}")
    return [], []

def write_csv_header():
    """CSVファイルのヘッダーを書き込む"""
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['message_id', 'body'])

def append_to_csv(records):
    """レコードをCSVファイルに追記する"""
    mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
    with open(OUTPUT_FILE, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if mode == 'w':
            writer.writerow(['message_id', 'body'])
        for record in records:
            writer.writerow([record['message_id'], record['body']])

def get_connection():
    """IMAP接続を確立して返す"""
    print("Gmailに接続しています...")
    imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
    imap.login(GMAIL_USER, APP_PASSWORD)
    imap.select('INBOX')
    print("接続成功しました")
    return imap

def main():
    # 既存のチェックポイントを読み込む
    processed_ids, sampled_ids = load_checkpoint()
    
    # 初回実行時またはすべて処理完了している場合は新しいサンプルを選ぶ
    if not sampled_ids:
        try:
            # 1) IMAP 接続
            imap = get_connection()
            
            # 2) メッセージ ID 取得 → ランダム抽出
            print("メールIDを取得しています...")
            status, data = imap.search(None, 'ALL')
            all_ids = data[0].split()
            
            sample_count = min(NUM_SAMPLES, len(all_ids))
            print(f"合計 {len(all_ids)} 件のメールから {sample_count} 件をランダム抽出します")
            sampled_ids = random.sample(all_ids, sample_count)
            
            # CSVファイルの初期化
            write_csv_header()
            
            # チェックポイントを保存
            save_checkpoint(processed_ids, sampled_ids)
            
            imap.close()
            imap.logout()
        except imaplib.IMAP4.error as e:
            print(f"認証エラー: {e}")
            print("環境変数が設定されていない場合は、以下のコマンドで設定してください:")
            print("export GMAIL_USER='yourname@gmail.com'")
            print("export GMAIL_APP_PASSWORD='your_app_password'")
            sys.exit(1)
        except Exception as e:
            print(f"初期化中にエラーが発生しました: {e}")
            sys.exit(1)
    
    # 未処理のメールIDを特定
    remaining_ids = [id for id in sampled_ids if id not in processed_ids]
    
    if not remaining_ids:
        print("すべてのメールの処理が完了しています。")
        return
    
    print(f"残り {len(remaining_ids)} 件のメールを処理します")
    
    # バッチ処理のメインループ
    batch_start = 0
    
    while batch_start < len(remaining_ids):
        try:
            # IMAPに接続
            imap = get_connection()
            
            # 現在のバッチを取得
            batch_end = min(batch_start + BATCH_SIZE, len(remaining_ids))
            current_batch = remaining_ids[batch_start:batch_end]
            
            print(f"バッチを処理中: {batch_start+1} から {batch_end} / {len(remaining_ids)}")
            
            # 3) 本文フェッチ & デコード
            batch_records = []
            
            # tqdmで進捗バーを表示
            for msg_id in tqdm(current_batch, desc="メール取得中", unit="件"):
                try:
                    _, msg_data = imap.fetch(msg_id, '(RFC822)')
                    raw = msg_data[0][1]
                    msg = message_from_bytes(raw)
                    
                    # プレーンテキスト部抽出
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == 'text/plain':
                                body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                                break
                    else:
                        body = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='ignore')
                    
                    batch_records.append({
                        'message_id': msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                        'body': body
                    })
                    processed_ids.append(msg_id)
                except Exception as e:
                    print(f"メールID {msg_id} の処理中にエラー: {e}")
            
            # 4) バッチをCSVに追記
            print(f"バッチデータをCSVファイルに追記しています...")
            append_to_csv(batch_records)
            
            # チェックポイントを更新
            save_checkpoint(processed_ids, sampled_ids)
            
            # 接続をクローズ
            imap.close()
            imap.logout()
            
            # 次のバッチへ
            batch_start = batch_end
            
            # バッチ間に短い休止を入れる（レート制限対策）
            if batch_start < len(remaining_ids):
                print("次のバッチ処理のために10秒間休止します...")
                time.sleep(10)
        
        except imaplib.IMAP4.error as e:
            print(f"IMAP接続エラー: {e}")
            print("30秒後に再接続を試みます...")
            time.sleep(30)
        except Exception as e:
            print(f"バッチ処理中にエラーが発生しました: {e}")
            # チェックポイントを保存して続行可能にする
            save_checkpoint(processed_ids, sampled_ids)
            print("30秒後に処理を再開します...")
            time.sleep(30)

    # 全バッチの処理完了
    print(f"すべての処理が完了しました。合計 {len(processed_ids)} 件のメールを処理しました。")
    print(f"結果は {OUTPUT_FILE} に保存されています。")
    
    # チェックポイントファイルを削除（オプション）
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("チェックポイントファイルを削除しました。")

if __name__ == '__main__':
    main()
