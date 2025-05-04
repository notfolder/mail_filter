#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import imaplib, base64, random, os, sys
from email import message_from_bytes
import pandas as pd
from tqdm import tqdm  # 進捗表示のためのライブラリを追加

# 環境変数から認証情報を取得（環境変数がない場合はデフォルト値を使用）
GMAIL_USER = os.environ.get('GMAIL_USER', 'notfolder@gmail.com')
APP_PASSWORD = os.environ.get('GMAIL_APP_PASSWORD', 'your_app_password')
NUM_SAMPLES = 100000  # 抽出するメール件数

def main():
    # 1) IMAP 接続
    try:
        print("Gmailに接続しています...")
        imap = imaplib.IMAP4_SSL('imap.gmail.com', 993)
        imap.login(GMAIL_USER, APP_PASSWORD)
        imap.select('INBOX')
        print("接続成功しました")
    except imaplib.IMAP4.error as e:
        print(f"認証エラー: {e}")
        print("環境変数が設定されていない場合は、以下のコマンドで設定してください:")
        print("export GMAIL_USER='yourname@gmail.com'")
        print("export GMAIL_APP_PASSWORD='your_app_password'")
        sys.exit(1)

    try:
        # 2) メッセージ ID 取得 → ランダム抽出
        print("メールIDを取得しています...")
        status, data = imap.search(None, 'ALL')
        all_ids = data[0].split()
        print(f"合計 {len(all_ids)} 件のメールから {min(NUM_SAMPLES, len(all_ids))} 件をランダム抽出します")
        sampled_ids = random.sample(all_ids, min(NUM_SAMPLES, len(all_ids)))

        # 3) 本文フェッチ & デコード
        records = []
        # tqdmで進捗バーを表示
        for msg_id in tqdm(sampled_ids, desc="メール取得中", unit="件"):
            _, msg_data = imap.fetch(msg_id, '(RFC822)')
            raw = msg_data[0][1]
            msg = message_from_bytes(raw)
            # プレーンテキスト部抽出
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                        break
                else:
                    body = ""  # プレーンテキストが見つからない場合
            else:
                body = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='ignore')
            records.append({'message_id': msg_id.decode(), 'body': body})

        # 4) CSV 保存
        print("CSVファイルに保存しています...")
        df = pd.DataFrame(records)
        df.to_csv('emails.csv', index=False, encoding='utf-8')
        print(f"Saved {len(records)} emails to emails.csv")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        # 5) クリーンアップ
        print("接続をクローズしています...")
        imap.close()
        imap.logout()

if __name__ == '__main__':
    main()
