#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import random
import pickle
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
from email import message_from_bytes
import io

# OAuth 2.0認証の設定
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
NUM_SAMPLES = 50  # 抽出するメール件数

def get_gmail_service():
    """Gmail APIのサービスオブジェクトを取得する"""
    creds = None
    # トークンがあれば読み込む
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # 有効な認証情報がなければ新たに取得
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # client_secrets.jsonは、Google Cloud ConsoleからダウンロードしたOAuth 2.0クライアント情報
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # 次回のために認証情報を保存
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('gmail', 'v1', credentials=creds)

def main():
    # Gmail APIサービスの取得
    service = get_gmail_service()
    
    try:
        # 1) メッセージIDのリストを取得
        results = service.users().messages().list(userId='me', maxResults=500).execute()
        messages = results.get('messages', [])
        
        if not messages:
            print('No messages found.')
            return
        
        # 2) ランダムにメッセージを選択
        sampled_messages = random.sample(messages, min(NUM_SAMPLES, len(messages)))
        
        # 3) メッセージの内容を取得してデコード
        records = []
        for message in sampled_messages:
            msg_id = message['id']
            msg = service.users().messages().get(userId='me', id=msg_id, format='raw').execute()
            
            # Base64 URLエンコードされたメッセージをデコード
            msg_bytes = base64.urlsafe_b64decode(msg['raw'])
            mime_msg = message_from_bytes(msg_bytes)
            
            # プレーンテキスト部抽出
            if mime_msg.is_multipart():
                for part in mime_msg.walk():
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                        break
                else:
                    body = ""  # プレーンテキストが見つからない場合
            else:
                body = mime_msg.get_payload(decode=True).decode(mime_msg.get_content_charset() or 'utf-8', errors='ignore')
            
            records.append({'message_id': msg_id, 'body': body})
        
        # 4) CSV保存
        df = pd.DataFrame(records)
        df.to_csv('emails.csv', index=False, encoding='utf-8')
        print(f"Saved {len(records)} emails to emails.csv")
    
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == '__main__':
    main()
