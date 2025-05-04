#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import csv
import mailbox
import zipfile
import tempfile
import shutil
from email import message_from_bytes, policy
from tqdm import tqdm

# 設定
INPUT_ZIP_FILE = 'input.zip'  # 入力ZIP圧縮ファイル
MBOX_FILENAME = 'input.mbox'  # ZIP内のMBOXファイル名
OUTPUT_FILE = 'emails.csv'  # 出力CSVファイル
BATCH_SIZE = 1000  # 一度に処理するバッチサイズ
CHECKPOINT_FILE = 'mbox_checkpoint.json'  # チェックポイントファイル

def save_checkpoint(processed_count, total_count):
    """進捗状況をチェックポイントファイルに保存"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed_count': processed_count,
            'total_count': total_count,
            'timestamp': time.time()
        }, f)
    print(f"チェックポイントを保存しました: {processed_count} / {total_count} 件処理済み")

def load_checkpoint():
    """チェックポイントファイルから進捗状況を読み込む"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                print(f"チェックポイントを読み込みました: {data['processed_count']} 件処理済み")
                return data['processed_count']
        except Exception as e:
            print(f"チェックポイントの読み込みに失敗しました: {e}")
    return 0

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

def extract_text_from_message(msg):
    """メールメッセージからプレーンテキスト部分を抽出"""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == 'text/plain':
                try:
                    charset = part.get_content_charset() or 'utf-8'
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode(charset, errors='ignore')
                        break
                except Exception as e:
                    print(f"デコードエラー: {e}")
    else:
        try:
            charset = msg.get_content_charset() or 'utf-8'
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode(charset, errors='ignore')
        except Exception as e:
            print(f"デコードエラー: {e}")
    return body

def extract_mbox_from_zip(zip_path, temp_dir):
    """ZIPファイルからMBOXファイルを展開"""
    try:
        print(f"ZIPファイルを展開しています...")
        # ZIP64をサポートするために、zipfile.ZipFileを使用
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # ZIPファイル内のファイル一覧を表示
            file_list = zip_ref.namelist()
            print(f"ZIPファイル内のファイル: {file_list}")
            
            # MBOXファイルを探す
            mbox_file = None
            for file in file_list:
                if file.endswith('.mbox') or file == MBOX_FILENAME:
                    mbox_file = file
                    break
            
            if not mbox_file:
                print(f"エラー: ZIPファイル内にMBOXファイルが見つかりません")
                return None
            
            # MBOXファイルを一時ディレクトリに展開
            extract_path = os.path.join(temp_dir, os.path.basename(mbox_file))
            print(f"MBOXファイル '{mbox_file}' を展開しています...")
            
            # ファイルサイズを取得
            file_info = zip_ref.getinfo(mbox_file)
            total_size = file_info.file_size
            print(f"ファイルサイズ: {total_size / (1024*1024):.2f} MB")
            
            # 大きなファイルを展開するためのバッファ付き展開
            with zip_ref.open(mbox_file) as source, open(extract_path, 'wb') as target:
                chunk_size = 1024 * 1024  # 1MB
                with tqdm(total=total_size, unit='B', unit_scale=True, desc="ZIP展開中") as pbar:
                    while True:
                        chunk = source.read(chunk_size)
                        if not chunk:
                            break
                        target.write(chunk)
                        pbar.update(len(chunk))
            
            print(f"MBOXファイルを {extract_path} に展開しました")
            return extract_path
            
    except zipfile.BadZipFile:
        print(f"エラー: 無効なZIPファイルです")
        return None
    except Exception as e:
        print(f"ZIPファイル展開中にエラーが発生しました: {e}")
        return None

def count_messages(mbox_path):
    """MBOXファイル内のメッセージ数をカウント"""
    try:
        print(f"MBOXファイル内のメッセージ数をカウントしています...")
        mbox = mailbox.mbox(mbox_path)
        count = len(mbox)
        mbox.close()
        print(f"合計 {count} 件のメールが見つかりました")
        return count
    except Exception as e:
        print(f"メッセージカウント中にエラーが発生しました: {e}")
        return 0

def main():
    # 入力ファイルの存在確認
    if not os.path.exists(INPUT_ZIP_FILE):
        print(f"エラー: 入力ファイル '{INPUT_ZIP_FILE}' が見つかりません")
        print("このファイルパスを変更するか、同じディレクトリにZIPファイルを配置してください")
        sys.exit(1)
    
    # 一時ディレクトリを作成
    temp_dir = tempfile.mkdtemp()
    print(f"一時ディレクトリを作成しました: {temp_dir}")
    
    try:
        # ZIPファイルからMBOXを展開
        mbox_path = extract_mbox_from_zip(INPUT_ZIP_FILE, temp_dir)
        if not mbox_path:
            print("MBOXファイルの展開に失敗しました")
            sys.exit(1)
        
        # メッセージ総数を取得
        total_count = count_messages(mbox_path)
        if total_count == 0:
            print("処理するメッセージがありません")
            sys.exit(0)
        
        # チェックポイントから再開位置を読み込む
        processed_count = load_checkpoint()
        
        # 初回実行時はCSVヘッダーを書き込む
        if processed_count == 0:
            write_csv_header()
        
        # MBOXを開く
        mbox = mailbox.mbox(mbox_path)
        
        try:
            # 残りのメッセージ数を表示
            remaining_count = total_count - processed_count
            print(f"残り {remaining_count} 件のメールを処理します")
            
            # バッチ処理のメインループ
            batch_start = processed_count
            
            while batch_start < total_count:
                try:
                    # 現在のバッチ範囲を計算
                    batch_end = min(batch_start + BATCH_SIZE, total_count)
                    print(f"バッチを処理中: {batch_start+1} から {batch_end} / {total_count}")
                    
                    # バッチレコードを初期化
                    batch_records = []
                    
                    # バッチ内のメッセージを処理
                    with tqdm(total=batch_end-batch_start, desc="メール変換中", unit="件") as pbar:
                        for i in range(batch_start, batch_end):
                            try:
                                # MBOXからメッセージを取得
                                msg = mbox[i]
                                
                                # メッセージIDを取得（なければインデックスを使用）
                                msg_id = msg.get('Message-ID', f"no-id-{i}")
                                if msg_id is None:
                                    msg_id = f"no-id-{i}"
                                
                                # プレーンテキストを抽出
                                body = extract_text_from_message(msg)
                                
                                # レコードに追加
                                batch_records.append({
                                    'message_id': str(msg_id).strip('<>'),
                                    'body': body
                                })
                                
                                # 進捗バーを更新
                                pbar.update(1)
                                
                            except Exception as e:
                                print(f"メッセージ {i} の処理中にエラー: {e}")
                                pbar.update(1)
                    
                    # バッチをCSVに追記
                    print(f"バッチデータをCSVファイルに追記しています...")
                    append_to_csv(batch_records)
                    
                    # 処理カウントを更新
                    processed_count = batch_end
                    
                    # チェックポイントを保存
                    save_checkpoint(processed_count, total_count)
                    
                    # 次のバッチへ
                    batch_start = batch_end
                    
                    # バッチ間に短い休止を入れる
                    if batch_start < total_count:
                        print("次のバッチ処理のために5秒間休止します...")
                        time.sleep(5)
                    
                except Exception as e:
                    print(f"バッチ処理中にエラーが発生しました: {e}")
                    # チェックポイントを保存して続行可能にする
                    save_checkpoint(processed_count, total_count)
                    print("10秒後に処理を再開します...")
                    time.sleep(10)
            
            # 全バッチの処理完了
            print(f"すべての処理が完了しました。合計 {processed_count} 件のメールを処理しました。")
            print(f"結果は {OUTPUT_FILE} に保存されています。")
            
            # チェックポイントファイルを削除（オプション）
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                print("チェックポイントファイルを削除しました。")
        
        finally:
            # MBOXを閉じる
            mbox.close()
    
    finally:
        # 一時ディレクトリを削除
        print(f"一時ファイルをクリーンアップしています...")
        try:
            shutil.rmtree(temp_dir)
            print(f"一時ディレクトリを削除しました: {temp_dir}")
        except Exception as e:
            print(f"一時ディレクトリの削除中にエラーが発生しました: {e}")

if __name__ == '__main__':
    main()