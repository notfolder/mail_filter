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
import glob
from email import message_from_bytes, policy
from tqdm import tqdm

# 設定
INPUT_ZIP_PATTERN = 'input*.zip'  # 入力ZIP圧縮ファイルのパターン
MBOX_FILENAME = 'input.mbox'  # ZIP内のMBOXファイル名
OUTPUT_FILE = 'emails.csv'  # 出力CSVファイル
BATCH_SIZE = 1000  # 一度に処理するバッチサイズ
CHECKPOINT_FILE = 'mbox_checkpoint.json'  # チェックポイントファイル

def save_checkpoint(processed_count, total_count, current_zip=None, processed_zips=None):
    """進捗状況をチェックポイントファイルに保存"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump({
            'processed_count': processed_count,
            'total_count': total_count,
            'current_zip': current_zip,
            'processed_zips': processed_zips or [],
            'timestamp': time.time()
        }, f)
    print(f"チェックポイントを保存しました: {processed_count} / {total_count} 件処理済み")
    if current_zip:
        print(f"現在処理中のZIPファイル: {current_zip}")

def load_checkpoint():
    """チェックポイントファイルから進捗状況を読み込む"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
                print(f"チェックポイントを読み込みました: {data['processed_count']} 件処理済み")
                if 'current_zip' in data and data['current_zip']:
                    print(f"前回処理中のZIPファイル: {data['current_zip']}")
                if 'processed_zips' in data and data['processed_zips']:
                    print(f"処理済みZIPファイル: {', '.join(data['processed_zips'])}")
                return data.get('processed_count', 0), data.get('current_zip'), data.get('processed_zips', [])
        except Exception as e:
            print(f"チェックポイントの読み込みに失敗しました: {e}")
    return 0, None, []

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
        print(f"ZIPファイル '{zip_path}' を展開しています...")
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
            # 一意的なファイル名を生成（複数ZIPからの展開に対応）
            zip_basename = os.path.basename(zip_path).replace('.zip', '')
            extract_path = os.path.join(temp_dir, f"{zip_basename}_{os.path.basename(mbox_file)}")
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
        print(f"エラー: '{zip_path}' は無効なZIPファイルです")
        return None
    except Exception as e:
        print(f"ZIPファイル '{zip_path}' 展開中にエラーが発生しました: {e}")
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

def process_mbox_file(mbox_path, processed_count=0):
    """MBOXファイルを処理してCSVに変換"""
    # メッセージ総数を取得
    total_count = count_messages(mbox_path)
    if total_count == 0:
        print("処理するメッセージがありません")
        return processed_count
    
    # 初回実行時はCSVヘッダーを書き込む
    if processed_count == 0 and not os.path.exists(OUTPUT_FILE):
        write_csv_header()
    
    # MBOXを開く
    mbox = mailbox.mbox(mbox_path)
    
    try:
        # 残りのメッセージ数を表示
        remaining_count = total_count
        print(f"残り {remaining_count} 件のメールを処理します")
        
        # バッチ処理のメインループ
        batch_start = 0
        
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
                            msg_id = msg.get('Message-ID', f"no-id-{processed_count+i-batch_start}")
                            if msg_id is None:
                                msg_id = f"no-id-{processed_count+i-batch_start}"
                            
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
                processed_count += (batch_end - batch_start)
                
                # 次のバッチへ
                batch_start = batch_end
                
                # バッチ間に短い休止を入れる
                if batch_start < total_count:
                    print("次のバッチ処理のために5秒間休止します...")
                
            except Exception as e:
                print(f"バッチ処理中にエラーが発生しました: {e}")
                print("10秒後に処理を再開します...")
                time.sleep(10)
        
        print(f"このMBOXファイルの処理が完了しました。")
    
    finally:
        # MBOXを閉じる
        mbox.close()
    
    return processed_count

def main():
    # 入力ZIPファイルを検索
    zip_files = sorted(glob.glob(INPUT_ZIP_PATTERN))
    if not zip_files:
        print(f"エラー: '{INPUT_ZIP_PATTERN}' に一致するZIPファイルが見つかりません")
        print("検索パターンを変更するか、ZIPファイルを配置してください")
        sys.exit(1)
    
    print(f"処理対象のZIPファイル: {len(zip_files)}個")
    for i, zip_file in enumerate(zip_files):
        print(f"{i+1}. {zip_file}")
    
    # チェックポイントから再開位置を読み込む
    processed_count, current_zip, processed_zips = load_checkpoint()
    
    # 処理済みZIPファイルを除外
    if processed_zips:
        remaining_zips = [zip_file for zip_file in zip_files if zip_file not in processed_zips]
        print(f"残りの処理対象ZIPファイル: {len(remaining_zips)}個")
    else:
        remaining_zips = zip_files
    
    # 前回処理中のZIPファイルを最初に処理
    if current_zip and current_zip in remaining_zips:
        remaining_zips.remove(current_zip)
        remaining_zips.insert(0, current_zip)
    
    # 一時ディレクトリを作成
    temp_dir = tempfile.mkdtemp()
    print(f"一時ディレクトリを作成しました: {temp_dir}")
    
    try:
        # 各ZIPファイルを順番に処理
        for zip_index, zip_file in enumerate(remaining_zips):
            try:
                print(f"\n===== ZIPファイル {zip_index+1}/{len(remaining_zips)} を処理しています: {zip_file} =====\n")
                
                # チェックポイントを更新
                save_checkpoint(processed_count, -1, current_zip=zip_file, processed_zips=processed_zips)
                
                # ZIPファイルからMBOXを展開
                mbox_path = extract_mbox_from_zip(zip_file, temp_dir)
                if not mbox_path:
                    print(f"警告: ZIPファイル '{zip_file}' からMBOXの展開に失敗しました。スキップします。")
                    continue
                
                # MBOXを処理
                new_processed_count = process_mbox_file(mbox_path, processed_count)
                processed_count = new_processed_count
                
                # 処理済みリストに追加
                processed_zips.append(zip_file)
                current_zip = None
                
                # チェックポイントを更新
                save_checkpoint(processed_count, -1, current_zip=None, processed_zips=processed_zips)
                
                # 一時ファイルを削除
                if os.path.exists(mbox_path):
                    os.remove(mbox_path)
                    print(f"一時MBOXファイルを削除しました: {mbox_path}")
                
                print(f"\n===== ZIPファイル '{zip_file}' の処理が完了しました =====\n")
                
            except Exception as e:
                print(f"ZIPファイル '{zip_file}' の処理中にエラーが発生しました: {e}")
                # チェックポイントを保存して続行可能にする
                save_checkpoint(processed_count, -1, current_zip=zip_file, processed_zips=processed_zips)
                print("30秒後に次のZIPファイルの処理を開始します...")
                time.sleep(30)
        
        # 全ファイルの処理完了
        print(f"\nすべてのZIPファイルの処理が完了しました。合計 {processed_count} 件のメールを処理しました。")
        print(f"結果は {OUTPUT_FILE} に保存されています。")
        
        # チェックポイントファイルを削除（オプション）
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            print("チェックポイントファイルを削除しました。")
    
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