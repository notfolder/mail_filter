#!/usr/bin/env python3
import asyncio
import telnetlib3
import json
import os

DEFAULT_TIMEOUT = 120  # 各コマンドの応答待ちタイムアウト（秒）

async def send_command_and_wait(reader, writer, command, timeout=DEFAULT_TIMEOUT):
    """
    JSON コマンドを送信し、タイムアウト付きで1行分の応答を待つ
    """
    command_str = json.dumps(command)
    full_command = command_str + "\n"
    writer.write(full_command)
    await writer.drain()
    print("送信:", command_str)

    try:
        response_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"タイムアウト({timeout}秒): 応答がありません。")
    response_line = response_line.strip()
    try:
        response_json = json.loads(response_line)
    except Exception as e:
        raise ValueError(f"応答のパースに失敗しました: {e}")
    if response_json.get("object", "") == "llm.utf-8.stream":
        print(response_json["data"]["delta"], end='')
    else:
        print("受信:", response_json)
    return response_json

async def receiver_loop(reader):
    """
    Telnet接続から継続的に JSON メッセージを受信し、表示するループ
    """
    print("受信ループ開始：常時メッセージ待機中...")
    while True:
        try:
            line = await reader.readline()
            if not line:
                print("リモート側が接続を閉じました。")
                break
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                #print("非同期受信:", msg)
                print(msg["data"]["delta"], end='', flush=True)
            except Exception as e:
                print("JSONパースエラー:", e, "受信データ:", line)
        except Exception as e:
            print("受信ループで例外発生:", e)
            break

async def main():
    host = '127.0.0.1'
    port = 10001

    try:
        reader, writer = await telnetlib3.open_connection(host, port)
        print(f"Telnet接続確立: {host}:{port}")
    except Exception as e:
        print("Telnet接続に失敗:", e)
        return

    # 初期化・セットアップ用コマンドのリスト
    commands = [
        {
            "request_id": "2",
            "work_id": "llm",
            "action": "setup",
            "object": "llm.setup",
            "data": {
                "model": "qwen2.5-0.5B-prefill-20e",
                "response_format": "llm.utf-8.stream",
                "input": "llm.utf-8",
                "enoutput": True,
                "max_token_len": 2000,
                "prompt": "You are an expert in mail sorting."
            }
        },
        {
            "request_id": "2",
            "work_id": "llm.1000",
            "action": "inference",
            "object": "llm.utf-8",
            "data": "Read the following email body and return an importance score (1-5), the reason (within 50 characters), and your confidence in this classification (0.0-1.0) in JSON format.\n\nClassification criteria:\n* 5: Urgent + financial matters\n* 4: Urgent or financial matters\n* 3: Important but not urgent communication\n* 2: Informational or promotional content\n* 1: Spam, phishing,email ewsletter, or unknown sender\n\nOutput format (JSON only):\n{\"importance\":<1-5>,\"reason\":\"<reason>\",\"confidence\":<0.0-1.0>}\n\nEmail body:\nお金を払ってください\n"
        }
    ]

    # 各コマンドを順次送信して応答を待つ
    work_id = "llm"
    for cmd in commands:
        try:
            cmd["work_id"] = work_id
            response = await send_command_and_wait(reader, writer, cmd, timeout=DEFAULT_TIMEOUT)
            error_code = response.get("error", {}).get("code")
            if error_code != 0:
                print("エラー応答:", response)
                break
            work_id = response.get("work_id")
        except Exception as e:
            print("コマンド送信中に例外発生:", e)
            break
        await asyncio.sleep(0.5)

    print("初期化完了。セッションは維持し、受信待ち状態に入ります。")
    os.system("echo 0 > /sys/class/leds/R/brightness; echo 0 > /sys/class/leds/G/brightness; echo 255 > /sys/class/leds/B/brightness")

    try:
        # 初期化後も接続を維持して常時受信待ち
        await receiver_loop(reader)
    except KeyboardInterrupt:
        print("KeyboardInterruptを受信、受信ループ終了。")
    finally:
        writer.close()
        print("Telnet接続をクローズしました。")

if __name__ == "__main__":
    asyncio.run(main())
