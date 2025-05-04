#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import pandas as pd
import requests

# — 設定 —  
LM_STUDIO_URL   = os.getenv("LM_STUDIO_URL", "http://127.0.0.1:8000")
LM_STUDIO_MODEL = os.getenv("LM_STUDIO_MODEL", "your-local-model")
INPUT_CSV       = "emails.csv"
OUTPUT_JSONL    = "labeled_emails_lmstudio.jsonl"

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

def annotate():
    df = pd.read_csv(INPUT_CSV)
    if "email_body" not in df.columns:
        raise ValueError("CSV に email_body カラムが存在しません")

    endpoint = f"{LM_STUDIO_URL}/v1/chat/completions"
    headers = {"Content-Type": "application/json"}

    with open(OUTPUT_JSONL, "w", encoding="utf-8") as fout:
        for idx, body in enumerate(df["email_body"], start=1):
            payload = {
                "model": LM_STUDIO_MODEL,
                "messages": [
                    {"role": "system", "content": ""},
                    {"role": "user",   "content": PROMPT_HEADER + body.strip()}
                ],
                "temperature": 0.0,
                "max_tokens": 60
            }
            resp = requests.post(endpoint, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            text = data["choices"][0]["message"]["content"].strip()

            # JSON 形式にパース
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                obj = json.loads(text.replace("\n", ""))

            record = {
                "email_body": body,
                "importance": obj.get("importance"),
                "reason": obj.get("reason")
            }
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")

            print(f"[{idx}/{len(df)}] importance={record['importance']} reason={record['reason']}")
            time.sleep(0.2)  # サーバ負荷軽減

    print(f"アノテーション完了：{len(df)} 件を {OUTPUT_JSONL} に保存しました")

if __name__ == "__main__":
    annotate()
