import json, pandas as pd

IN_FILE  = "labeled_emails_with_conf.jsonl"
OUT_FILE = "to_review.csv"
THRESH   = 0.6   # 自信度閾値

recs = []
with open(IN_FILE, "r", encoding="utf-8") as fin:
    for line in fin:
        obj = json.loads(line)
        if obj.get("confidence",1.0) < THRESH:
            recs.append({
                "email_body": obj["email_body"],
                "pred_importance": obj["importance"],
                "reason": obj["reason"],
                "confidence": obj["confidence"]
            })

# ランダムに500件サンプリングしてレビュー
df = pd.DataFrame(recs).sample(n=500, random_state=42)
df.to_csv(OUT_FILE, index=False, encoding="utf-8")
print(f"Exported {len(df)} low-confidence samples to {OUT_FILE}")
