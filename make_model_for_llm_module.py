#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from datasets import load_dataset, Dataset, concatenate_datasets
from transformers import (
    AutoModelForCausalLM,
    AutoModelForSequenceClassification,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
    BitsAndBytesConfig
)
from peft import LoraConfig, get_peft_model
from setfit import DistillationTrainer

# — 設定 —
ANNOT_JSONL      = "labeled_emails.jsonl"  # 元アノテーション
RETRAIN_CSV      = "to_review.csv"         # 人手レビュー済み訂正データ
TEACHER_LM_ID    = "your_org/llama-7b-japanese"
STUDENT_LM_ID    = "your_org/1.5b-student-japanese"
DISTILL_OUTPUT   = "output/distill"
LORA_OUTPUT      = "output/lora"
FINAL_MODEL_DIR  = "output/mail_classifier_1.5b_int8"
M5STACK_LLM_FILE = "mail_classifier_1.5b.llm"

def run_distillation():
    """Teacher→Student 蒸留（未変更）"""
    teacher = AutoModelForCausalLM.from_pretrained(TEACHER_LM_ID, device_map="auto")
    student = AutoModelForCausalLM.from_pretrained(STUDENT_LM_ID, device_map="auto")
    tokenizer = AutoTokenizer.from_pretrained(STUDENT_LM_ID)

    raw_ds = load_dataset("csv", data_files={"train": "emails.csv"})["train"]

    distill_args = TrainingArguments(
        output_dir=DISTILL_OUTPUT,
        per_device_train_batch_size=4,
        learning_rate=5e-5,
        max_steps=1000,
        logging_steps=50,
        save_total_limit=2
    )
    trainer = DistillationTrainer(
        teacher_model=teacher,
        student_model=student,
        args=distill_args,
        train_dataset=raw_ds,
        tokenizer=tokenizer
    )
    trainer.train()

def load_training_dataset():
    """
    元アノテーションと再訓練データを併合。
    再訓練データの 'correct_importance','correct_reason' で元データを上書き。
    """
    # 1) 元アノテーション読み込み
    ds_orig = load_dataset("json", data_files={"train": ANNOT_JSONL})["train"]

    # 2) 再訓練データ（CSV）を検出・読み込み
    if os.path.exists(RETRAIN_CSV):
        df_re = pd.read_csv(RETRAIN_CSV, encoding="utf-8")
        # カラム名を統一
        df_re = df_re.rename(columns={
            "correct_importance": "importance",
            "correct_reason": "reason"
        })[["email_body","importance","reason"]]
        ds_re = Dataset.from_pandas(df_re)

        # 3) 再訓練データのキー(email_body)で元データをフィルタ除去
        keys_re = set(df_re["email_body"])
        ds_filtered = ds_orig.filter(lambda ex: ex["email_body"] not in keys_re)

        # 4) DSを併合して返却
        return concatenate_datasets([ds_filtered, ds_re])

    # 再訓練データがなければ元データのみ返却
    return ds_orig

def run_lora_and_quant():
    """LoRA → int8量子化 → M5Stack変換"""
    # 1) 学習データセット準備
    train_ds = load_training_dataset()
    
    # 2) トークナイズ＋ラベル付与
    tokenizer = AutoTokenizer.from_pretrained(DISTILL_OUTPUT)
    def preprocess(ex):
        toks = tokenizer(
            ex["email_body"], truncation=True, padding="max_length"
        )
        toks["labels"] = ex["importance"]
        return toks
    tokenized = train_ds.map(preprocess, batched=True, remove_columns=train_ds.column_names)

    # 3) LoRAファインチューニング
    cls_model = AutoModelForSequenceClassification.from_pretrained(
        DISTILL_OUTPUT, num_labels=5
    )
    peft_config = LoraConfig(
        task_type="SEQ_CLS",
        inference_mode=False,
        r=8,
        lora_alpha=32,
        lora_dropout=0.05
    )
    model = get_peft_model(cls_model, peft_config)

    cls_args = TrainingArguments(
        output_dir=LORA_OUTPUT,
        per_device_train_batch_size=8,
        learning_rate=3e-4,
        num_train_epochs=2,
        logging_steps=20,
        save_total_limit=1
    )
    trainer = Trainer(
        model=model,
        args=cls_args,
        train_dataset=tokenized,
        tokenizer=tokenizer
    )
    trainer.train()

    # 4) int8 量子化
    quant_config = BitsAndBytesConfig(load_in_8bit=True)
    model_8bit = AutoModelForSequenceClassification.from_pretrained(
        LORA_OUTPUT,
        quantization_config=quant_config,
        device_map="auto"
    )
    model_8bit.save_pretrained(FINAL_MODEL_DIR)

    # 5) M5Stack用変換
    os.system(
        f"m5loader convert "
        f"--input {FINAL_MODEL_DIR}/pytorch_model.bin "
        f"--output {M5STACK_LLM_FILE} "
        f"--backend ax630c_llm"
    )

if __name__ == "__main__":
    # 1) 蒸留
    run_distillation()
    # 2) LoRAファインチューニング＋量子化＋変換
    run_lora_and_quant()
