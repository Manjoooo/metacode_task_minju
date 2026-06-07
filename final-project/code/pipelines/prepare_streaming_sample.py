import argparse
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/pcb_dataset_final.tsv")
    parser.add_argument("--output", default="data/ad_events_sample.csv")
    parser.add_argument("--sample", type=int, default=10000)
    args = parser.parse_args()

    print(f"[prepare] input={args.input}")
    print(f"[prepare] output={args.output}")
    print(f"[prepare] sample={args.sample}")

    df = pd.read_csv(args.input, sep="\t")

    print("[prepare] original columns:")
    print(df.columns.tolist())
    print(f"[prepare] original rows={len(df)}")

    if len(df) > args.sample:
        df = df.sample(n=args.sample, random_state=42).reset_index(drop=True)
    else:
        df = df.reset_index(drop=True)

    # 이미 event_id, event_time, campaign_id 등이 있으면 그대로 쓰고,
    # 없으면 실습용 표준 스키마를 생성한다.
    out = pd.DataFrame()

    if "event_id" in df.columns:
        out["event_id"] = df["event_id"].astype(str)
    else:
        out["event_id"] = ["e" + str(i).zfill(8) for i in range(len(df))]

    if "event_time" in df.columns:
        out["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    else:
        out["event_time"] = pd.date_range(
            start="2026-06-01 00:00:00",
            periods=len(df),
            freq="s"
        )

    if "campaign_id" in df.columns:
        out["campaign_id"] = df["campaign_id"].astype(str)
    elif "campaign" in df.columns:
        out["campaign_id"] = df["campaign"].astype(str)
    else:
        out["campaign_id"] = "camp_" + (df.index % 20 + 1).astype(str).str.zfill(2)

    if "user_id" in df.columns:
        out["user_id"] = df["user_id"].astype(str)
    elif "uid" in df.columns:
        out["user_id"] = df["uid"].astype(str)
    else:
        out["user_id"] = "user_" + (df.index % 5000 + 1).astype(str).str.zfill(5)

    # event_type 생성
    # 원본에 conversion/click 여부 컬럼이 있으면 활용하고,
    # 없으면 impression/click/conversion을 비율로 생성한다.
    if "event_type" in df.columns:
        out["event_type"] = df["event_type"].astype(str)
    elif "conversion" in df.columns:
        out["event_type"] = df["conversion"].apply(
            lambda x: "conversion" if int(x) == 1 else "click"
        )
    else:
        # 실습용: impression 70%, click 25%, conversion 5%
        types = []
        for i in range(len(df)):
            r = i % 100
            if r < 70:
                types.append("impression")
            elif r < 95:
                types.append("click")
            else:
                types.append("conversion")
        out["event_type"] = types

    if "amount" in df.columns:
        out["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    else:
        out["amount"] = out["event_type"].apply(
            lambda x: 30000 if x == "conversion" else 0
        )

    out["event_time"] = out["event_time"].fillna(
        pd.Timestamp("2026-06-01 00:00:00")
    )

    out.to_csv(args.output, index=False)

    print("[prepare] output preview:")
    print(out.head())
    print(f"[prepare] saved rows={len(out)}")


if __name__ == "__main__":
    main()