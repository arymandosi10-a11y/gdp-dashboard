
import io
import pandas as pd
import streamlit as st
from typing import Optional, Dict

# =========================
# HELPERS TO CLEAN FILES
# =========================

def load_daily_report(file) -> pd.DataFrame:
    df = pd.read_excel(file, header=2)
    df = df.rename(columns=lambda c: str(c).strip())
    return df

def load_summary(file) -> pd.DataFrame:
    df = pd.read_excel(file, header=2)
    df = df.rename(columns=lambda c: str(c).strip())
    if "Login" in df.columns:
        df = df[df["Login"].notna()]
        df = df[df["Login"] != "Total"]
        df["Login"] = pd.to_numeric(df["Login"], errors="coerce")
        df = df[df["Login"].notna()]
        df["Login"] = df["Login"].astype(int)
    return df

def load_trade_accounts(file) -> pd.DataFrame:
    df = pd.read_excel(file, header=2)
    df = df.rename(columns=lambda c: str(c).strip())
    if "Login" in df.columns:
        df = df[df["Login"].notna()]
        df["Login"] = pd.to_numeric(df["Login"], errors="coerce")
        df = df[df["Login"].notna()]
        df["Login"] = df["Login"].astype(int)
    return df

def load_account_master(file) -> pd.DataFrame:
    xls = pd.ExcelFile(file)
    # Use specific sheet if available, else first sheet
    sheet_name = "2.12.2025" if "2.12.2025" in xls.sheet_names else xls.sheet_names[0]
    df = pd.read_excel(file, sheet_name=sheet_name)
    df = df.rename(columns=lambda c: str(c).strip())
    keep_cols = [c for c in ["Login", "Group", "Type"] if c in df.columns]
    master = df[keep_cols].copy()
    master = master.dropna(subset=["Login"])
    master["Login"] = pd.to_numeric(master["Login"], errors="coerce").astype("Int64")
    master = master.dropna(subset=["Login"])
    master["Login"] = master["Login"].astype(int)
    return master

# =========================
# CORE CALCULATION
# =========================

def build_daily_table(
    opening_df: pd.DataFrame,
    closing_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    trades_df: Optional[pd.DataFrame] = None,
    master_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:

    opening = opening_df.rename(columns=lambda c: str(c).strip())
    closing = closing_df.rename(columns=lambda c: str(c).strip())
    summary = summary_df.rename(columns=lambda c: str(c).strip())

    open_eq = opening[["Login", "Name", "Equity", "Currency"]].copy()
    open_eq = open_eq.rename(columns={"Equity": "Net Equity Old"})
    close_eq = closing[["Login", "Name", "Equity", "Currency"]].copy()
    close_eq = close_eq.rename(columns={"Equity": "Net Equity New"})

    for df in (open_eq, close_eq, summary):
        if "Login" in df.columns:
            df["Login"] = pd.to_numeric(df["Login"], errors="coerce")
            df.dropna(subset=["Login"], inplace=True)
            df["Login"] = df["Login"].astype(int)

    sum_cols = ["Login", "Deposit", "Withdraw", "In/Out",
                "Volume", "Profit", "Currency"]
    sum_cols = [c for c in sum_cols if c in summary.columns]
    s = summary[sum_cols].copy()

    if "In/Out" in s.columns:
        s = s.rename(columns={"In/Out": "NET DP/WD CCY"})
    else:
        s["NET DP/WD CCY"] = 0.0

    s = s.rename(columns={
        "Volume": "Closed Volume",
        "Profit": "Closed P&L"
    })

    daily = pd.merge(open_eq, close_eq[["Login", "Net Equity New"]],
                     on="Login", how="outer", suffixes=("_open", "_close"))
    daily = pd.merge(daily, s, on="Login", how="left")

    if "Currency_close" in daily.columns and "Currency_open" in daily.columns:
        daily["Currency"] = daily["Currency_close"].fillna(daily["Currency_open"])
        daily.drop(columns=["Currency_open", "Currency_close"], inplace=True)
    elif "Currency_open" in daily.columns:
        daily["Currency"] = daily["Currency_open"]
        daily.drop(columns=["Currency_open"], inplace=True)

    if "Name_open" in daily.columns and "Name_close" in daily.columns:
        daily["Name"] = daily["Name_close"].fillna(daily["Name_open"])
        daily.drop(columns=["Name_open", "Name_close"], inplace=True)
    elif "Name_open" in daily.columns:
        daily["Name"] = daily["Name_open"]
        daily.drop(columns=["Name_open"], inplace=True)

    daily["Net Equity Old"] = daily["Net Equity Old"].fillna(0.0)
    daily["Net Equity New"] = daily["Net Equity New"].fillna(0.0)
    daily["NET DP/WD CCY"] = daily["NET DP/WD CCY"].fillna(0.0)

    daily["NET PNL CCY"] = (
        daily["Net Equity New"]
        - daily["Net Equity Old"]
        - daily["NET DP/WD CCY"]
    )

    if trades_df is not None and "Login" in trades_df.columns:
        t = trades_df.rename(columns=lambda c: str(c).strip())
        t["Login"] = pd.to_numeric(t["Login"], errors="coerce")
        t = t[t["Login"].notna()]
        t["Login"] = t["Login"].astype(int)
        t_small = t[["Login", "Volume", "Profit"]].copy()
        t_small = t_small.rename(columns={
            "Volume": "Volume (Trade Accounts)",
            "Profit": "Profit (Trade Accounts)"
        })
        daily = pd.merge(daily, t_small, on="Login", how="left")

    if master_df is not None and "Login" in master_df.columns:
        m = master_df.copy()
        m["Login"] = pd.to_numeric(m["Login"], errors="coerce")
        m = m[m["Login"].notna()]
        m["Login"] = m["Login"].astype(int)
        keep_cols = ["Login"]
        if "Group" in m.columns:
            keep_cols.append("Group")
        if "Type" in m.columns:
            keep_cols.append("Type")
        m = m[keep_cols].drop_duplicates(subset=["Login"])
        daily = pd.merge(daily, m, on="Login", how="left")

    daily = daily.sort_values("Login")

    final_cols = []
    for c in ["Login", "Name", "Group", "Type",
              "Net Equity Old", "Net Equity New",
              "NET DP/WD CCY", "NET PNL CCY",
              "Closed Volume", "Closed P&L",
              "Volume (Trade Accounts)", "Profit (Trade Accounts)",
              "Currency"]:
        if c in daily.columns:
            final_cols.append(c)

    others = [c for c in daily.columns if c not in final_cols]
    final_cols.extend(others)

    return daily[final_cols]


def build_summary_tables(daily_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}

    overall = daily_df.agg({
        "NET PNL CCY": "sum",
        "Closed P&L": "sum",
        "Closed Volume": "sum"
    }).to_frame(name="Total").reset_index().rename(
        columns={"index": "Metric"}
    )
    out["Overall"] = overall

    if "Type" in daily_df.columns:
        by_type = daily_df.groupby("Type").agg(
            total_net_pnl=("NET PNL CCY", "sum"),
            total_closed_pnl=("Closed P&L", "sum"),
            total_volume=("Closed Volume", "sum"),
            accounts=("Login", "count"),
        ).reset_index()
        out["By_Type"] = by_type

    return out


def export_to_excel(daily_df: pd.DataFrame, summary_tables: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        daily_df.to_excel(writer, sheet_name="Daily_Detail", index=False)
        for name, df in summary_tables.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    output.seek(0)
    return output.getvalue()

# =========================
# STREAMLIT UI
# =========================

st.set_page_config(page_title="ForexBroker P&L Report", layout="wide")
st.title("📊 ForexBroker P&L Report")

st.markdown(
    "Simple tool to see **daily client P&L, groups, and broker view**.\n"
    "Upload your MT5 / broker Excel reports and download a ready Excel file."
)

st.subheader("1️⃣ Upload Files")
account_master_file = st.file_uploader(
    "Optional: Account Master (with Login / Group / Type)",
    type=["xlsx", "xls"],
    key="master"
)
opening_file = st.file_uploader("Opening Daily Report", type=["xlsx", "xls"], key="open")
closing_file = st.file_uploader("Closing Daily Report", type=["xlsx", "xls"], key="close")
summary_file = st.file_uploader("Summary Report", type=["xlsx", "xls"], key="summary")
trade_accounts_file = st.file_uploader("Trade Accounts Detailed", type=["xlsx", "xls"], key="trades")

if opening_file and closing_file and summary_file:
    with st.spinner("Processing files and building P&L table..."):
        opening_df = load_daily_report(opening_file)
        closing_df = load_daily_report(closing_file)
        summary_df = load_summary(summary_file)
        trades_df = load_trade_accounts(trade_accounts_file) if trade_accounts_file else None
        master_df = load_account_master(account_master_file) if account_master_file else None

        daily_df = build_daily_table(opening_df, closing_df, summary_df, trades_df, master_df)

    st.success("✅ Report generated successfully!")

    st.subheader("2️⃣ Account-wise P&L")
    st.dataframe(daily_df, use_container_width=True)

    summaries = build_summary_tables(daily_df)

    st.subheader("3️⃣ Overall Summary")
    st.dataframe(summaries["Overall"], use_container_width=True)

    if "By_Type" in summaries:
        st.subheader("4️⃣ Summary by Type (A-Book / B-Book etc.)")
        st.dataframe(summaries["By_Type"], use_container_width=True)

    excel_bytes = export_to_excel(daily_df, summaries)
    st.subheader("5️⃣ Download Excel Output")
    st.download_button(
        "⬇️ Download ForexBroker P&L Excel",
        data=excel_bytes,
        file_name="forexbroker_pl_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Please upload **Opening**, **Closing**, and **Summary** reports to generate the P&L.")
