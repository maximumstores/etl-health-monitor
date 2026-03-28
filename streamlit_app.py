"""
ETL Health Monitor — Streamlit Cloud
Dashboard для моніторингу loader_runs + інфо про БД
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

KYIV_TZ = ZoneInfo("Europe/Kyiv")

st.set_page_config(
    page_title="ETL Health Monitor",
    page_icon="🏥",
    layout="wide",
)

# ============================================
# DB connection
# ============================================
@st.cache_resource
def get_conn():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def run_query(sql, params=None):
    conn = get_conn()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception as e:
        try:
            conn.reset()
        except:
            st.cache_resource.clear()
        st.error(f"DB error: {e}")
        return pd.DataFrame()

# ============================================
# Safe query (rollback on error)
# ============================================
def safe_query(sql, params=None):
    """Запит з rollback — помилка в одному не ламає інші."""
    conn = get_conn()
    try:
        df = pd.read_sql(sql, conn, params=params)
        conn.commit()
        return df
    except Exception:
        try:
            conn.rollback()
        except:
            pass
        return pd.DataFrame()

# ============================================
# Queries — ETL Health
# ============================================
def load_summary(days=7):
    return run_query("""
        SELECT
            loader_name, loader_num,
            COUNT(*) as total_runs,
            COUNT(*) FILTER (WHERE status = 'OK') as ok,
            COUNT(*) FILTER (WHERE status = 'FAIL') as fails,
            COUNT(*) FILTER (WHERE status = 'ERROR') as errors,
            COUNT(*) FILTER (WHERE status = 'ZOMBIE') as zombies,
            COUNT(*) FILTER (WHERE status = 'SKIP') as skips,
            COUNT(*) FILTER (WHERE attempt > 1) as retried,
            COUNT(*) FILTER (WHERE attempt > 1 AND status = 'OK') as retry_saved,
            ROUND(AVG(duration_sec) FILTER (WHERE status = 'OK'))::int as avg_duration,
            ROUND(MAX(duration_sec) FILTER (WHERE status = 'OK'))::int as max_duration
        FROM public.loader_runs
        WHERE started_at > NOW() - INTERVAL '%s days'
        GROUP BY loader_name, loader_num
        ORDER BY loader_num
    """, (days,))

def load_hourly_heatmap(days=7):
    return run_query("""
        SELECT
            loader_name,
            EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Kyiv')::int as hour,
            EXTRACT(DOW FROM started_at AT TIME ZONE 'Europe/Kyiv')::int as dow,
            status,
            COUNT(*) as cnt
        FROM public.loader_runs
        WHERE started_at > NOW() - INTERVAL '%s days'
        GROUP BY loader_name, hour, dow, status
    """, (days,))

def load_duration_trend(days=14):
    return run_query("""
        SELECT
            loader_name, loader_num,
            (started_at AT TIME ZONE 'Europe/Kyiv')::date as run_date,
            ROUND(AVG(duration_sec) FILTER (WHERE status = 'OK'))::int as avg_sec,
            ROUND(MAX(duration_sec) FILTER (WHERE status = 'OK'))::int as max_sec
        FROM public.loader_runs
        WHERE started_at > NOW() - INTERVAL '%s days'
          AND status = 'OK'
        GROUP BY loader_name, loader_num, run_date
        ORDER BY run_date
    """, (days,))

def load_recent_failures(limit=30):
    return run_query("""
        SELECT
            loader_name, status, attempt,
            duration_sec::int as duration,
            error_msg, group_label,
            started_at AT TIME ZONE 'Europe/Kyiv' as started_at
        FROM public.loader_runs
        WHERE status IN ('FAIL', 'ERROR', 'ZOMBIE')
        ORDER BY started_at DESC
        LIMIT %s
    """, (limit,))

def load_today_timeline():
    return run_query("""
        SELECT
            loader_name, loader_num, status, attempt,
            duration_sec,
            started_at AT TIME ZONE 'Europe/Kyiv' as started_at,
            finished_at AT TIME ZONE 'Europe/Kyiv' as finished_at
        FROM public.loader_runs
        WHERE (started_at AT TIME ZONE 'Europe/Kyiv')::date =
              (NOW() AT TIME ZONE 'Europe/Kyiv')::date
        ORDER BY started_at
    """)

# ============================================
# Charts
# ============================================
STATUS_COLORS = {
    "OK": "#22c55e", "FAIL": "#ef4444", "ERROR": "#f97316",
    "ZOMBIE": "#8b5cf6", "SKIP": "#94a3b8",
}

def chart_uptime_bars(summary):
    if summary.empty:
        return None
    df = summary.copy()
    df["success_rate"] = (df["ok"] / df["total_runs"] * 100).round(1)
    df = df.sort_values("success_rate")

    colors = ["#22c55e" if r >= 95 else "#f59e0b" if r >= 80 else "#ef4444"
              for r in df["success_rate"]]

    fig = go.Figure(go.Bar(
        x=df["success_rate"], y=df["loader_name"],
        orientation="h", marker_color=colors,
        text=[f"{r}%" for r in df["success_rate"]],
        textposition="auto",
    ))
    fig.update_layout(
        title="Success Rate по лоадерам",
        xaxis_title="Success %", xaxis_range=[0, 105],
        height=max(300, len(df) * 40),
        margin=dict(l=10, r=10, t=40, b=10),
    )
    fig.add_vline(x=95, line_dash="dash", line_color="#22c55e",
                  annotation_text="95% SLA")
    return fig

def chart_failure_heatmap(heatmap_data):
    if heatmap_data.empty:
        return None
    fails = heatmap_data[heatmap_data["status"].isin(["FAIL", "ERROR", "ZOMBIE"])]
    if fails.empty:
        return None

    pivot = fails.groupby(["dow", "hour"])["cnt"].sum().reset_index()
    matrix = pivot.pivot(index="dow", columns="hour", values="cnt").fillna(0)
    for h in range(24):
        if h not in matrix.columns:
            matrix[h] = 0
    matrix = matrix[sorted(matrix.columns)]
    for d in range(7):
        if d not in matrix.index:
            matrix.loc[d] = 0
    matrix = matrix.sort_index()

    day_names = ["Нд", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"]
    fig = go.Figure(go.Heatmap(
        z=matrix.values,
        x=[f"{h:02d}:00" for h in matrix.columns],
        y=[day_names[d] for d in matrix.index],
        colorscale=[[0, "#1e293b"], [0.5, "#f59e0b"], [1, "#ef4444"]],
        showscale=True, colorbar_title="Fails",
    ))
    fig.update_layout(
        title="Падіння по годинам і дням",
        height=280, margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig

def chart_duration_trends(trend_data):
    if trend_data.empty:
        return None
    fig = px.line(
        trend_data, x="run_date", y="avg_sec", color="loader_name",
        title="Середня тривалість (секунди)",
        labels={"run_date": "Дата", "avg_sec": "Сек", "loader_name": "Лоадер"},
    )
    fig.update_layout(
        height=400, margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    return fig

def chart_today_timeline(timeline_data):
    if timeline_data.empty:
        return None
    df = timeline_data.copy()
    df["started_at"] = pd.to_datetime(df["started_at"])
    df["finished_at"] = pd.to_datetime(df["finished_at"])
    df = df.dropna(subset=["finished_at"])
    if df.empty:
        return None
    fig = px.timeline(
        df, x_start="started_at", x_end="finished_at",
        y="loader_name", color="status",
        color_discrete_map=STATUS_COLORS,
        title="Сьогоднішні запуски",
        hover_data=["attempt", "duration_sec"],
    )
    fig.update_layout(
        height=max(300, len(df["loader_name"].unique()) * 35),
        margin=dict(l=10, r=10, t=40, b=10), showlegend=True,
    )
    return fig

def chart_retry_effectiveness(summary):
    if summary.empty:
        return None
    df = summary[summary["retried"] > 0].copy()
    if df.empty:
        return None
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Retry saved", x=df["loader_name"],
        y=df["retry_saved"], marker_color="#22c55e",
    ))
    fig.add_trace(go.Bar(
        name="Retry failed", x=df["loader_name"],
        y=df["retried"] - df["retry_saved"], marker_color="#ef4444",
    ))
    fig.update_layout(
        barmode="stack", title="Ефективність ретраїв",
        height=300, margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig

# ============================================
# Sidebar — навігація
# ============================================
st.sidebar.markdown("## 📡 ETL Monitor")
page = st.sidebar.radio("Сторінка", ["🏥 ETL Health", "🗄️ База даних"], label_visibility="collapsed")

if st.sidebar.button("🔄 Оновити"):
    st.cache_resource.clear()
    st.rerun()

# ============================================
# Sidebar — навігація
# ============================================
st.sidebar.markdown("## 📡 ETL Monitor")
page = st.sidebar.radio("Сторінка", ["🏥 ETL Health", "🗄️ База даних", "📋 Архітектура"], label_visibility="collapsed")

if st.sidebar.button("🔄 Оновити"):
    st.cache_resource.clear()
    st.rerun()

# ============================================
# ETL Architecture data
# ============================================
SCHEDULE = [
    ( 5,  0,  [13],        "FBA Operations"),
    ( 6,  0,  [5, 99],     "Listings + Restock"),
    ( 7,  0,  [1, 0],      "Inventory + DQ"),
    ( 8,  0,  [2, 11],     "Finance + Pricing"),
    ( 9,  0,  [3],         "Orders (fast)"),
    ( 9, 30,  [98],        "Returns Agent"),
    (10,  0,  [4, 6],      "Returns + Traffic"),
    (12,  0,  [11, 99],    "Pricing + Restock"),
    (13,  0,  [1],         "Inventory"),
    (16,  0,  [11],        "Pricing"),
    (18,  0,  [99],        "Restock Agent"),
    (19,  0,  [1],         "Inventory"),
    (20,  0,  [2, 11],     "Finance + Pricing"),
    (21,  0,  [3],         "Orders (fast)"),
    (22,  0,  [0],         "DQ Evening Check"),
]

LOADER_INFO = {
    0:  {"name": "DQ Check",        "emoji": "🔍", "file": "data_quality.py",         "tables": [],                                              "api": "—",                "type": "check"},
    1:  {"name": "Inventory",       "emoji": "📦", "file": "01_inventory_loader.py",   "tables": ["fba_inventory"],                                "api": "SP-API Reports",   "type": "loader"},
    2:  {"name": "Finance",         "emoji": "💰", "file": "02_finance_loader.py",     "tables": ["settlements", "finance_events", "finance_event_groups"], "api": "SP-API Finance", "type": "loader"},
    3:  {"name": "Orders",          "emoji": "🛒", "file": "03_orders_loader.py",      "tables": ["orders"],                                      "api": "SP-API Reports",   "type": "loader"},
    4:  {"name": "Returns",         "emoji": "🔙", "file": "04_returns_loader.py",     "tables": ["fba_returns"],                                 "api": "SP-API Reports",   "type": "loader"},
    5:  {"name": "Listings",        "emoji": "📝", "file": "05_listings_loader.py",    "tables": ["listings_all", "catalog_items"],                "api": "SP-API Catalog",   "type": "loader"},
    6:  {"name": "Traffic",         "emoji": "📈", "file": "06_sales traffic_loader.py","tables": ["sales_traffic"],                               "api": "SP-API Reports",   "type": "loader"},
    7:  {"name": "Ads",             "emoji": "🎯", "file": "07_ads_loader.py",         "tables": ["ads_campaigns", "ads_keywords"],                "api": "Ads API",          "type": "loader"},
    8:  {"name": "Brand Analytics", "emoji": "🏷️", "file": "08_brand_analytics_loader.py", "tables": [],                                          "api": "SP-API BA",        "type": "loader"},
    9:  {"name": "Tax/VAT",         "emoji": "📋", "file": "09_tax_loader.py",         "tables": [],                                              "api": "SP-API Reports",   "type": "loader"},
    11: {"name": "Pricing",         "emoji": "💲", "file": "11_pricing_loader.py",     "tables": ["pricing_current", "pricing_offers", "pricing_buybox"], "api": "SP-API Pricing", "type": "loader"},
    13: {"name": "FBA Operations",  "emoji": "📦", "file": "13_fba_operations_loader.py","tables": ["fba_shipments", "fba_shipment_items", "fba_removals"], "api": "SP-API FBA", "type": "loader"},
    98: {"name": "Returns Agent",   "emoji": "🔴", "file": "returns_agent.py",         "tables": [],                                              "api": "Gemini AI",        "type": "agent"},
    99: {"name": "Restock Agent",   "emoji": "🤖", "file": "restock_agent.py",         "tables": [],                                              "api": "Gemini AI",        "type": "agent"},
}

# ============================================
# PAGE: Архітектура
# ============================================
if page == "📋 Архітектура":
    st.markdown("## 📋 Архітектура ETL Pipeline")

    # ── System overview ──
    st.markdown("### 🏗️ Система")
    co1, co2, co3, co4 = st.columns(4)
    co1.metric("Лоадерів", len([l for l in LOADER_INFO.values() if l["type"] == "loader"]))
    co2.metric("Агентів", len([l for l in LOADER_INFO.values() if l["type"] == "agent"]))
    co3.metric("Слотів/день", len(SCHEDULE))
    parallel_count = len([s for s in SCHEDULE if len(s[2]) > 1])
    co4.metric("Паралельних груп", parallel_count)

    st.divider()

    # ── Розклад ──
    st.markdown("### 📅 Розклад (Kyiv timezone)")

    schedule_rows = []
    for h, m, nums, label in sorted(SCHEDULE, key=lambda x: (x[0], x[1])):
        loaders = []
        for n in nums:
            info = LOADER_INFO.get(n, {})
            loaders.append(f"{info.get('emoji','')} {info.get('name', f'#{n}')}")
        parallel = "⚡" if len(nums) > 1 else ""
        schedule_rows.append({
            "Час": f"{h:02d}:{m:02d}",
            "": parallel,
            "Група": label,
            "Лоадери": " + ".join(loaders),
            "Тип": "Паралельно" if len(nums) > 1 else "Послідовно",
        })

    df_schedule = pd.DataFrame(schedule_rows)
    st.dataframe(df_schedule, use_container_width=True, hide_index=True)

    st.divider()

    # ── Timeline візуалізація ──
    st.markdown("### ⏰ Timeline дня")

    timeline_bars = []
    for h, m, nums, label in sorted(SCHEDULE, key=lambda x: (x[0], x[1])):
        start_min = h * 60 + m
        # Приблизна тривалість
        est_duration = max(5, len(nums) * 5)
        for n in nums:
            info = LOADER_INFO.get(n, {})
            timeline_bars.append({
                "Loader": f"{info.get('emoji','')} {info.get('name', f'#{n}')}",
                "Start": pd.Timestamp(f"2026-01-01 {h:02d}:{m:02d}:00"),
                "End": pd.Timestamp(f"2026-01-01 {h:02d}:{m:02d}:00") + pd.Timedelta(minutes=est_duration),
                "Type": info.get("type", "?"),
            })

    if timeline_bars:
        df_tl = pd.DataFrame(timeline_bars)
        type_colors = {"loader": "#3b82f6", "agent": "#f59e0b", "check": "#22c55e"}
        fig = px.timeline(
            df_tl, x_start="Start", x_end="End",
            y="Loader", color="Type",
            color_discrete_map=type_colors,
        )
        fig.update_layout(
            height=max(350, len(df_tl) * 25),
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="Час (Kyiv)",
            showlegend=True,
        )
        fig.update_xaxes(tickformat="%H:%M")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Лоадери — детальна інформація ──
    st.markdown("### 🔧 Лоадери")

    for num, info in sorted(LOADER_INFO.items()):
        if info["type"] == "check":
            continue

        # Скільки разів на день запускається
        runs_per_day = sum(1 for _, _, nums, _ in SCHEDULE if num in nums)
        times = [f"{h:02d}:{m:02d}" for h, m, nums, _ in SCHEDULE if num in nums]

        # Статус з БД якщо є
        last_run = safe_query("""
            SELECT status, duration_sec::int as dur,
                   started_at AT TIME ZONE 'Europe/Kyiv' as started_at
            FROM public.loader_runs
            WHERE loader_num = %s
            ORDER BY started_at DESC LIMIT 1
        """, (num,))

        type_badge = "🤖 Agent" if info["type"] == "agent" else "📥 Loader"

        with st.expander(f"{info['emoji']} **{info['name']}** — {type_badge} · {runs_per_day}x/день", expanded=False):
            c1, c2, c3 = st.columns(3)
            c1.markdown(f"**Файл:** `{info['file']}`")
            c2.markdown(f"**API:** {info['api']}")
            c3.markdown(f"**Запусків/день:** {runs_per_day}")

            if info["tables"]:
                st.markdown(f"**Таблиці:** {', '.join([f'`{t}`' for t in info['tables']])}")

            st.markdown(f"**Розклад:** {' · '.join(times)}")

            if not last_run.empty:
                r = last_run.iloc[0]
                status_icon = {"OK":"✅","FAIL":"❌","ERROR":"💥","ZOMBIE":"🧟","SKIP":"⏭"}.get(r["status"],"?")
                started = r["started_at"].strftime("%d.%m %H:%M") if pd.notna(r["started_at"]) else "?"
                st.markdown(f"**Останній запуск:** {status_icon} {r['status']} · {r['dur']}с · {started}")
            else:
                st.markdown("**Останній запуск:** немає даних (scheduler v2.4 ще не писав)")

    st.divider()

    # ── Data Flow diagram ──
    st.markdown("### 🔀 Data Flow")
    st.markdown("""
    ```
    Amazon SP-API / Ads API
            │
            ▼
    ┌──────────────────────────┐
    │   run_forever.py v2.4    │
    │   ├─ Circuit Breaker     │
    │   ├─ Auto-Retry (2x)     │
    │   ├─ Exponential Backoff │
    │   └─ Parallel Groups     │
    └──────────┬───────────────┘
               │
         ┌─────┼─────┐
         ▼     ▼     ▼
    ┌────────┐┌──────┐┌────────┐
    │Loaders ││Agents││DQ Check│
    │(11 шт) ││(2 шт)││        │
    └───┬────┘└──┬───┘└───┬────┘
        │        │        │
        ▼        ▼        ▼
    ┌──────────────────────────┐
    │   PostgreSQL (Heroku)     │
    │   ├─ 25+ таблиць          │
    │   ├─ loader_runs (історія)│
    │   └─ pending_reports      │
    └──────────┬───────────────┘
               │
         ┌─────┼─────┐
         ▼     ▼     ▼
    ┌────────┐┌──────────┐┌──────────────┐
    │Dashboard││ETL Health││Telegram Bot  │
    │(BI)     ││Monitor   ││Notifications │
    └────────┘└──────────┘└──────────────┘
    ```
    """)

# ============================================
# PAGE: База даних
# ============================================
elif page == "🗄️ База даних":
    st.markdown("## 🗄️ База даних")

    # ── Загальні метрики ──
    db_size = safe_query("""
        SELECT pg_size_pretty(pg_database_size(current_database())) as size,
               pg_database_size(current_database()) as size_bytes
    """)

    tables_info = safe_query("""
        SELECT
            schemaname || '.' || relname as table_name,
            relname,
            schemaname,
            n_live_tup as rows,
            pg_size_pretty(pg_total_relation_size(relid)) as total_size,
            pg_total_relation_size(relid) as size_bytes,
            pg_size_pretty(pg_relation_size(relid)) as data_size,
            pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) as index_size
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
    """)

    if not tables_info.empty:
        total_tables = len(tables_info)
        total_rows = tables_info["rows"].sum()
        db_size_str = db_size.iloc[0]["size"] if not db_size.empty else "?"
        size_bytes = db_size.iloc[0]["size_bytes"] if not db_size.empty else 0

        # Heroku plan limits (bytes)
        PLAN_LIMITS = {
            "Mini":       1  * 1024**3,   # 1 GB
            "Basic":      10 * 1024**3,   # 10 GB
            "Standard-0": 64 * 1024**3,   # 64 GB
        }

        # Автодетект плану по розміру
        if size_bytes <= 1.1 * 1024**3:
            plan_name, plan_limit = "Mini", PLAN_LIMITS["Mini"]
        elif size_bytes <= 11 * 1024**3:
            plan_name, plan_limit = "Basic", PLAN_LIMITS["Basic"]
        else:
            plan_name, plan_limit = "Standard-0", PLAN_LIMITS["Standard-0"]

        used_pct = (size_bytes / plan_limit * 100) if plan_limit else 0
        remaining_bytes = max(0, plan_limit - size_bytes)
        remaining_mb = remaining_bytes / (1024**2)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Розмір БД", db_size_str)
        c2.metric("📊 Таблиць", f"{total_tables}")
        c3.metric("📝 Рядків (всього)", f"{total_rows:,.0f}")
        c4.metric("💾 Залишок", f"{remaining_mb:.0f} MB")

        # Progress bar
        bar_color = "🟢" if used_pct < 70 else "🟡" if used_pct < 90 else "🔴"
        st.markdown(f"{bar_color} **Heroku {plan_name}**: {db_size_str} / "
                    f"{plan_limit / (1024**3):.0f} GB ({used_pct:.1f}%)")
        st.progress(min(used_pct / 100, 1.0))

        if used_pct >= 90:
            st.error(f"⚠️ БД майже повна! Залишилось {remaining_mb:.0f} MB. "
                     f"Видали старі таблиці або апгрейд на Basic (10 GB).")
        elif used_pct >= 70:
            st.warning(f"БД заповнена на {used_pct:.0f}%. Слідкуй за розміром.")

    st.divider()

    # ── Freshness: один UNION ALL запит ──
    ts_columns = safe_query("""
        SELECT DISTINCT ON (table_schema, table_name)
            table_schema || '.' || table_name as full_name,
            column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND data_type IN ('timestamp with time zone', 'timestamp without time zone')
        ORDER BY table_schema, table_name,
            CASE
                WHEN column_name = 'updated_at' THEN 1
                WHEN column_name = 'created_at' THEN 2
                WHEN column_name LIKE '%_at' THEN 3
                WHEN column_name LIKE '%_date' THEN 4
                ELSE 5
            END,
            ordinal_position
    """)

    last_writes = {}
    if not ts_columns.empty:
        parts = []
        for _, row in ts_columns.iterrows():
            tname = row["full_name"]
            col = row["column_name"]
            parts.append(
                f"SELECT '{tname}' as tbl, "
                f"MAX({col}::timestamptz) as last_write "
                f"FROM {tname}"
            )
        if parts:
            freshness = safe_query(" UNION ALL ".join(parts))
            if not freshness.empty:
                for _, row in freshness.iterrows():
                    if pd.notna(row["last_write"]):
                        last_writes[row["tbl"]] = row["last_write"]

    # ── Розмір по таблицям (chart) ──
    if not tables_info.empty:
        st.markdown("### 💾 Розмір таблиць")
        df_chart = tables_info[tables_info["size_bytes"] > 0].copy()
        df_chart = df_chart.sort_values("size_bytes")
        df_chart["short_name"] = df_chart["table_name"].str.replace("public.", "", regex=False)

        now = datetime.now(KYIV_TZ)

        # Підпис: ім'я + дата останнього запису
        labels = []
        bar_colors = []
        for _, r in df_chart.iterrows():
            name = r["short_name"]
            last = last_writes.get(r["table_name"])
            if last is not None:
                try:
                    last_dt = pd.to_datetime(last, utc=True).astimezone(KYIV_TZ)
                    total_hours = (now - last_dt).total_seconds() / 3600
                    date_str = last_dt.strftime("%d.%m %H:%M")
                    labels.append(f"{name}  🕒 {date_str}")
                    bar_colors.append("#22c55e" if total_hours < 6 else "#f59e0b" if total_hours < 24 else "#ef4444")
                except:
                    labels.append(name)
                    bar_colors.append("#64748b")
            else:
                labels.append(name)
                bar_colors.append("#64748b")

        fig = go.Figure(go.Bar(
            x=df_chart["size_bytes"] / (1024 * 1024),
            y=labels,
            orientation="h",
            marker_color=bar_colors,
            text=df_chart["total_size"],
            textposition="auto",
        ))
        fig.update_layout(
            xaxis_title="MB",
            height=max(400, len(df_chart) * 28),
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Таблиці — детальна інформація ──
    st.markdown("### 📋 Всі таблиці")

    # Delete/Truncate confirmation state
    if "confirm_delete" not in st.session_state:
        st.session_state.confirm_delete = None
    if "confirm_truncate" not in st.session_state:
        st.session_state.confirm_truncate = None

    if not tables_info.empty:
        now = datetime.now(KYIV_TZ)

        # Якщо є підтвердження видалення
        if st.session_state.confirm_delete:
            tbl = st.session_state.confirm_delete
            st.warning(f"⚠️ Видалити таблицю **{tbl}**? Структура + дані зникнуть назавжди!")
            col_yes, col_no, _ = st.columns([1, 1, 6])
            with col_yes:
                if st.button("🗑️ Так, видалити", type="primary"):
                    conn = get_conn()
                    try:
                        cur = conn.cursor()
                        cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
                        conn.commit()
                        cur.close()
                        st.success(f"✅ Таблиця {tbl} видалена")
                        st.session_state.confirm_delete = None
                        st.cache_resource.clear()
                        import time as _t; _t.sleep(1)
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Помилка: {e}")
                        st.session_state.confirm_delete = None
            with col_no:
                if st.button("Скасувати", key="cancel_delete"):
                    st.session_state.confirm_delete = None
                    st.rerun()
            st.divider()

        # Якщо є підтвердження очистки
        if st.session_state.confirm_truncate:
            tbl = st.session_state.confirm_truncate
            st.warning(f"🧹 Очистити таблицю **{tbl}**? Всі дані будуть видалені, структура залишиться.")
            col_yes, col_no, _ = st.columns([1, 1, 6])
            with col_yes:
                if st.button("🧹 Так, очистити", type="primary"):
                    conn = get_conn()
                    try:
                        cur = conn.cursor()
                        cur.execute(f"TRUNCATE TABLE {tbl} CASCADE")
                        conn.commit()
                        cur.close()
                        st.success(f"✅ Таблиця {tbl} очищена (0 рядків)")
                        st.session_state.confirm_truncate = None
                        st.cache_resource.clear()
                        import time as _t; _t.sleep(1)
                        st.rerun()
                    except Exception as e:
                        conn.rollback()
                        st.error(f"❌ Помилка: {e}")
                        st.session_state.confirm_truncate = None
            with col_no:
                if st.button("Скасувати", key="cancel_truncate"):
                    st.session_state.confirm_truncate = None
                    st.rerun()
            st.divider()

        # Header
        cols = st.columns([0.3, 2.5, 1, 1, 1, 1, 2, 1.2, 0.8])
        headers = ["", "Таблиця", "Рядків", "Розмір", "Дані", "Індекси", "Останній запис", "Давність", ""]
        for col, h in zip(cols, headers):
            col.markdown(f"**{h}**")

        for idx, row in tables_info.iterrows():
            tname = row["table_name"]
            rows_count = int(row["rows"]) if pd.notna(row["rows"]) else 0
            last = last_writes.get(tname)

            last_str = "—"
            ago_str = ""
            freshness_status = "⚪"

            if last is not None:
                try:
                    last_dt = pd.to_datetime(last, utc=True).astimezone(KYIV_TZ)
                    last_str = last_dt.strftime("%d.%m.%Y %H:%M")
                    diff = now - last_dt
                    total_hours = diff.total_seconds() / 3600

                    if diff.days > 0:
                        ago_str = f"{diff.days}д тому"
                    elif total_hours >= 1:
                        ago_str = f"{int(total_hours)}г тому"
                    else:
                        ago_str = f"{int(diff.total_seconds() // 60)}хв тому"

                    if total_hours < 6:
                        freshness_status = "🟢"
                    elif total_hours < 24:
                        freshness_status = "🟡"
                    else:
                        freshness_status = "🔴"
                except:
                    pass

            short = tname.replace("public.", "")
            cols = st.columns([0.3, 2.5, 1, 1, 1, 1, 2, 1.2, 0.8])
            cols[0].markdown(freshness_status)
            cols[1].markdown(f"`{short}`")
            cols[2].markdown(f"{rows_count:,}")
            cols[3].markdown(row["total_size"])
            cols[4].markdown(row["data_size"])
            cols[5].markdown(row["index_size"])
            cols[6].markdown(last_str)
            cols[7].markdown(ago_str)
            # Кнопки: очистити + видалити
            btn_col = cols[8]
            bc1, bc2 = btn_col.columns(2)
            if bc1.button("🧹", key=f"trunc_{tname}", help="Очистити"):
                st.session_state.confirm_truncate = tname
                st.rerun()
            if bc2.button("🗑️", key=f"del_{tname}", help="Видалити"):
                st.session_state.confirm_delete = tname
                st.rerun()

# ============================================
# PAGE: ETL Health
# ============================================
else:
    st.markdown("## 🏥 ETL Health Monitor")

    col_period, _ = st.columns([3, 1])
    with col_period:
        days = st.selectbox("Період", [1, 3, 7, 14, 30],
                            index=2, format_func=lambda d: f"Останні {d} днів")

    summary = load_summary(days)
    if summary.empty:
        st.warning("Немає даних в loader_runs. Scheduler v2.4 ще не записував історію.")
        st.info("Дані з'являться після першого запуску лоадера з run_forever.py v2.4")
        st.stop()

    # ── KPI cards ──
    total_runs = summary["total_runs"].sum()
    total_ok = summary["ok"].sum()
    total_fails = summary["fails"].sum() + summary["errors"].sum() + summary["zombies"].sum()
    total_retried = summary["retried"].sum()
    total_retry_saved = summary["retry_saved"].sum()
    overall_rate = (total_ok / total_runs * 100) if total_runs else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Всього запусків", f"{total_runs:,}")
    c2.metric("Success rate", f"{overall_rate:.1f}%",
              delta="OK" if overall_rate >= 95 else "Low",
              delta_color="normal" if overall_rate >= 95 else "inverse")
    c3.metric("Падінь", f"{total_fails:,}",
              delta_color="inverse" if total_fails > 0 else "off")
    c4.metric("Ретраїв", f"{total_retried:,}")
    c5.metric("Врятовано ретраєм", f"{total_retry_saved:,}",
              delta=f"{total_retry_saved}/{total_retried}" if total_retried else "—")

    st.divider()

    # ── Row 1: Uptime + Heatmap ──
    col1, col2 = st.columns(2)
    with col1:
        fig = chart_uptime_bars(summary)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    with col2:
        heatmap = load_hourly_heatmap(days)
        fig = chart_failure_heatmap(heatmap)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("🎉 Жодних падінь за період!")

    st.divider()

    # ── Row 2: Duration trends ──
    trend = load_duration_trend(days)
    fig = chart_duration_trends(trend)
    if fig:
        st.plotly_chart(fig, use_container_width=True)

    # ── Row 3: Today timeline + Retry effectiveness ──
    col3, col4 = st.columns(2)
    with col3:
        timeline = load_today_timeline()
        fig = chart_today_timeline(timeline)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Сьогодні ще немає запусків")
    with col4:
        fig = chart_retry_effectiveness(summary)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Ретраїв ще не було")

    st.divider()

    # ── Summary table ──
    st.markdown("### 📊 Зведена таблиця")
    display = summary[[
        "loader_name", "total_runs", "ok", "fails", "errors",
        "zombies", "skips", "retried", "retry_saved",
        "avg_duration", "max_duration"
    ]].copy()
    display.columns = [
        "Лоадер", "Запусків", "✅ OK", "❌ FAIL", "💥 ERROR",
        "🧟 ZOMBIE", "⏭ SKIP", "🔄 Retried", "✅ Saved",
        "⏱ Avg (с)", "⏱ Max (с)"
    ]
    st.dataframe(display, use_container_width=True, hide_index=True)

    # ── Recent failures ──
    st.markdown("### 🔴 Останні падіння")
    failures = load_recent_failures(20)
    if failures.empty:
        st.success("Жодних падінь!")
    else:
        for _, row in failures.iterrows():
            status_icon = {"FAIL": "❌", "ERROR": "💥", "ZOMBIE": "🧟"}.get(row["status"], "?")
            retry_tag = f" (retry #{row['attempt']-1})" if row["attempt"] > 1 else ""
            time_str = row["started_at"].strftime("%d.%m %H:%M") if pd.notna(row["started_at"]) else "?"
            with st.expander(
                f"{status_icon} {row['loader_name']} — {time_str}{retry_tag}",
                expanded=False
            ):
                st.text(f"Status:   {row['status']}")
                st.text(f"Duration: {row['duration']}s")
                st.text(f"Group:    {row['group_label']}")
                if row["error_msg"]:
                    st.code(row["error_msg"], language="text")
