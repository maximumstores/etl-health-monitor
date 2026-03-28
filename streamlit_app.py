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
# Sidebar — База даних
# ============================================
def render_sidebar_db():
    st.sidebar.markdown("## 🗄️ База даних")

    if st.sidebar.button("🔄 Оновити БД інфо"):
        st.cache_resource.clear()
        st.rerun()

    # ── Загальний розмір БД ──
    db_size = run_query("""
        SELECT
            pg_database.datname as db_name,
            pg_size_pretty(pg_database_size(pg_database.datname)) as size,
            pg_database_size(pg_database.datname) as size_bytes
        FROM pg_database
        WHERE pg_database.datname = current_database()
    """)
    if not db_size.empty:
        st.sidebar.metric("📦 Розмір БД", db_size.iloc[0]["size"])

    # ── Кількість таблиць ──
    tables_info = run_query("""
        SELECT
            schemaname || '.' || relname as table_name,
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
        st.sidebar.metric("📊 Таблиць", f"{total_tables}")
        st.sidebar.metric("📝 Рядків (всього)", f"{total_rows:,.0f}")

    st.sidebar.divider()

    # ── Останній запис по кожній таблиці ──
    freshness = run_query("""
        WITH table_list AS (
            SELECT
                schemaname || '.' || relname as table_name,
                relname,
                schemaname
            FROM pg_stat_user_tables
        )
        SELECT table_name FROM table_list ORDER BY table_name
    """)

    # Для ключових таблиць визначаємо колонку дати
    date_columns = {
        "public.loader_runs": "started_at",
        "public.orders": "purchase_date",
        "public.fba_inventory": "updated_at",
        "public.finance_events": "posted_date",
        "public.finance_settlements": "posted_date",
        "public.finance_event_groups": "updated_at",
        "public.fba_returns": "return_date",
        "public.listings_all": "updated_at",
        "public.catalog_items": "updated_at",
        "public.sales_traffic": "date",
        "public.pricing_current": "updated_at",
        "public.pricing_offers": "updated_at",
        "public.pricing_buybox": "updated_at",
        "public.fba_shipments": "updated_at",
        "public.fba_shipment_items": "updated_at",
        "public.fba_removals": "updated_at",
        "public.ads_campaigns": "updated_at",
        "public.ads_keywords": "updated_at",
        "public.pending_reports": "created_at",
    }

    # Пробуємо знайти останній запис для кожної таблиці
    last_writes = {}
    for _, row in freshness.iterrows():
        tname = row["table_name"]
        # Спочатку спробуємо відому колонку
        date_col = date_columns.get(tname)
        if date_col:
            try:
                res = run_query(f"""
                    SELECT MAX({date_col}) AT TIME ZONE 'Europe/Kyiv' as last_write
                    FROM {tname}
                """)
                if not res.empty and pd.notna(res.iloc[0]["last_write"]):
                    last_writes[tname] = res.iloc[0]["last_write"]
                    continue
            except:
                pass
        # Fallback: шукаємо будь-яку timestamp колонку
        try:
            cols = run_query("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema || '.' || table_name = %s
                  AND data_type IN ('timestamp with time zone', 'timestamp without time zone')
                ORDER BY ordinal_position
                LIMIT 1
            """, (tname,))
            if not cols.empty:
                col = cols.iloc[0]["column_name"]
                res = run_query(f"""
                    SELECT MAX({col}) AT TIME ZONE 'Europe/Kyiv' as last_write
                    FROM {tname}
                """)
                if not res.empty and pd.notna(res.iloc[0]["last_write"]):
                    last_writes[tname] = res.iloc[0]["last_write"]
        except:
            pass

    # ── Таблиця в сайдбарі ──
    st.sidebar.markdown("### 📋 Таблиці")

    if not tables_info.empty:
        for _, row in tables_info.iterrows():
            tname = row["table_name"]
            rows = int(row["rows"]) if pd.notna(row["rows"]) else 0
            size = row["total_size"]
            last = last_writes.get(tname)

            if last is not None:
                if isinstance(last, str):
                    last_str = last[:16]
                else:
                    last_str = last.strftime("%d.%m %H:%M")
                ago = ""
                try:
                    now = datetime.now(KYIV_TZ)
                    if isinstance(last, str):
                        last_dt = pd.to_datetime(last)
                    else:
                        last_dt = last
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=KYIV_TZ)
                    diff = now - last_dt
                    if diff.days > 0:
                        ago = f" ({diff.days}д тому)"
                    elif diff.seconds > 3600:
                        ago = f" ({diff.seconds // 3600}г тому)"
                    else:
                        ago = f" ({diff.seconds // 60}хв тому)"
                except:
                    ago = ""
                fresh_icon = "🟢" if ago and ("хв" in ago or ("г" in ago and int(ago.split("(")[1].split("г")[0]) < 6)) else "🟡" if "д" not in ago else "🔴"
            else:
                last_str = "—"
                ago = ""
                fresh_icon = "⚪"

            short_name = tname.replace("public.", "")
            st.sidebar.markdown(
                f"**{fresh_icon} {short_name}**  \n"
                f"📊 `{rows:,}` rows · 💾 `{size}`  \n"
                f"🕒 {last_str}{ago}"
            )
            st.sidebar.markdown("---")

    # ── Розмір по схемам ──
    schema_sizes = run_query("""
        SELECT
            schemaname as schema,
            COUNT(*) as tables,
            pg_size_pretty(SUM(pg_total_relation_size(relid))) as total_size
        FROM pg_stat_user_tables
        GROUP BY schemaname
        ORDER BY SUM(pg_total_relation_size(relid)) DESC
    """)
    if not schema_sizes.empty and len(schema_sizes) > 1:
        st.sidebar.markdown("### 📁 Схеми")
        for _, row in schema_sizes.iterrows():
            st.sidebar.markdown(
                f"**{row['schema']}** — {row['tables']} таблиць · {row['total_size']}"
            )

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
# Sidebar
# ============================================
render_sidebar_db()

# ============================================
# Main page — ETL Health
# ============================================
st.markdown("## 🏥 ETL Health Monitor")

col_period, col_refresh = st.columns([3, 1])
with col_period:
    days = st.selectbox("Період", [1, 3, 7, 14, 30],
                        index=2, format_func=lambda d: f"Останні {d} днів")
with col_refresh:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Оновити"):
        st.cache_resource.clear()
        st.rerun()

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
