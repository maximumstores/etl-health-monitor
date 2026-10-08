"""
usage_monitor.py — «Активність дашбордів» для ETL Monitor (UK / RU / EN).

Збирає в одному місці те, що кожен тул показує на своїй вкладці «Активність дашборда»:
  * Регулярність (це число йде в Scorecard) + динаміка в п.п. до попереднього періоду
  * «Зайшли X з Y» і середня кількість днів із входом
  * вибір періоду 7 / 14 / 30 / 60 днів
  * мініграфік за 4 тижні, останній вхід, статус
  * матриця «хто чим користується», регулярність по тижнях (пн–нд) для Scorecard

Формула (та сама, що на вкладці BSR Radar):
    днів_у_людини       = кількість різних днів із входом у періоді (київський час)
    робочих_днів        = пн–пт у періоді (для 7 днів це завжди 5)
    регулярність_людини = min(днів_у_людини / робочих_днів, 1)
    Регулярність        = середнє по всіх, хто хоч раз заходив у тул до кінця періоду

Вимога до кожного тулу: таблиця <schema>.login_log (email TEXT, logged_in_at TIMESTAMPTZ).
Підключення до БД: змінна DATABASE_URL; запасний варіант — st.secrets["db"].

У app.py:  apply_style(), sidebar_controls() (повертає ключ сторінки), show_usage_monitor().
"""

import os

import pandas as pd
import psycopg2
import streamlit as st

TZ = "Europe/Kyiv"
PERIODS = [7, 14, 30, 60]

# Розробники / адміни: їхні входи (тестування, дебаг) не рахуються в метриках. Ім'я до @.
ADMIN_USERS = {"a.borulko", "v.tereshyn"}

# goal — ціль регулярності з Scorecard (%). Додати тул = додати рядок. Тули на Google Sheets — теж сюди, коли вони пишуть login_log.
TOOLS = [
    # {"name": "Kabinet", "schema": "kabinet"},   # додати, коли буде відомо, де лежить його login_log
    {"name": "BSR Radar", "schema": "bsr_radar", "url": "https://competitor-bsr.streamlit.app", "goal": 80},
    {"name": "Rating Radar", "schema": "public", "url": "https://rating-radar.streamlit.app", "goal": 80},  # login_log у public
    # Forecast пише login_log у BigQuery, а не в Postgres → потрібен секрет [gcp_service_account]
    {"name": "Forecast", "bq_table": "reorder-497714.forecast.login_log",
     "url": "https://forecast-merino.streamlit.app", "goal": 80},
    # {"name": "Check Parent Rating", "schema": "check_parent_rating"},   # Google Sheets
]


# ──────────────────────────────────────────────
# Переклади
# ──────────────────────────────────────────────

LANGS = {"uk": "УКР", "ru": "РУС", "en": "ENG"}

I18N = {
    "uk": {
        "nav_health": "ETL Health", "nav_db": "База даних", "nav_arch": "Архітектура",
        "nav_usage": "Активність дашбордів", "refresh": "Оновити",
        "title": "Активність дашбордів",
        "subtitle": "Час київський. Регулярність — середня частка робочих днів із входом серед людей, "
                    "які хоч раз заходили в тул. Динаміка — до попереднього періоду такої ж довжини.",
        "period": "Період", "days_n": "{n} дн.", "of": "з",
        "k_tools": "Тулів у моніторингу", "k_avg": "Середня регулярність", "k_idle": "Без входів 7+ днів",
        "scorecard": "% для Scorecard · останні {n} дн.",
        "regularity": "Регулярність", "delta": "п.п. до попереднього періоду",
        "entered": "Зайшли", "avg_days": "Днів у середньому",
        "summary": "Зведення", "tool": "Інструмент", "trend": "4 тижні",
        "last_login": "Останній вхід", "status": "Статус", "dyn": "Динаміка, п.п.",
        "who": "Хто чим користується · входів за {n} дн.", "employee": "Співробітник",
        "total": "Всього", "no_logins": "За {n} дн. входів не було.",
        "weekly": "% для Scorecard по тижнях", "week": "Тиждень (пн–нд)", "from": "з",
        "weekly_note": "Для Scorecard бери завершений тиждень: верхній рядок рахується за пройдені "
                       "дні поточного тижня і ще зміниться.",
        "no_data": "Немає даних: жоден тул ще не пише login_log.",
        "read_error": "Не вдалось прочитати логи — {msg}",
        "where": "Де в БД таблиці входів (для налаштування TOOLS)",
        "no_tables": "Таблиць, схожих на login / session / page_view, не знайдено.",
        "schema_error": "Не вдалось переглянути схему БД: {e}",
        "goal_line": "🎯 Ціль {goal}% · зараз {reg}%", "goal_left": "ще {n} п.п.", "goal_ok": "ціль досягнута ✅", "inactive": "Не заходили за період", "all_active": "Усі заходили ✅", "scorecard_line": "Рядок для Scorecard", "trend4": "4 тижні, %", "incl_admins": "Враховувати розробників (адмінів)", "open": "Відкрити", "chart": "Динаміка по тижнях · регулярність, %", "never": "ніколи", "today": "сьогодні", "ago": "{n} дн тому",
    },
    "ru": {
        "nav_health": "ETL Health", "nav_db": "База данных", "nav_arch": "Архитектура",
        "nav_usage": "Активность дашбордов", "refresh": "Обновить",
        "title": "Активность дашбордов",
        "subtitle": "Время киевское. Регулярность — средняя доля рабочих дней со входом среди людей, "
                    "которые хоть раз заходили в тул. Динамика — к предыдущему периоду такой же длины.",
        "period": "Период", "days_n": "{n} дн.", "of": "из",
        "k_tools": "Тулов в мониторинге", "k_avg": "Средняя регулярность", "k_idle": "Без входов 7+ дней",
        "scorecard": "% для Scorecard · последние {n} дн.",
        "regularity": "Регулярность", "delta": "п.п. к предыдущему периоду",
        "entered": "Зашли", "avg_days": "Дней в среднем",
        "summary": "Сводка", "tool": "Инструмент", "trend": "4 недели",
        "last_login": "Последний вход", "status": "Статус", "dyn": "Динамика, п.п.",
        "who": "Кто чем пользуется · входов за {n} дн.", "employee": "Сотрудник",
        "total": "Всего", "no_logins": "За {n} дн. входов не было.",
        "weekly": "% для Scorecard по неделям", "week": "Неделя (пн–вс)", "from": "с",
        "weekly_note": "Для Scorecard бери завершённую неделю: верхняя строка считается за прошедшие "
                       "дни текущей недели и ещё изменится.",
        "no_data": "Нет данных: ни один тул ещё не пишет login_log.",
        "read_error": "Не удалось прочитать логи — {msg}",
        "where": "Где в БД таблицы входов (для настройки TOOLS)",
        "no_tables": "Таблиц, похожих на login / session / page_view, не найдено.",
        "schema_error": "Не удалось просмотреть схему БД: {e}",
        "goal_line": "🎯 Цель {goal}% · сейчас {reg}%", "goal_left": "ещё {n} п.п.", "goal_ok": "цель достигнута ✅", "inactive": "Не заходили за период", "all_active": "Все заходили ✅", "scorecard_line": "Строка для Scorecard", "trend4": "4 недели, %", "incl_admins": "Учитывать разработчиков (админов)", "open": "Открыть", "chart": "Динамика по неделям · регулярность, %", "never": "никогда", "today": "сегодня", "ago": "{n} дн назад",
    },
    "en": {
        "nav_health": "ETL Health", "nav_db": "Database", "nav_arch": "Architecture",
        "nav_usage": "Dashboard activity", "refresh": "Refresh",
        "title": "Dashboard activity",
        "subtitle": "Kyiv time. Regularity is the average share of working days with a login among people "
                    "who have ever used the tool. Change is vs. the previous period of the same length.",
        "period": "Period", "days_n": "{n} d", "of": "of",
        "k_tools": "Tools monitored", "k_avg": "Average regularity", "k_idle": "No logins in 7+ days",
        "scorecard": "% for Scorecard · last {n} days",
        "regularity": "Regularity", "delta": "pp vs. previous period",
        "entered": "Logged in", "avg_days": "Avg. days",
        "summary": "Overview", "tool": "Tool", "trend": "4 weeks",
        "last_login": "Last login", "status": "Status", "dyn": "Change, pp",
        "who": "Who uses what · logins in {n} days", "employee": "Employee",
        "total": "Total", "no_logins": "No logins in the last {n} days.",
        "weekly": "% for Scorecard by week", "week": "Week (Mon–Sun)", "from": "from",
        "weekly_note": "Use a completed week for Scorecard: the top row covers the elapsed days "
                       "of the current week and will still change.",
        "no_data": "No data: no tool writes login_log yet.",
        "read_error": "Could not read logs — {msg}",
        "where": "Where login tables live in the DB (for configuring TOOLS)",
        "no_tables": "No tables that look like login / session / page_view were found.",
        "schema_error": "Could not inspect the DB schema: {e}",
        "goal_line": "🎯 Goal {goal}% · now {reg}%", "goal_left": "{n} pp to go", "goal_ok": "goal reached ✅", "inactive": "No logins in period", "all_active": "Everyone logged in ✅", "scorecard_line": "Scorecard line", "trend4": "4 weeks, %", "incl_admins": "Include developers (admins)", "open": "Open", "chart": "Weekly trend · regularity, %", "never": "never", "today": "today", "ago": "{n} d ago",
    },
}


def t(key: str, **kw) -> str:
    lang = st.session_state.get("lang", "uk")
    text = I18N.get(lang, I18N["uk"]).get(key) or I18N["uk"][key]
    return text.format(**kw) if kw else text


# ──────────────────────────────────────────────
# Стиль і навігація (спільні для всього застосунку)
# ──────────────────────────────────────────────

_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
.stApp { font-family: 'Inter', -apple-system, 'Segoe UI', sans-serif; }
.block-container { padding-top: 2.4rem; max-width: 1180px; }
h1 { font-size: 1.7rem !important; font-weight: 600 !important; letter-spacing: -0.02em; }
h2, h3 { font-size: 1.05rem !important; font-weight: 600 !important; letter-spacing: -0.01em;
         margin-top: 1.6rem !important; }
[data-testid="stHeaderActionElements"] { display: none; }
header[data-testid="stHeader"] { background: transparent; }
section[data-testid="stSidebar"] { border-right: 1px solid rgba(128,128,128,.16); }
[data-testid="stMetricLabel"] p { font-size: .72rem; text-transform: uppercase;
         letter-spacing: .05em; opacity: .6; }
[data-testid="stMetricValue"] { font-size: 2rem; font-weight: 600; letter-spacing: -0.02em; }
[data-testid="stMetricDelta"] { font-size: .8rem; }
div[data-testid="stVerticalBlockBorderWrapper"]:has(> div > [data-testid="stVerticalBlock"] [data-testid="stMetric"]) {
         border-radius: 14px; border-color: rgba(128,128,128,.2); }
[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
.um-caption { font-size: .85rem; opacity: .6; margin: -.4rem 0 1.2rem; line-height: 1.5; }
.um-tool { font-weight: 600; font-size: 1.05rem; margin-bottom: .4rem; }
.um-open { font-size: .78rem; font-weight: 500; margin-left: .6rem; text-decoration: none;
           padding: .1rem .5rem; border: 1px solid rgba(128,128,128,.35); border-radius: 999px; }
</style>
"""


def apply_style():
    st.markdown(_CSS, unsafe_allow_html=True)


def sidebar_controls() -> str:
    """Бічне меню: навігація + мова + оновлення. Повертає ключ сторінки."""
    if "lang" not in st.session_state:
        st.session_state["lang"] = "uk"
    st.sidebar.markdown("### 📡 ETL Monitor")
    labels = {"health": "nav_health", "db": "nav_db", "arch": "nav_arch", "usage": "nav_usage"}
    icons = {"health": "🏥", "db": "🗄️", "arch": "📋", "usage": "📈"}
    page = st.sidebar.radio(
        "nav", list(labels), label_visibility="collapsed",
        format_func=lambda k: f"{icons[k]}  {t(labels[k])}",
    )
    st.sidebar.radio(
        "lang", list(LANGS), key="lang", horizontal=True, label_visibility="collapsed",
        format_func=lambda k: LANGS[k],
    )
    return page


# ──────────────────────────────────────────────
# Дані
# ──────────────────────────────────────────────

def get_conn():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    s = st.secrets["db"]
    return psycopg2.connect(
        host=s["host"], port=s["port"], dbname=s["dbname"],
        user=s["user"], password=s["password"],
    )


@st.cache_data(ttl=300, show_spinner=False)
def find_login_tables() -> pd.DataFrame:
    """Де в БД таблиці входів: schema.table, схожі на login/session/visit/audit."""
    conn = get_conn()
    try:
        return pd.read_sql(
            """SELECT table_schema AS schema, table_name AS "table"
               FROM information_schema.tables
               WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                 AND (table_name ILIKE '%login%' OR table_name ILIKE '%signin%'
                      OR table_name ILIKE '%session%' OR table_name ILIKE '%page_view%'
                      OR table_name ILIKE '%access_log%' OR table_name ILIKE '%audit%')
               ORDER BY 1, 2""",
            conn,
        )
    finally:
        conn.close()


def _bq_logins(table: str) -> pd.DataFrame:
    """login_log із BigQuery (email, logged_in_at). Ключ сервісного акаунта — st.secrets["gcp_service_account"]."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/bigquery"])
    client = bigquery.Client(credentials=creds, project=table.split(".")[0], location="EU")
    return client.query(f"SELECT email, logged_in_at FROM `{table}`").result().to_dataframe()


@st.cache_data(ttl=300, show_spinner=False)
def load_logins(schema: str = "", bq_table: str = "") -> pd.DataFrame:
    """Усі входи тулу: email, user (до @), ts (київський час), d (дата входу)."""
    if bq_table:
        df = _bq_logins(bq_table)
    else:
        conn = get_conn()
        try:
            df = pd.read_sql(f"SELECT email, logged_in_at FROM {schema}.login_log", conn)
        finally:
            conn.close()
    df["ts"] = pd.to_datetime(df["logged_in_at"], utc=True).dt.tz_convert(TZ)
    df["user"] = df["email"].astype(str).str.split("@").str[0]
    df["d"] = df["ts"].dt.tz_localize(None).dt.normalize()
    return df[["email", "user", "ts", "d"]]


# ──────────────────────────────────────────────
# Розрахунки
# ──────────────────────────────────────────────

def workdays(d_start: pd.Timestamp, d_end: pd.Timestamp) -> int:
    """Скільки пн–пт у календарному відрізку [d_start, d_end]."""
    return len(pd.bdate_range(d_start, d_end))


def window_stats(df: pd.DataFrame, d_start: pd.Timestamp, d_end: pd.Timestamp) -> dict:
    """Статистика за календарні дні [d_start, d_end] включно.

    База (Y) — ті, хто вперше зайшов не пізніше d_end. Так минулі періоди не «пливуть»,
    коли пізніше в тул приходять нові люди.
    """
    empty = {"entered": 0, "base": 0, "reg": 0.0, "avg_days": 0.0, "workdays": workdays(d_start, d_end)}
    if df.empty:
        return empty
    first_seen = df.groupby("email")["d"].min()
    base_users = first_seen[first_seen <= d_end].index
    if len(base_users) == 0:
        return empty

    wd = max(workdays(d_start, d_end), 1)
    in_win = df[(df["d"] >= d_start) & (df["d"] <= d_end) & df["email"].isin(base_users)]
    days_per_user = in_win.groupby("email")["d"].nunique().reindex(base_users, fill_value=0)

    reg = (days_per_user / wd).clip(upper=1).mean() * 100
    return {
        "entered": int((days_per_user > 0).sum()),
        "base": int(len(base_users)),
        "reg": round(float(reg), 1),
        "avg_days": round(float(days_per_user.mean()), 1),
        "workdays": wd,
    }


def summarize(df: pd.DataFrame, now: pd.Timestamp, period: int) -> dict:
    today = now.tz_localize(None).normalize()
    cur = window_stats(df, today - pd.Timedelta(days=period - 1), today)
    prev = window_stats(df, today - pd.Timedelta(days=2 * period - 1), today - pd.Timedelta(days=period))

    # тижні пн–нд, останні 8; поточний рахується за пройдені дні тижня
    monday = today - pd.Timedelta(days=today.weekday())
    weeks = {}
    for i in range(7, -1, -1):
        start = monday - pd.Timedelta(weeks=i)
        end = min(start + pd.Timedelta(days=6), today)
        weeks["з " + start.strftime("%d.%m")] = window_stats(df, start, end)["reg"]

    last_login = df["ts"].max() if not df.empty else None
    days_idle = None if last_login is None else (today - last_login.tz_localize(None).normalize()).days

    d_start = today - pd.Timedelta(days=period - 1)
    people = (
        df[df["d"] >= d_start].groupby("user").size()
        if not df.empty else pd.Series(dtype=int)
    )

    if df.empty:
        inactive = []
    else:
        first_seen = df.groupby("email")["d"].min()
        base_emails = first_seen[first_seen <= today].index
        active_emails = set(df[df["d"] >= d_start]["email"])
        inactive = sorted(e.split("@")[0] for e in base_emails if e not in active_emails)

    return {
        **cur, "inactive": inactive, "today": today,
        "delta": round(cur["reg"] - prev["reg"], 1),
        "weeks": weeks, "days_idle": days_idle, "people": people,
    }


def status_icon(days_idle):
    if days_idle is None or days_idle > 14:
        return "🔴"
    if days_idle > 7:
        return "🟡"
    return "🟢"


def idle_text(days_idle):
    if days_idle is None:
        return t("never")
    if days_idle == 0:
        return t("today")
    return t("ago", n=days_idle)


# ──────────────────────────────────────────────
# Сторінка
# ──────────────────────────────────────────────

def show_usage_monitor():
    st.title(t("title"))
    st.markdown(f'<div class="um-caption">{t("subtitle")}</div>', unsafe_allow_html=True)

    period = st.radio(
        t("period"), PERIODS, horizontal=True, index=0,
        format_func=lambda n: t("days_n", n=n),
    )
    incl_admins = st.toggle(t("incl_admins"), value=False)
    of = t("of")

    now = pd.Timestamp.now(tz=TZ)
    results, errors = {}, []
    for tool in TOOLS:
        try:
            df = load_logins(tool.get("schema", ""), tool.get("bq_table", ""))
            if not incl_admins:
                df = df[~df["user"].isin(ADMIN_USERS)]
            results[tool["name"]] = summarize(df, now, period)
        except Exception as e:  # схеми/таблиці може ще не бути — не ламаємо всю сторінку
            src = tool.get("bq_table") or f"{tool.get('schema')}.login_log"
            errors.append(f"{tool['name']} ({src}): {e}")

    for msg in errors:
        st.warning(t("read_error", msg=msg))
    if errors:
        with st.expander("🔎 " + t("where"), expanded=True):
            try:
                found = find_login_tables()
                if found.empty:
                    st.caption(t("no_tables"))
                else:
                    st.dataframe(found, hide_index=True, use_container_width=True)
            except Exception as e:
                st.caption(t("schema_error", e=e))
    if not results:
        st.info(t("no_data"))
        return

    # --- KPI ---
    avg_reg = sum(r["reg"] for r in results.values()) / len(results)
    idle_tools = sum(1 for r in results.values() if r["days_idle"] is None or r["days_idle"] > 7)
    k1, k2, k3 = st.columns(3)
    k1.metric(t("k_tools"), len(results))
    k2.metric(t("k_avg"), f"{avg_reg:.0f}%")
    k3.metric(t("k_idle"), idle_tools)

    # --- Картки тулів: те, що переноситься в Scorecard ---
    st.subheader(t("scorecard", n=period))
    tool_url = {tl["name"]: tl.get("url", "") for tl in TOOLS}
    goals = {tl["name"]: tl.get("goal") for tl in TOOLS}
    names = list(results)
    for i in range(0, len(names), 2):
        cols = st.columns(2)
        for col, name in zip(cols, names[i:i + 2]):
            r = results[name]
            with col.container(border=True):
                url = tool_url.get(name, "")
                link = f' <a class="um-open" href="{url}" target="_blank">{t("open")} ↗</a>' if url else ""
                st.markdown(
                    f'<div class="um-tool">{status_icon(r["days_idle"])}&nbsp; {name}{link}</div>',
                    unsafe_allow_html=True,
                )
                st.metric(t("regularity"), f"{r['reg']:.0f}%", f"{r['delta']:+.0f} {t('delta')}")
                goal = goals.get(name)
                if goal:
                    left = goal - r["reg"]
                    note = t("goal_ok") if left <= 0 else t("goal_left", n=f"{left:.0f}")
                    reg_txt = f"{r['reg']:.0f}"
                    st.progress(min(r["reg"] / goal, 1.0),
                                text=t("goal_line", goal=goal, reg=reg_txt) + " — " + note)
                m2, m3 = st.columns(2)
                m2.metric(t("entered"), f"{r['entered']} {of} {r['base']}")
                m3.metric(t("avg_days"), f"{r['avg_days']:g} {of} {r['workdays']}")
                st.caption(t("trend4"))
                st.bar_chart(pd.Series(list(r["weeks"].values())[-4:],
                                       index=list(r["weeks"].keys())[-4:]),
                             height=110, y_label="", x_label="")
                if r["inactive"]:
                    st.markdown(f"**{t('inactive')}:** " + ", ".join(r["inactive"]))
                else:
                    st.markdown(f"**{t('inactive')}:** {t('all_active')}")
                st.caption(t("scorecard_line"))
                st.code(f"{r['today']:%Y-%m-%d} — {r['reg']:.0f}%", language=None)

    # --- Зведення ---
    st.subheader(t("summary"))
    rows = []
    for name, r in results.items():
        rows.append({
            t("tool"): name,
            t("regularity") + " %": r["reg"],
            t("dyn"): r["delta"],
            t("trend"): list(r["weeks"].values())[-4:],
            t("last_login"): idle_text(r["days_idle"]),
            t("status"): status_icon(r["days_idle"]),
            "↗": tool_url.get(name) or None,
        })
    reg_col, dyn_col, trend_col = t("regularity") + " %", t("dyn"), t("trend")
    st.dataframe(
        pd.DataFrame(rows), hide_index=True, use_container_width=True,
        column_config={
            "↗": st.column_config.LinkColumn("↗", display_text=t("open"), width="small"),
            reg_col: st.column_config.ProgressColumn(reg_col, format="%.0f%%", min_value=0, max_value=100),
            dyn_col: st.column_config.NumberColumn(dyn_col, format="%+.0f"),
            trend_col: st.column_config.BarChartColumn(trend_col, y_min=0, y_max=100),
        },
    )

    # --- Хто чим користується ---
    st.subheader(t("who", n=period))
    matrix = pd.DataFrame({n: r["people"] for n, r in results.items()})
    if matrix.empty:
        st.caption(t("no_logins", n=period))
    else:
        matrix = matrix.fillna(0).astype(int)
        matrix.index.name = t("employee")
        matrix[t("total")] = matrix.sum(axis=1)
        st.dataframe(matrix.sort_values(t("total"), ascending=False), use_container_width=True)

    # --- Графік і таблиця по тижнях ---
    weekly = pd.DataFrame({n: r["weeks"] for n, r in results.items()})
    weekly.index = [i.replace("з ", t("from") + " ", 1) for i in weekly.index]
    st.subheader(t("chart"))
    st.line_chart(weekly, height=280)
    st.subheader(t("weekly"))
    weekly = weekly.iloc[::-1]  # свіжі тижні зверху
    weekly.index.name = t("week")
    st.dataframe(
        weekly, use_container_width=True,
        column_config={n: st.column_config.NumberColumn(n, format="%.0f%%") for n in weekly.columns},
    )
    st.caption(t("weekly_note"))


if __name__ == "__main__":
    st.set_page_config(page_title="Activity", layout="wide")
    apply_style()
    if "lang" not in st.session_state:
        st.session_state["lang"] = "uk"
    show_usage_monitor()
