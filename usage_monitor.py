"""
usage_monitor.py — «Активність дашбордів» для ETL Monitor (UK / RU / EN).

Збирає в одному місці те, що кожен тул показує на своїй вкладці «Активність дашборда»:
  * Регулярність (це число йде в Scorecard) + динаміка в п.п. до попереднього періоду
  * «Зайшли X з Y» і середня кількість днів із входом
  * вибір періоду 7 / 14 / 30 / 60 днів
  * мініграфік за 4 тижні, останній вхід, статус
  * матриця «хто чим користується», регулярність по тижнях (пн–нд) для Scorecard

Формула (та сама, що на вкладці BSR Radar):
    днів_у_людини       = кількість різних робочих днів (пн–пт) із входом у періоді (київський час)
    робочих_днів        = пн–пт у періоді (для 7 днів це завжди 5)
    регулярність_людини = min(днів_у_людини / робочих_днів, 1)
    Регулярність        = середнє по всіх, хто хоч раз заходив у тул до кінця періоду

Вимога до кожного тулу: таблиця <schema>.login_log (email TEXT, logged_in_at TIMESTAMPTZ).
Підключення до БД: змінна DATABASE_URL; запасний варіант — st.secrets["db"].

У app.py:  apply_style(), sidebar_controls() (повертає ключ сторінки), show_usage_monitor().
"""

import os
from html import escape

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
    {"name": "BSR Radar", "schema": "bsr_radar", "url": "https://competitor-bsr.streamlit.app", "desc": {"uk": "Динаміка BSR та позицій по ASIN і конкурентах", "ru": "Динамика BSR и позиций по ASIN и конкурентам", "en": "BSR and rank trends for ASINs and competitors"}, "goal": 80},
    {"name": "Rating Radar", "schema": "public", "url": "https://rating-radar.streamlit.app", "desc": {"uk": "Рейтинги child-ASIN, прогноз тренду й алерти", "ru": "Рейтинги child-ASIN, прогноз тренда и алерты", "en": "Child-ASIN ratings, trend forecast and alerts"}, "goal": 80},  # login_log у public
    # Forecast пише login_log у BigQuery, а не в Postgres → потрібен секрет [gcp_service_account]
    {"name": "Forecast", "bq_table": "reorder-497714.forecast.login_log",
     "url": "https://forecast-merino.streamlit.app", "desc": {"uk": "План продажів і прогноз по групах", "ru": "План продаж и прогноз по группам", "en": "Sales plan and forecast by group"}, "goal": 80},
    {"name": "FBA Replenishment", "bq_table": "reorder-497714.fba_replenishment.login_log",
     "url": "https://fba-replenishment.streamlit.app", "desc": {"uk": "Що поповнити на FBA: ASIN, покриття, алерти", "ru": "Что пополнить на FBA: ASIN, покрытие, алерты", "en": "What to replenish on FBA: ASINs, coverage, alerts"}, "goal": 80},
    {"name": "Insights Engine", "schema": "insights_radar",
     "url": "https://insights-engine-radar.streamlit.app", "goal": 80,
     "desc": {"uk": "Інсайти з відгуків і запитів клієнтів", "ru": "Инсайты из отзывов и запросов клиентов",
              "en": "Insights from customer reviews and queries"}},
    {"name": "AEO Radar", "schema": "aeo", "url": "https://aeo-monitor.streamlit.app", "goal": 80,
     "desc": {"uk": "Видимість бренду у відповідях AI (Share of Voice)", "ru": "Видимость бренда в ответах AI (Share of Voice)",
              "en": "Brand visibility in AI answers (Share of Voice)"}},
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
        "goal_line": "🎯 Ціль {goal}% · зараз {reg}%", "goal_left": "ще {n} п.п.", "goal_ok": "ціль досягнута ✅", "inactive": "Не заходили за період", "all_active": "Усі заходили ✅", "scorecard_line": "Рядок для Scorecard", "trend4": "4 тижні, %", "incl_admins": "Враховувати розробників (адмінів)", "desc": "Опис", "prev_period": "Попередній період", "days_short": "Днів", "pp": "п.п.", "more": "Детальніше", "col_days": "Дні", "col_logins": "Входів", "col_last": "Останній вхід", "weeks8": "Регулярність по тижнях", "open": "Відкрити", "chart": "Динаміка по тижнях · регулярність, %", "never": "ніколи", "today": "сьогодні", "ago": "{n} дн тому",
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
        "goal_line": "🎯 Цель {goal}% · сейчас {reg}%", "goal_left": "ещё {n} п.п.", "goal_ok": "цель достигнута ✅", "inactive": "Не заходили за период", "all_active": "Все заходили ✅", "scorecard_line": "Строка для Scorecard", "trend4": "4 недели, %", "incl_admins": "Учитывать разработчиков (админов)", "desc": "Описание", "prev_period": "Предыдущий период", "days_short": "Дней", "pp": "п.п.", "more": "Подробнее", "col_days": "Дни", "col_logins": "Входов", "col_last": "Последний вход", "weeks8": "Регулярность по неделям", "open": "Открыть", "chart": "Динамика по неделям · регулярность, %", "never": "никогда", "today": "сегодня", "ago": "{n} дн назад",
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
        "goal_line": "🎯 Goal {goal}% · now {reg}%", "goal_left": "{n} pp to go", "goal_ok": "goal reached ✅", "inactive": "No logins in period", "all_active": "Everyone logged in ✅", "scorecard_line": "Scorecard line", "trend4": "4 weeks, %", "incl_admins": "Include developers (admins)", "desc": "Description", "prev_period": "Previous period", "days_short": "Days", "pp": "pp", "more": "Details", "col_days": "Days", "col_logins": "Logins", "col_last": "Last login", "weeks8": "Regularity by week", "open": "Open", "chart": "Weekly trend · regularity, %", "never": "never", "today": "today", "ago": "{n} d ago",
    },
}


def tool_desc(tool: dict) -> str:
    d = tool.get("desc") or {}
    return d.get(st.session_state.get("lang", "uk")) or d.get("uk", "")


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

/* --- картки тулів (компактний стиль) --- */
.mk-card { border: 1px solid rgba(128,128,128,.28); border-top: 3px solid #1d4ed8; border-radius: 2px;
           padding: 14px 16px 12px; margin-bottom: 14px; }
.mk-head { display: flex; align-items: center; gap: 8px; }
.mk-dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; flex: none; }
.mk-dot.g { background: #16a34a; } .mk-dot.y { background: #d97706; } .mk-dot.r { background: #dc2626; }
.mk-name { font-weight: 600; font-size: .98rem; text-decoration: none !important; color: inherit !important;
           border-bottom: 1px solid rgba(128,128,128,.5); }
.mk-name:hover { color: #1d4ed8 !important; border-color: #1d4ed8; }
.mk-last { margin-left: auto; font-size: .72rem; opacity: .55; white-space: nowrap; }
.mk-desc { font-size: .88rem; font-weight: 500; opacity: .92; margin: 4px 0 12px; line-height: 1.35; }
.mk-more { border-top: 1px solid rgba(128,128,128,.22); margin-top: 8px; padding-top: 7px; }
.mk-more summary { cursor: pointer; font-size: .76rem; font-weight: 600; color: #1d4ed8; list-style: none; }
.mk-more summary::-webkit-details-marker { display: none; }
.mk-more summary::after { content: " ▾"; }
.mk-more[open] summary::after { content: " ▴"; }
.mk-tbl { width: 100%; border-collapse: collapse; font-size: .76rem; margin-top: 8px; }
.mk-tbl th { text-align: left; font-weight: 500; font-size: .62rem; text-transform: uppercase; letter-spacing: .06em;
             opacity: .55; padding: 2px 0; }
.mk-tbl td { padding: 4px 0; border-top: 1px solid rgba(128,128,128,.15); }
.mk-tbl td.n, .mk-tbl th.n { text-align: right; }
.mk-sub { font-size: .62rem; text-transform: uppercase; letter-spacing: .06em; opacity: .55; margin: 10px 0 2px; }
.mk-main { display: flex; align-items: flex-end; gap: 12px; }
.mk-num { font-family: Georgia, 'Times New Roman', serif; font-size: 2.6rem; line-height: 1; letter-spacing: -0.02em; }
.mk-num span { font-size: 1.2rem; opacity: .6; margin-left: 2px; }
.mk-delta { font-size: .78rem; font-weight: 600; padding-bottom: 5px; }
.mk-delta.up { color: #16a34a; } .mk-delta.dn { color: #dc2626; } .mk-delta.z { opacity: .5; }
.mk-spark { margin-left: auto; display: block; }
.mk-goal { margin: 10px 0 2px; }
.mk-bar { position: relative; height: 4px; background: rgba(128,128,128,.22); border-radius: 2px; }
.mk-bar i { position: absolute; left: 0; top: 0; bottom: 0; background: #1d4ed8; border-radius: 2px; }
.mk-bar b { position: absolute; top: -3px; width: 2px; height: 10px; background: currentColor; opacity: .7; }
.mk-goal small { display: block; font-size: .72rem; opacity: .65; margin-top: 5px; }
.mk-stats { display: flex; border-top: 1px solid rgba(128,128,128,.22); margin-top: 10px; padding-top: 8px; }
.mk-stats > div { flex: 1; }
.mk-stats label { display: block; font-size: .64rem; text-transform: uppercase; letter-spacing: .06em; opacity: .55; }
.mk-stats b { font-size: .98rem; font-weight: 600; }
.mk-foot { border-top: 1px solid rgba(128,128,128,.22); margin-top: 8px; padding-top: 7px;
           font-size: .74rem; line-height: 1.5; }
.mk-foot label { text-transform: uppercase; letter-spacing: .06em; font-size: .64rem; opacity: .55; margin-right: 6px; }
.mk-sc { font-family: ui-monospace, Menlo, monospace; font-size: .74rem; opacity: .7; user-select: all;
         display: block; margin-top: 4px; }
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
    # єдиний стандарт: у «днях» рахуються тільки робочі дні пн–пт; «зайшли» — будь-який вхід у періоді
    in_win_wd = in_win[in_win["d"].dt.weekday < 5]
    days_per_user = in_win_wd.groupby("email")["d"].nunique().reindex(base_users, fill_value=0)
    entered = int(in_win["email"].nunique())

    reg = (days_per_user / wd).clip(upper=1).mean() * 100
    return {
        "entered": entered,
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

    people_rows = []
    if not df.empty:
        win = df[(df["d"] >= d_start) & (df["d"] <= today)]
        days_by = win[win["d"].dt.weekday < 5].groupby("email")["d"].nunique()
        logins_by = win.groupby("email").size()
        last_by = df.groupby("email")["ts"].max()
        for e in base_emails:
            people_rows.append({"user": e.split("@")[0], "days": int(days_by.get(e, 0)),
                                "logins": int(logins_by.get(e, 0)), "last": last_by[e]})
        people_rows.sort(key=lambda x: (-x["days"], -x["logins"], x["user"]))

    return {
        **cur, "inactive": inactive, "today": today, "people_rows": people_rows,
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

def _spark(values) -> str:
    """Мініграфік за 4 тижні: стовпчики 0–100%, останній — акцентний."""
    w, h, bw, gap = 64, 28, 12, 5
    out = []
    for i, v in enumerate(values):
        bh = max(2.0, h * min(max(v, 0), 100) / 100)
        fill = "#1d4ed8" if i == len(values) - 1 else "rgba(128,128,128,.45)"
        out.append(f'<rect x="{i * (bw + gap)}" y="{h - bh:.1f}" width="{bw}" height="{bh:.1f}" fill="{fill}"/>')
    return f'<svg class="mk-spark" width="{w}" height="{h}" viewBox="0 0 {w} {h}">{"".join(out)}</svg>'


def card_html(name: str, r: dict, url: str, desc: str, goal) -> str:
    of = t("of")
    dot = {"🟢": "g", "🟡": "y", "🔴": "r"}[status_icon(r["days_idle"])]
    title = f'<a class="mk-name" href="{escape(url)}" target="_blank">{escape(name)} ↗</a>' if url \
        else f'<span class="mk-name" style="border:0">{escape(name)}</span>'
    d = r["delta"]
    dcls, arrow = ("up", "▲") if d > 0 else (("dn", "▼") if d < 0 else ("z", "•"))
    goal_html = ""
    if goal:
        left = goal - r["reg"]
        note = t("goal_ok") if left <= 0 else t("goal_left", n=f"{left:.0f}")
        goal_text = t("goal_line", goal=goal, reg=f"{r['reg']:.0f}")
        goal_html = (f'<div class="mk-goal"><div class="mk-bar"><i style="width:{min(r["reg"] / goal, 1) * 100:.0f}%"></i>'
                     f'<b style="left:calc(100% - 2px)"></b></div>'
                     f'<small>{goal_text} · {note}</small></div>')
    inactive = ", ".join(escape(u) for u in r["inactive"]) if r["inactive"] else t("all_active")
    rows = "".join(
        f'<tr><td>{escape(p["user"])}</td><td class="n">{p["days"]} {of} {r["workdays"]}</td>'
        f'<td class="n">{p["logins"]}</td><td class="n">{p["last"]:%d.%m %H:%M}</td></tr>'
        for p in r["people_rows"])
    wk = "".join(
        f'<tr><td>{escape(k.replace("з ", t("from") + " ", 1))}</td><td class="n">{v:.0f}%</td></tr>'
        for k, v in reversed(list(r["weeks"].items())))
    more = (
        f'<details class="mk-more"><summary>{t("more")}</summary>'
        f'<table class="mk-tbl"><tr><th>{t("employee")}</th><th class="n">{t("col_days")}</th>'
        f'<th class="n">{t("col_logins")}</th><th class="n">{t("col_last")}</th></tr>{rows}</table>'
        f'<div class="mk-sub">{t("weeks8")}</div><table class="mk-tbl">{wk}</table></details>'
    )
    return (
        f'<div class="mk-card">'
        f'<div class="mk-head"><span class="mk-dot {dot}"></span>{title}<span class="mk-last">{idle_text(r["days_idle"])}</span></div>'
        f'<div class="mk-desc">{escape(desc)}</div>'
        f'<div class="mk-main"><div class="mk-num">{r["reg"]:.0f}<span>%</span></div>'
        f'<div class="mk-delta {dcls}">{arrow} {d:+.0f} {t("pp")}</div>'
        f'{_spark(list(r["weeks"].values())[-4:])}</div>'
        f'{goal_html}'
        f'<div class="mk-stats">'
        f'<div><label>{t("entered")}</label><b>{r["entered"]} {of} {r["base"]}</b></div>'
        f'<div><label>{t("days_short")}</label><b>{r["avg_days"]:g} {of} {r["workdays"]}</b></div>'
        f'<div><label>{t("prev_period")}</label><b>{r["reg"] - d:.0f}%</b></div></div>'
        f'<div class="mk-foot"><label>{t("inactive")}</label>{inactive}'
        f'<span class="mk-sc">{r["today"]:%Y-%m-%d} — {r["reg"]:.0f}%</span></div>'
        f'{more}</div>'
    )


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
    descs = {tl["name"]: tool_desc(tl) for tl in TOOLS}
    names = list(results)
    for i in range(0, len(names), 2):
        cols = st.columns(2)
        for col, name in zip(cols, names[i:i + 2]):
            col.markdown(card_html(name, results[name], tool_url.get(name, ""),
                                   descs.get(name, ""), goals.get(name)), unsafe_allow_html=True)

    # --- Зведення ---
    st.subheader(t("summary"))
    rows = []
    for name, r in results.items():
        rows.append({
            # посилання з #назвою: LinkColumn показує лише текст після #, клік відкриває тул
            t("tool"): (tool_url.get(name) or "https://etl-health-monitor.streamlit.app") + "#" + name,
            t("desc"): descs.get(name, ""),
            t("regularity") + " %": r["reg"],
            t("dyn"): r["delta"],
            t("trend"): list(r["weeks"].values())[-4:],
            t("last_login"): idle_text(r["days_idle"]),
            t("status"): status_icon(r["days_idle"]),
        })
    reg_col, dyn_col, trend_col = t("regularity") + " %", t("dyn"), t("trend")
    st.dataframe(
        pd.DataFrame(rows), hide_index=True, use_container_width=True,
        column_config={
            t("tool"): st.column_config.LinkColumn(t("tool"), display_text=r"#(.*)$", width="medium"),
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
