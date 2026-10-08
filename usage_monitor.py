"""
usage_monitor.py — розділ «Активність дашбордів» для ETL Monitor.

Збирає в одному місці те, що кожен тул показує на своїй вкладці «Активність дашборда»:
  * Регулярність (це число йде в Scorecard) + динаміка в п.п. до попереднього періоду
  * «Зайшли X з Y» і середня кількість днів із входом
  * вибір періоду 7 / 14 / 30 / 60 днів, як у вкладках тулів
  * мініграфік за 4 тижні, останній вхід, статус (🟢 / 🟡 / 🔴)
  * матриця «хто чим користується»
  * Регулярність по тижнях (пн–нд) для переносу в Scorecard

Формула (та сама, що на вкладці BSR Radar):
    днів_у_людини   = кількість різних днів із входом у періоді (київський час)
    робочих_днів    = пн–пт у періоді (для 7 днів це завжди 5)
    регулярність_людини = min(днів_у_людини / робочих_днів, 1)
    Регулярність    = середнє по всіх, хто хоч раз заходив у тул до кінця періоду
    Зайшли X з Y    = X — хто зайшов у періоді, Y — усі, хто хоч раз заходив

Вимоги до кожного тулу: таблиця <schema>.login_log (email TEXT, logged_in_at TIMESTAMPTZ).
Тули на Google Sheets пишуть у таку саму таблицю (через скрипт Drive Activity),
тож для монітора вони виглядають так само, як дашборди.
Підключення до БД: змінна DATABASE_URL (як в ETL Monitor); запасний варіант — st.secrets["db"].

Запуск окремо для перевірки:  streamlit run usage_monitor.py
Підключення в основний app.py — див. коментар у кінці файлу.
"""

import os

import pandas as pd
import psycopg2
import streamlit as st

TZ = "Europe/Kyiv"
PERIODS = [7, 14, 30, 60]

# Додати тул = додати рядок. Тули на Google Sheets — теж сюди, коли вони пишуть login_log.
TOOLS = [
    {"name": "Kabinet", "schema": "kabinet"},      # схема, куди Kabinet пише login_log
    {"name": "BSR Radar", "schema": "bsr_radar"},
    # {"name": "Rating Radar", "schema": "rating_radar"},
    # {"name": "Check Parent Rating", "schema": "check_parent_rating"},   # Google Sheets
]


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
def load_logins(schema: str) -> pd.DataFrame:
    """Усі входи тулу: email, user (до @), ts (київський час), d (дата входу)."""
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

    return {
        **cur,
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
        return "ніколи"
    if days_idle == 0:
        return "сьогодні"
    return f"{days_idle} дн тому"


# ──────────────────────────────────────────────
# Сторінка
# ──────────────────────────────────────────────

def show_usage_monitor():
    st.title("📈 Активність дашбордів")
    top_l, top_r = st.columns([1, 3])
    with top_l:
        if st.button("🔄 Оновити"):
            st.cache_data.clear()
            st.rerun()
    with top_r:
        period = st.radio("Період", PERIODS, horizontal=True, format_func=lambda n: f"{n} дн.")
    st.caption(
        "Час київський. Регулярність = середня частка робочих днів із входом по людях, "
        "які хоч раз заходили в тул. Динаміка — до попереднього періоду такої ж довжини."
    )

    now = pd.Timestamp.now(tz=TZ)
    results, errors = {}, []
    for tool in TOOLS:
        try:
            results[tool["name"]] = summarize(load_logins(tool["schema"]), now, period)
        except Exception as e:  # схеми/таблиці може ще не бути — не ламаємо всю сторінку
            errors.append(f"{tool['name']} ({tool['schema']}.login_log): {e}")

    for msg in errors:
        st.warning(f"Не вдалось прочитати логи — {msg}")
    if not results:
        st.info("Немає даних: жоден тул ще не пише login_log.")
        return

    # --- Верхні лічильники ---
    avg_reg = sum(r["reg"] for r in results.values()) / len(results)
    idle_tools = sum(1 for r in results.values() if r["days_idle"] is None or r["days_idle"] > 7)
    c1, c2, c3 = st.columns(3)
    c1.metric("Тулів у моніторингу", len(results))
    c2.metric(f"Середня регулярність за {period} дн.", f"{avg_reg:.0f}%")
    c3.metric("Тулів без входів 7+ днів", idle_tools)

    # --- % для Scorecard по кожному тулу (як шапка вкладки в самому тулі) ---
    st.subheader(f"% для Scorecard — останні {period} днів")
    for name, r in results.items():
        st.markdown(f"**{name}**")
        m1, m2, m3 = st.columns(3)
        m1.metric("Регулярність", f"{r['reg']:.0f}%", f"{r['delta']:+.0f} п.п. до попереднього періоду")
        m2.metric("Зайшли", f"{r['entered']} з {r['base']}")
        m3.metric("У середньому днів", f"{r['avg_days']:g} з {r['workdays']}")

    # --- Зведення по тулах ---
    st.subheader("Зведення по інструментах")
    rows = []
    for name, r in results.items():
        rows.append({
            "Інструмент": name,
            f"Регулярність, % ({period} дн.)": r["reg"],
            "Динаміка, п.п.": r["delta"],
            "4 тижні": list(r["weeks"].values())[-4:],
            "Зайшли": f"{r['entered']} з {r['base']}",
            "Останній вхід": idle_text(r["days_idle"]),
            "Статус": status_icon(r["days_idle"]),
        })
    st.dataframe(
        pd.DataFrame(rows),
        hide_index=True,
        use_container_width=True,
        column_config={
            f"Регулярність, % ({period} дн.)": st.column_config.ProgressColumn(
                f"Регулярність, % ({period} дн.)", format="%.0f%%", min_value=0, max_value=100),
            "Динаміка, п.п.": st.column_config.NumberColumn("Динаміка, п.п.", format="%+.0f"),
            "4 тижні": st.column_config.BarChartColumn("4 тижні", y_min=0, y_max=100),
        },
    )

    # --- Хто чим користується ---
    st.subheader(f"Хто чим користується (входів за {period} дн.)")
    matrix = pd.DataFrame({n: r["people"] for n, r in results.items()})
    if matrix.empty:
        st.caption(f"За {period} дн. входів не було.")
    else:
        matrix = matrix.fillna(0).astype(int)
        matrix.index.name = "Співробітник"
        matrix["Всього"] = matrix.sum(axis=1)
        st.dataframe(
            matrix.sort_values("Всього", ascending=False),
            use_container_width=True,
        )

    # --- Регулярність по тижнях для Scorecard ---
    st.subheader("% для Scorecard по тижнях")
    weekly = pd.DataFrame({n: r["weeks"] for n, r in results.items()})
    weekly = weekly.iloc[::-1]  # свіжі тижні зверху
    weekly.index.name = "Тиждень (пн–нд)"
    st.dataframe(
        weekly,
        use_container_width=True,
        column_config={
            n: st.column_config.NumberColumn(n, format="%.0f%%") for n in weekly.columns
        },
    )
    st.caption(
        "Для Scorecard бери завершений тиждень: верхній рядок рахується за пройдені дні "
        "поточного тижня і ще зміниться."
    )


if __name__ == "__main__":
    show_usage_monitor()
