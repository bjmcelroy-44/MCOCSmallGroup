from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from html import escape
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st


PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = PROJECT_DIR / "smallgroup.db"
CSV_PATH = PROJECT_DIR / "bibleproject_devotionals_24.csv"
JSON_PATH = PROJECT_DIR / "bibleproject_devotionals_24.json"
BIG_IDEA_JSON_PATH = PROJECT_DIR / "bibleproject_devotionals_24_with_big_idea.json"
TOTAL_LESSONS = 24

REQUIRED_LESSON_FIELDS = [
    "week",
    "theme",
    "anchor_verse",
    "video_name",
    "video_url",
    "one_sentence_summary",
]

MEETING_STATUS_OPTIONS = ["Completed", "Skipped", "Postponed"]
QUESTION_LEVELS = ["notice", "understand", "live"]
TBD_OPTION = "TBD"
DEFAULT_FAMILY_OPTIONS = ["McElroy", "McIntosh", "Selby", "Peace", "Taylor"]
QUESTION_LABELS = {
    "notice": "Notice (kid-friendly / easy start)",
    "understand": "Understand (meaning / connect to Scripture)",
    "live": "Live it (application / discipleship)",
}

DEFAULT_QUESTION_TEMPLATES = {
    "notice": [
        "Big Idea word: From the Big Idea, what word or phrase stands out most to you? (Big Idea: \"{big_idea}\")",
        "Theme snapshot: In one word, what do you think the theme {theme} means?",
        "Video moment: What image, diagram, or moment from the video helped you understand {theme}?",
        "Anchor verse hook: When you hear {anchor_verse}, what word grabs your attention first?",
    ],
    "understand": [
        "Connect the dots: How does {anchor_verse} connect to the Big Idea: \"{big_idea}\"?",
        "God focus: What does this theme {theme} show us about what God is like?",
        "Summary unpack: The summary says \"{one_sentence_summary}\" - what part feels most important, and why?",
        "Restate it: How would you say the Big Idea in your own words (still one sentence)?",
    ],
    "live": [
        "Action step: What's one small step you can take in the next 48 hours to live out {theme}?",
        "Name a situation: Where will you likely face a moment this week to practice tonight's Big Idea?",
        "If-Then plan: If ______ happens, then I will ______ to live the Big Idea.",
        "Obstacle + help: What might get in the way, and what would help you follow through?",
    ],
}

FLOW_15_MIN = [
    "0:00-1:00 Welcome + Big idea",
    "1:00-7:30 Watch video (5-7 min)",
    "7:30-10:00 Read anchor verse reference aloud (option: 2 readers)",
    "10:00-14:00 Discussion (pick 2 questions: Level 1 + Level 2 or 3)",
    "14:00-15:00 Prayer (one-sentence 'popcorn' prayers)",
]

FLOW_SHORT = [
    "Welcome + one-sentence big idea (1 min)",
    "Watch video or key segment (4-5 min)",
    "Read anchor verse reference and ask 1 question (2-3 min)",
    "Close in prayer (1 min)",
]

FLOW_LONG = [
    "Welcome + check-in (2-3 min)",
    "Watch video and replay key moment (7-8 min)",
    "Read anchor verse reference + brief context (3-4 min)",
    "Discussion with 3-4 questions across all levels (7-8 min)",
    "Prayer in pairs or whole group (3-4 min)",
]

QUICK_OUTLINE = [
    "Welcome and state the big idea (1 minute)",
    "Read the anchor verse (NIV) together",
    "Watch the BibleProject video",
    "Discuss 2-3 questions",
    "Pray and share one next step",
]

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meeting_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_date TEXT NOT NULL,
                lesson_week INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('Completed', 'Skipped', 'Postponed')),
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_meeting_log_week ON meeting_log(lesson_week);
            CREATE INDEX IF NOT EXISTS idx_meeting_log_status ON meeting_log(status);

            CREATE TABLE IF NOT EXISTS lesson_custom_questions (
                lesson_week INTEGER NOT NULL,
                level TEXT NOT NULL CHECK (level IN ('notice', 'understand', 'live')),
                position INTEGER NOT NULL,
                question TEXT NOT NULL,
                PRIMARY KEY (lesson_week, level, position)
            );

            CREATE TABLE IF NOT EXISTS lesson_notes (
                lesson_week INTEGER PRIMARY KEY,
                notes TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS lesson_verse_text (
                lesson_week INTEGER PRIMARY KEY,
                verse_text TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS upcoming_meetings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                meeting_date TEXT NOT NULL,
                lesson_week INTEGER NOT NULL,
                host_name TEXT NOT NULL DEFAULT '',
                facilitator_name TEXT NOT NULL DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_upcoming_meetings_date ON upcoming_meetings(meeting_date);

            CREATE TABLE IF NOT EXISTS upcoming_meal_signups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                upcoming_meeting_id INTEGER NOT NULL,
                attendee_name TEXT NOT NULL DEFAULT '',
                dish TEXT NOT NULL DEFAULT '',
                position INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_upcoming_meal_meeting
            ON upcoming_meal_signups(upcoming_meeting_id);

            CREATE TABLE IF NOT EXISTS small_group_families (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                family_name TEXT NOT NULL UNIQUE,
                position INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        _ensure_column(conn, "meeting_log", "host_name", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "meeting_log", "facilitator_name", "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "upcoming_meetings", "main_meal", "TEXT NOT NULL DEFAULT ''")

        existing_count = conn.execute(
            "SELECT COUNT(*) AS count FROM small_group_families"
        ).fetchone()["count"]
        if existing_count == 0:
            for position, family_name in enumerate(DEFAULT_FAMILY_OPTIONS, start=1):
                conn.execute(
                    """
                    INSERT INTO small_group_families (family_name, position)
                    VALUES (?, ?)
                    """,
                    (family_name, position),
                )


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    existing_columns = {
        row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in existing_columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def build_sample_lessons(count: int = TOTAL_LESSONS) -> List[dict]:
    return [
        {
            "week": week,
            "theme": f"Sample Theme {week}",
            "anchor_verse": "John 3:16",
            "video_name": f"Sample Video {week}",
            "video_url": "https://bibleproject.com/",
            "one_sentence_summary": "Sample lesson summary. Add real lesson data file for production use.",
            "big_idea": f"Sample big idea {week}",
        }
        for week in range(1, count + 1)
    ]


def load_lessons() -> Tuple[pd.DataFrame, str, str]:
    warning_msg = ""

    if BIG_IDEA_JSON_PATH.exists():
        with open(BIG_IDEA_JSON_PATH, "r", encoding="utf-8") as f:
            lessons_df = pd.DataFrame(json.load(f))
        source_msg = f"Loaded lessons from {BIG_IDEA_JSON_PATH.name}"
    elif CSV_PATH.exists():
        lessons_df = pd.read_csv(CSV_PATH)
        source_msg = f"Loaded lessons from {CSV_PATH.name}"
    elif JSON_PATH.exists():
        with open(JSON_PATH, "r", encoding="utf-8") as f:
            lessons_df = pd.DataFrame(json.load(f))
        source_msg = f"Loaded lessons from {JSON_PATH.name}"
    else:
        lessons_df = pd.DataFrame(build_sample_lessons())
        source_msg = "Loaded built-in sample lessons"
        warning_msg = (
            "Lesson file not found. Add bibleproject_devotionals_24_with_big_idea.json, "
            "bibleproject_devotionals_24.csv, or bibleproject_devotionals_24.json in this folder."
        )

    missing_fields = [col for col in REQUIRED_LESSON_FIELDS if col not in lessons_df.columns]
    if missing_fields:
        raise ValueError(
            "Lesson data is missing required fields: " + ", ".join(missing_fields)
        )

    optional_fields: List[str] = []
    if "anchor_verse_text_niv" in lessons_df.columns:
        optional_fields.append("anchor_verse_text_niv")
    if "big_idea" in lessons_df.columns:
        optional_fields.append("big_idea")
    lessons_df = lessons_df[REQUIRED_LESSON_FIELDS + optional_fields].copy()
    lessons_df["week"] = pd.to_numeric(lessons_df["week"], errors="coerce")
    lessons_df = lessons_df.dropna(subset=["week"]).copy()
    lessons_df["week"] = lessons_df["week"].astype(int)

    for col in REQUIRED_LESSON_FIELDS:
        if col != "week":
            lessons_df[col] = lessons_df[col].fillna("").astype(str).str.strip()

    if "anchor_verse_text_niv" in lessons_df.columns:
        lessons_df["anchor_verse_text_niv"] = (
            lessons_df["anchor_verse_text_niv"].fillna("").astype(str).str.strip()
        )
    if "big_idea" in lessons_df.columns:
        lessons_df["big_idea"] = lessons_df["big_idea"].fillna("").astype(str).str.strip()

    lessons_df = (
        lessons_df.sort_values("week")
        .drop_duplicates(subset=["week"], keep="first")
        .reset_index(drop=True)
    )

    if "anchor_verse_text_niv" not in lessons_df.columns:
        lessons_df["anchor_verse_text_niv"] = ""
    if "big_idea" not in lessons_df.columns:
        lessons_df["big_idea"] = ""

    return lessons_df, source_msg, warning_msg


def notify(message: str, level: str = "success") -> None:
    if hasattr(st, "toast"):
        st.toast(message)
        return

    if level == "success":
        st.success(message)
    elif level == "warning":
        st.warning(message)
    elif level == "error":
        st.error(message)
    else:
        st.info(message)


def queue_message(message: str, level: str = "success") -> None:
    st.session_state["_flash_message"] = (message, level)


def show_queued_message() -> None:
    payload = st.session_state.pop("_flash_message", None)
    if payload:
        notify(payload[0], payload[1])


def mark_state_true(state_key: str) -> None:
    st.session_state[state_key] = True


def inject_global_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --sg-bg: #f6f4ef;
            --sg-surface: #fffdfa;
            --sg-surface-muted: #faf7f2;
            --sg-border: #e4ded2;
            --sg-border-strong: #cfd7d0;
            --sg-text: #20231f;
            --sg-muted: #586057;
            --sg-muted-soft: #7a8179;
            --sg-primary: #dbe4de;
            --sg-primary-strong: #cbd7d0;
            --sg-primary-soft: #edf2ee;
            --sg-primary-ink: #425147;
            --sg-success-soft: #ebf2ed;
            --sg-success-text: #4f6557;
            --sg-danger-soft: #f4eeea;
            --sg-danger-text: #75615b;
            --sg-warning-soft: #f4f0e5;
            --sg-warning-text: #76684e;
            --sg-destructive: #8d7c76;
            --sg-destructive-soft: #f4eeea;
            --sg-radius-sm: 10px;
            --sg-radius-md: 14px;
            --sg-radius-lg: 18px;
            --sg-shadow-sm: 0 1px 2px rgba(51, 40, 28, 0.05);
            --sg-shadow-md: 0 10px 22px rgba(51, 40, 28, 0.07);
        }
        .stApp {
            background: var(--sg-bg);
            color: var(--sg-text);
            font-family: "Manrope", "Avenir Next", "Segoe UI", sans-serif;
        }
        section[data-testid="stSidebar"] {
            background: var(--sg-bg) !important;
            border-right: 1px solid var(--sg-border);
        }
        section[data-testid="stSidebar"] div[data-testid="stSidebarContent"] {
            background: var(--sg-bg) !important;
            color: var(--sg-text);
        }
        section[data-testid="stSidebar"] h1,
        section[data-testid="stSidebar"] h2,
        section[data-testid="stSidebar"] h3,
        section[data-testid="stSidebar"] p,
        section[data-testid="stSidebar"] .stCaption {
            color: var(--sg-text) !important;
        }
        .main .block-container {
            max-width: 1200px;
            padding-top: 1.15rem;
            padding-bottom: 2rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        h1, h2, h3 {
            color: var(--sg-text);
            letter-spacing: -0.01em;
            font-family: "Manrope", "Avenir Next", "Segoe UI", sans-serif;
            font-weight: 620;
        }
        h1 { font-size: 1.86rem; }
        h2 { font-size: 1.34rem; }
        h3 { font-size: 1.08rem; }
        .stMarkdown, p, li, label, span {
            color: var(--sg-text);
        }
        .stMarkdown p {
            margin-bottom: 0.46rem;
            line-height: 1.5;
        }
        hr {
            border-color: var(--sg-border);
            margin: 0.7rem 0 1rem 0;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-lg);
            background: var(--sg-surface);
            box-shadow: var(--sg-shadow-sm);
        }
        div[data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: 0.18rem;
        }
        div[data-testid="stMetric"] {
            background: var(--sg-surface);
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            padding: 0.85rem 0.95rem;
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.07em;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        div[data-testid="stMetricValue"] {
            color: var(--sg-text);
            font-size: 1.4rem;
            font-weight: 620;
        }
        div[data-testid="stDataEditor"] {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            padding: 0.22rem 0.28rem;
            background: var(--sg-surface);
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
        }
        div[data-testid="stDataFrame"] [role="columnheader"],
        div[data-testid="stDataEditor"] [role="columnheader"] {
            background: var(--sg-surface-muted) !important;
            color: var(--sg-muted-soft) !important;
            font-size: 0.72rem !important;
            font-weight: 700 !important;
            letter-spacing: 0.06em !important;
            text-transform: uppercase;
            border-bottom: 1px solid var(--sg-border) !important;
        }
        div[data-testid="stDataFrame"] [role="gridcell"],
        div[data-testid="stDataEditor"] [role="gridcell"] {
            color: var(--sg-text) !important;
            border-top: 1px solid rgba(228, 222, 210, 0.8) !important;
        }
        div[data-testid="stDateInput"] label,
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextInput"] label,
        div[data-testid="stTextArea"] label,
        div[data-testid="stRadio"] label,
        div[data-testid="stFileUploader"] label {
            font-size: 0.72rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        div[data-baseweb="textarea"] > div {
            background: var(--sg-surface) !important;
            border: 1px solid var(--sg-border) !important;
            border-radius: var(--sg-radius-sm) !important;
            box-shadow: none !important;
            min-height: 2.7rem;
        }
        div[data-baseweb="select"] > div:hover,
        div[data-baseweb="input"] > div:hover,
        div[data-baseweb="textarea"] > div:hover {
            border-color: var(--sg-border-strong) !important;
        }
        div[data-baseweb="select"] > div:focus-within,
        div[data-baseweb="input"] > div:focus-within,
        div[data-baseweb="textarea"] > div:focus-within {
            border-color: #b9c8bf !important;
            box-shadow: 0 0 0 3px rgba(219, 228, 222, 0.88) !important;
        }
        div[data-baseweb="select"] * {
            color: var(--sg-text) !important;
        }
        div[data-baseweb="popover"] [role="listbox"] {
            background: var(--sg-surface) !important;
            border: 1px solid var(--sg-border) !important;
            border-radius: 10px !important;
        }
        div[data-baseweb="popover"] [role="option"] {
            color: var(--sg-text) !important;
        }
        div[data-baseweb="popover"] [role="option"][aria-selected="true"] {
            background: var(--sg-primary-soft) !important;
        }
        div[data-baseweb="popover"] [role="option"]:hover {
            background: var(--sg-surface-muted) !important;
        }
        .stButton > button,
        .stDownloadButton > button,
        .stFormSubmitButton > button {
            border-radius: var(--sg-radius-sm);
            border: 1px solid var(--sg-border);
            padding: 0.5rem 0.88rem;
            min-height: 2.55rem;
            font-weight: 600;
            letter-spacing: 0;
            background: var(--sg-surface);
            color: var(--sg-text);
            box-shadow: none;
            transition: all 120ms ease;
            font-family: "Manrope", "Avenir Next", "Segoe UI", sans-serif;
        }
        .stButton > button[kind="primary"],
        .stFormSubmitButton > button[kind="primary"] {
            background: var(--sg-primary) !important;
            border-color: #c2cdc5 !important;
            color: var(--sg-primary-ink) !important;
        }
        .stButton > button[kind="primary"]:hover,
        .stFormSubmitButton > button[kind="primary"]:hover {
            background: var(--sg-primary-strong) !important;
            border-color: #b7c5bd !important;
            color: var(--sg-primary-ink) !important;
            transform: translateY(-1px);
        }
        .stButton > button:hover,
        .stDownloadButton > button:hover,
        .stFormSubmitButton > button:hover {
            border-color: var(--sg-border-strong);
            background: var(--sg-surface-muted);
        }
        .stCaption {
            color: var(--sg-muted);
            font-size: 0.8rem;
        }
        div[data-testid="stAlert"] {
            border-radius: var(--sg-radius-md);
            border: 1px solid var(--sg-border);
            background: var(--sg-surface-muted);
        }
        div[data-baseweb="tab-list"] {
            gap: 0.5rem;
            margin: 0.45rem 0 1rem 0;
            border-bottom: none;
            padding: 0.15rem 0;
        }
        button[data-baseweb="tab"] {
            border: 1px solid var(--sg-border) !important;
            border-radius: 999px !important;
            padding: 0.42rem 0.78rem !important;
            font-size: 0.84rem !important;
            font-weight: 600 !important;
            color: var(--sg-muted) !important;
            background: var(--sg-surface) !important;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            border-color: #c1cdc5 !important;
            background: var(--sg-primary-soft) !important;
            color: var(--sg-primary-ink) !important;
        }
        div[data-baseweb="tab-highlight"] {
            background: transparent !important;
        }
        .sg-page-header {
            margin: 0.2rem 0 1rem 0;
            padding: 1.05rem 1.08rem;
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-lg);
            background: var(--sg-surface);
            box-shadow: var(--sg-shadow-sm);
        }
        .sg-page-eyebrow {
            margin: 0 0 0.25rem 0;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-page-title {
            margin: 0;
            font-size: 1.55rem;
            font-weight: 620;
            line-height: 1.2;
            color: var(--sg-text);
        }
        .sg-page-subtitle {
            margin: 0.32rem 0 0 0;
            color: var(--sg-muted);
            font-size: 0.94rem;
            line-height: 1.48;
            max-width: 70ch;
        }
        .sg-section-header {
            margin: 0.12rem 0 0.72rem 0;
        }
        .sg-section-title {
            margin: 0;
            font-size: 1.08rem;
            font-weight: 620;
            color: var(--sg-text);
            line-height: 1.35;
        }
        .sg-section-description {
            margin: 0.24rem 0 0 0;
            color: var(--sg-muted);
            font-size: 0.88rem;
            line-height: 1.45;
        }
        .sg-inline-nav-title {
            font-size: 0.72rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
            margin: 0 0 0.45rem 0;
        }
        .sg-status-badge {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.24rem 0.58rem;
            font-size: 0.75rem;
            font-weight: 620;
            border: 1px solid transparent;
        }
        .sg-status-badge-done {
            background: var(--sg-success-soft);
            color: var(--sg-success-text);
            border-color: #d7e2db;
        }
        .sg-status-badge-notdone {
            background: var(--sg-surface);
            color: var(--sg-muted);
            border-color: var(--sg-border);
        }
        .sg-status-badge-skipped {
            background: var(--sg-danger-soft);
            color: var(--sg-danger-text);
            border-color: #ead9d3;
        }
        .sg-status-badge-postponed {
            background: var(--sg-warning-soft);
            color: var(--sg-warning-text);
            border-color: #e8dcc1;
        }
        .sg-save-required,
        .sg-action-alert {
            border-radius: var(--sg-radius-sm);
            font-size: 0.81rem;
            font-weight: 600;
            padding: 0.56rem 0.72rem;
            margin: 0.32rem 0 0.68rem 0;
            border: 1px solid #ddd8ce;
            background: #f6f2eb;
            color: #4c514c;
        }
        .sg-empty-state {
            border: 1px dashed var(--sg-border-strong);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface-muted);
            padding: 1.15rem 0.9rem;
            text-align: left;
            margin-top: 0.2rem;
        }
        .sg-empty-state-title {
            margin: 0;
            font-size: 1rem;
            font-weight: 640;
            color: var(--sg-text);
        }
        .sg-empty-state-description {
            margin: 0.32rem 0 0 0;
            color: var(--sg-muted);
            font-size: 0.86rem;
            max-width: 60ch;
        }
        .sg-selection-summary {
            margin: 0.4rem 0 0.25rem 0;
            padding: 0.66rem 0.74rem;
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface-muted);
        }
        .sg-selection-summary-title {
            margin: 0;
            font-size: 0.66rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 650;
        }
        .sg-selection-summary-main {
            margin: 0.22rem 0 0 0;
            color: var(--sg-text);
            font-size: 0.9rem;
            font-weight: 540;
            line-height: 1.34;
        }
        .sg-selection-meta-grid {
            margin-top: 0.42rem;
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.34rem 0.58rem;
        }
        .sg-selection-meta-item {
            margin: 0;
            font-size: 0.74rem;
            line-height: 1.2;
            color: var(--sg-text);
            font-weight: 520;
        }
        .sg-selection-meta-label {
            color: var(--sg-muted-soft);
            font-weight: 650;
            margin-right: 0.2rem;
        }
        .sg-sunday-calendar-grid {
            display: block;
            margin: 0.25rem 0 0.55rem 0;
        }
        .sg-calendar-nav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.5rem;
            margin: 0.15rem 0 0.45rem 0;
        }
        .sg-calendar-nav-label {
            margin: 0;
            font-size: 0.92rem;
            font-weight: 620;
            color: var(--sg-text);
            text-align: center;
            flex: 1;
        }
        .sg-sunday-month {
            border: 1px solid var(--sg-border);
            border-radius: 16px;
            background: var(--sg-surface);
            padding: 0.8rem;
        }
        .sg-sunday-month-title {
            margin: 0 0 0.5rem 0;
            font-size: 0.9rem;
            font-weight: 620;
            color: var(--sg-text);
        }
        .sg-sunday-list {
            display: grid;
            grid-template-columns: 1fr;
            gap: 0.44rem;
        }
        .sg-sunday-item {
            border: 1px solid var(--sg-border);
            border-radius: 14px;
            padding: 0.5rem 0.58rem;
            min-height: 0;
            background: var(--sg-surface);
            color: var(--sg-text);
        }
        .sg-sunday-item-link {
            display: block;
            text-decoration: none !important;
            color: inherit !important;
        }
        .sg-sunday-item-clickable {
            cursor: pointer;
            transition: transform 100ms ease, box-shadow 100ms ease;
        }
        .sg-sunday-item-clickable:hover {
            transform: translateY(-1px);
            box-shadow: var(--sg-shadow-sm);
        }
        .sg-sunday-item-has-meeting {
            background: var(--sg-surface);
            border-color: var(--sg-border-strong);
            min-height: 118px;
            padding: 0.62rem 0.68rem;
        }
        .sg-sunday-item-empty {
            background: var(--sg-surface-muted);
            border-color: var(--sg-border);
            padding: 0.3rem 0.46rem;
        }
        .sg-sunday-item-selected {
            border-color: #b4c2b8;
            background: var(--sg-primary-soft);
            box-shadow: 0 0 0 2px rgba(219, 228, 222, 0.92);
        }
        .sg-sunday-date {
            margin: 0;
            font-size: 0.82rem;
            font-weight: 620;
            line-height: 1.2;
        }
        .sg-sunday-meta {
            margin: 0.2rem 0 0 0;
            font-size: 0.72rem;
            color: var(--sg-muted-soft);
            line-height: 1.2;
        }
        .sg-sunday-details {
            margin-top: 0.24rem;
            display: grid;
            gap: 0.14rem;
        }
        .sg-sunday-detail-line {
            margin: 0;
            font-size: 0.63rem;
            line-height: 1.18;
            color: var(--sg-muted);
        }
        .sg-sunday-detail-line b {
            color: var(--sg-muted-soft);
            font-weight: 700;
            margin-right: 0.12rem;
        }
        .sg-sunday-pill-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.3rem;
            margin-top: 0.32rem;
        }
        .sg-sunday-empty-label {
            margin: 0.16rem 0 0 0;
            font-size: 0.62rem;
            color: var(--sg-muted-soft);
            line-height: 1.15;
            font-weight: 600;
        }
        .sg-pill-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.38rem;
            margin-top: 0.5rem;
        }
        .sg-pill {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.24rem 0.62rem;
            font-size: 0.74rem;
            font-weight: 620;
            border: 1px solid transparent;
            line-height: 1.1;
        }
        .sg-pill-neutral {
            background: var(--sg-surface-muted);
            color: var(--sg-muted);
            border-color: var(--sg-border);
        }
        .sg-pill-open {
            background: #f2eee7;
            color: #6a6258;
            border-color: #ddd3c6;
        }
        .sg-pill-filled,
        .sg-pill-ready {
            background: var(--sg-primary-soft);
            color: var(--sg-primary-ink);
            border-color: #c8d4cc;
        }
        .sg-pill-soft {
            background: var(--sg-surface);
            color: var(--sg-muted);
            border-color: var(--sg-border);
        }
        .sg-meeting-summary {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-lg);
            background: var(--sg-surface-muted);
            padding: 0.92rem 0.95rem;
            margin: 0.12rem 0 0.85rem 0;
        }
        .sg-meeting-summary-kicker {
            margin: 0;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-meeting-summary-title {
            margin: 0.26rem 0 0 0;
            font-size: 1.18rem;
            font-weight: 620;
            color: var(--sg-text);
            line-height: 1.2;
        }
        .sg-meeting-summary-subtitle {
            margin: 0.22rem 0 0 0;
            color: var(--sg-muted);
            font-size: 0.88rem;
            line-height: 1.42;
        }
        .sg-service-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.7rem;
            margin-top: 0.8rem;
        }
        .sg-service-card {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface);
            padding: 0.82rem 0.86rem;
        }
        .sg-service-kicker {
            margin: 0;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-service-title {
            margin: 0.3rem 0 0 0;
            font-size: 0.97rem;
            font-weight: 620;
            color: var(--sg-text);
            line-height: 1.28;
        }
        .sg-service-detail {
            margin: 0.24rem 0 0.56rem 0;
            font-size: 0.82rem;
            line-height: 1.4;
            color: var(--sg-muted);
        }
        .sg-gathering-preview {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-lg);
            background: var(--sg-surface);
            padding: 0.92rem 0.96rem;
            transition: transform 100ms ease, box-shadow 100ms ease, border-color 100ms ease;
        }
        .sg-gathering-preview:hover {
            transform: translateY(-1px);
            box-shadow: var(--sg-shadow-sm);
            border-color: #b4c2b8;
        }
        .sg-gathering-preview-selected {
            background: var(--sg-primary-soft);
            border-color: #a6b5aa;
            box-shadow: 0 0 0 2px rgba(219, 228, 222, 0.92);
        }
        .sg-current-selection-banner {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 0.42rem 0.58rem;
            margin: 0.08rem 0 0.75rem 0;
            padding: 0.6rem 0.72rem;
            border: 1px solid #c8d4cc;
            border-radius: 14px;
            background: var(--sg-primary-soft);
        }
        .sg-current-selection-label {
            font-size: 0.67rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-current-selection-date {
            font-size: 0.94rem;
            font-weight: 620;
            color: var(--sg-text);
        }
        .sg-gathering-preview-head {
            margin-bottom: 0.82rem;
        }
        .sg-gathering-preview-kicker {
            margin: 0;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-gathering-preview-date {
            margin: 0.26rem 0 0 0;
            font-size: 1rem;
            font-weight: 620;
            color: var(--sg-text);
            line-height: 1.2;
        }
        .sg-gathering-preview-date a {
            color: inherit !important;
            text-decoration: none !important;
        }
        .sg-gathering-preview-lesson {
            margin: 0.18rem 0 0 0;
            font-size: 0.86rem;
            color: var(--sg-muted);
            line-height: 1.35;
        }
        .sg-gathering-preview-lesson a {
            color: inherit !important;
            text-decoration: none !important;
        }
        .sg-gathering-preview-lines {
            display: grid;
            gap: 0.38rem;
            margin-top: 0.72rem;
            padding-top: 0.72rem;
            border-top: 1px solid rgba(228, 222, 210, 0.92);
        }
        .sg-gathering-preview-line {
            display: flex;
            flex-wrap: wrap;
            align-items: baseline;
            gap: 0.24rem 0.42rem;
            margin: 0;
            line-height: 1.35;
        }
        .sg-gathering-preview-line-label {
            min-width: 82px;
            font-size: 0.69rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-gathering-preview-line-value {
            font-size: 0.88rem;
            color: var(--sg-text);
            font-weight: 560;
        }
        .sg-gathering-preview-line-value-open {
            color: #8b6f4a;
        }
        .sg-schedule-list {
            display: grid;
            gap: 0.9rem;
            margin: 0.2rem 0 0.55rem 0;
        }
        .sg-schedule-month {
            display: grid;
            gap: 0.55rem;
        }
        .sg-schedule-month-title {
            margin: 0;
            font-size: 0.96rem;
            font-weight: 620;
            color: var(--sg-text);
        }
        .sg-schedule-entry-link {
            display: block;
            text-decoration: none !important;
            color: inherit !important;
        }
        .sg-schedule-entry {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface);
            padding: 0.84rem 0.9rem;
            transition: transform 100ms ease, box-shadow 100ms ease, border-color 100ms ease;
        }
        .sg-schedule-entry:hover {
            transform: translateY(-1px);
            box-shadow: var(--sg-shadow-sm);
            border-color: #b4c2b8;
        }
        .sg-schedule-entry-selected {
            background: var(--sg-primary-soft);
            border-color: #b4c2b8;
            box-shadow: 0 0 0 2px rgba(219, 228, 222, 0.88);
        }
        .sg-schedule-entry-head {
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
            gap: 0.6rem;
            margin-bottom: 0.6rem;
        }
        .sg-schedule-date {
            margin: 0;
            font-size: 1rem;
            font-weight: 620;
            color: var(--sg-text);
            line-height: 1.2;
        }
        .sg-schedule-subtitle {
            margin: 0.16rem 0 0 0;
            font-size: 0.8rem;
            color: var(--sg-muted);
            line-height: 1.35;
        }
        .sg-schedule-activities {
            display: grid;
            gap: 0.44rem;
        }
        .sg-schedule-activity {
            border-top: 1px solid rgba(228, 222, 210, 0.9);
            padding-top: 0.44rem;
        }
        .sg-schedule-activity:first-child {
            border-top: none;
            padding-top: 0;
        }
        .sg-schedule-activity-label {
            margin: 0;
            font-size: 0.68rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-schedule-activity-value {
            margin: 0.18rem 0 0 0;
            font-size: 0.9rem;
            font-weight: 560;
            color: var(--sg-text);
            line-height: 1.3;
        }
        .sg-schedule-activity-note {
            margin: 0.14rem 0 0 0;
            font-size: 0.78rem;
            color: var(--sg-muted);
            line-height: 1.35;
        }
        .sg-schedule-footer {
            margin: 0.6rem 0 0 0;
            font-size: 0.78rem;
            color: var(--sg-muted-soft);
            line-height: 1.3;
        }
        .sg-calendar-shell {
            overflow-x: auto;
            margin: 0.25rem 0 0.6rem 0;
            padding-bottom: 0.1rem;
        }
        .sg-calendar-month {
            min-width: 760px;
            border: 1px solid var(--sg-border);
            border-radius: 16px;
            background: var(--sg-surface);
            padding: 0.85rem;
        }
        .sg-calendar-month-header {
            display: flex;
            align-items: baseline;
            justify-content: space-between;
            gap: 0.8rem;
            margin-bottom: 0.75rem;
        }
        .sg-calendar-month-title {
            margin: 0;
            font-size: 1rem;
            font-weight: 620;
            color: var(--sg-text);
        }
        .sg-calendar-month-summary {
            margin: 0;
            font-size: 0.82rem;
            color: var(--sg-muted);
            white-space: nowrap;
        }
        .sg-calendar-weekdays {
            display: grid;
            grid-template-columns: repeat(7, minmax(0, 1fr));
            gap: 0.42rem;
            margin-bottom: 0.4rem;
        }
        .sg-calendar-weekday {
            font-size: 0.67rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            padding: 0 0.14rem;
        }
        .sg-calendar-grid {
            display: grid;
            grid-template-columns: repeat(7, minmax(0, 1fr));
            gap: 0.42rem;
        }
        .sg-calendar-day-link {
            display: block;
            text-decoration: none !important;
            color: inherit !important;
        }
        .sg-calendar-day {
            min-height: 108px;
            border: 1px solid var(--sg-border);
            border-radius: 12px;
            background: var(--sg-surface-muted);
            padding: 0.36rem 0.4rem;
            transition: transform 100ms ease, box-shadow 100ms ease, border-color 100ms ease;
        }
        .sg-calendar-day-in-month {
            background: var(--sg-surface);
        }
        .sg-calendar-day-outside {
            border-color: transparent;
            background: transparent;
        }
        .sg-calendar-day-clickable {
            cursor: pointer;
        }
        .sg-calendar-day-clickable:hover {
            transform: translateY(-1px);
            box-shadow: var(--sg-shadow-sm);
            border-color: #b4c2b8;
        }
        .sg-calendar-day-has-meeting {
            background: var(--sg-primary-soft);
            border-color: #c1cdc5;
        }
        .sg-calendar-day-ready {
            background: var(--sg-primary);
            border-color: #b6c4bb;
        }
        .sg-calendar-day-selected {
            box-shadow: 0 0 0 2px rgba(219, 228, 222, 0.92);
            border-color: #8fa397;
        }
        .sg-calendar-day-number {
            margin: 0;
            font-size: 0.76rem;
            font-weight: 620;
            color: var(--sg-text);
            line-height: 1.1;
        }
        .sg-calendar-day-outside .sg-calendar-day-number {
            color: transparent;
        }
        .sg-calendar-day-title {
            margin: 0.28rem 0 0 0;
            font-size: 0.64rem;
            line-height: 1.18;
            color: var(--sg-muted);
            font-weight: 520;
        }
        .sg-calendar-day-caption {
            margin: 0.12rem 0 0 0;
            font-size: 0.6rem;
            line-height: 1.12;
            color: var(--sg-muted);
        }
        .sg-calendar-day-pill {
            margin-top: 0.18rem;
        }
        .sg-calendar-day-detail {
            margin: 0.11rem 0 0 0;
            font-size: 0.58rem;
            line-height: 1.14;
            color: var(--sg-muted);
        }
        .sg-calendar-day-detail strong {
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        .sg-calendar-day-pill .sg-pill {
            padding: 0.18rem 0.44rem;
            font-size: 0.64rem;
        }
        .sg-simple-list {
            display: grid;
            gap: 0.52rem;
        }
        .sg-simple-row {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface);
            padding: 0.72rem 0.82rem;
        }
        .sg-simple-row-title {
            margin: 0;
            font-size: 0.92rem;
            font-weight: 600;
            color: var(--sg-text);
            line-height: 1.25;
        }
        .sg-simple-row-meta {
            margin: 0.2rem 0 0 0;
            font-size: 0.8rem;
            line-height: 1.38;
            color: var(--sg-muted);
        }
        .sg-lesson-rolodex-scroll {
            display: flex;
            gap: 0.56rem;
            overflow-x: auto;
            overflow-y: hidden;
            padding: 0.22rem 0.08rem 0.58rem 0.04rem;
            scrollbar-width: thin;
        }
        .sg-lesson-rolodex-scroll::-webkit-scrollbar {
            height: 8px;
        }
        .sg-lesson-rolodex-scroll::-webkit-scrollbar-thumb {
            background: #b8bac1;
            border-radius: 999px;
        }
        .sg-lesson-card {
            min-width: 156px;
            max-width: 186px;
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface);
            color: var(--sg-text) !important;
            text-decoration: none !important;
            padding: 0.66rem 0.68rem;
            box-shadow: var(--sg-shadow-sm);
            flex: 0 0 auto;
            transition: border-color 120ms ease, transform 120ms ease, box-shadow 120ms ease;
        }
        .sg-lesson-card:hover {
            border-color: var(--sg-border-strong);
            transform: translateY(-1px);
            box-shadow: var(--sg-shadow-md);
        }
        .sg-lesson-card.selected {
            background: var(--sg-primary-soft);
            border-color: #c1cdc5;
            color: var(--sg-text) !important;
        }
        .sg-lesson-card-title {
            font-size: 0.81rem;
            font-weight: 630;
            line-height: 1.2;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .sg-lesson-card-status {
            font-size: 0.69rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-top: 0.32rem;
            opacity: 0.8;
        }
        .sg-lesson-status-line {
            margin: 0.1rem 0 0.6rem 0;
        }
        .sg-lesson-title {
            font-size: 1.34rem;
            font-weight: 620;
            line-height: 1.2;
            color: var(--sg-text);
            margin: 0.02rem 0 0.1rem 0;
            font-family: "Manrope", "Avenir Next", "Segoe UI", sans-serif;
        }
        .sg-lesson-section {
            border: 1px solid var(--sg-border);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface-muted);
            padding: 0.72rem 0.82rem;
            margin: 0.44rem 0;
        }
        .sg-lesson-section-title {
            font-size: 0.69rem;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
            margin: 0 0 0.26rem 0;
        }
        .sg-lesson-subsection-title {
            font-size: 0.7rem;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            color: var(--sg-muted-soft);
            font-weight: 700;
            margin: 0.62rem 0 0.24rem 0;
        }
        .sg-lesson-summary {
            color: var(--sg-text);
            font-size: 0.95rem;
            line-height: 1.45;
            margin-bottom: 0.1rem;
        }
        .sg-lesson-big-idea {
            color: var(--sg-text);
            font-size: 0.95rem;
            line-height: 1.45;
            margin-bottom: 0.02rem;
        }
        .sg-anchor-ref {
            font-size: 0.94rem;
            color: var(--sg-text);
            margin-bottom: 0.05rem;
        }
        .sg-tight-link {
            font-size: 0.78rem;
            margin: 0.05rem 0 0.05rem 0;
        }
        .sg-tight-link a {
            color: var(--sg-primary-strong);
            text-decoration: underline;
        }
        .sg-outline {
            line-height: 1.42;
            color: var(--sg-text);
            font-size: 0.9rem;
        }
        .sg-outline ul {
            margin: 0.05rem 0 0.26rem 0.95rem;
            padding-left: 0.52rem;
        }
        .sg-outline li {
            margin-bottom: 0.16rem;
        }
        .sg-outline-level {
            font-weight: 620;
            color: var(--sg-text);
        }
        .sg-sidebar-brand {
            margin: 0.15rem 0 0.75rem 0;
            padding: 0.65rem 0.72rem;
            border: 1px solid var(--sg-border-strong);
            border-radius: var(--sg-radius-md);
            background: var(--sg-surface);
        }
        .sg-sidebar-title {
            margin: 0;
            color: var(--sg-text);
            font-size: 0.98rem;
            font-weight: 640;
        }
        .sg-sidebar-subtitle {
            margin: 0.35rem 0 0 0;
            color: var(--sg-muted);
            font-size: 0.78rem;
            line-height: 1.38;
        }
        .sg-sidebar-section {
            margin: 0.95rem 0 0.35rem 0;
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--sg-muted-soft);
            font-weight: 700;
        }
        div[data-testid="stRadio"] div[role="radiogroup"] {
            gap: 0.5rem 0.85rem;
            flex-wrap: wrap;
        }
        @media (max-width: 960px) {
            .main .block-container {
                max-width: 100%;
                padding-top: 0.55rem;
                padding-left: 0.7rem;
                padding-right: 0.7rem;
                padding-bottom: 1.3rem;
            }
            h1 { font-size: 1.52rem; }
            h2 { font-size: 1.16rem; }
            h3 { font-size: 1.0rem; }
            .stMarkdown p { margin-bottom: 0.44rem; }
            div[data-testid="stVerticalBlockBorderWrapper"] {
                border-radius: var(--sg-radius-md);
            }
            div[data-testid="stHorizontalBlock"] {
                gap: 0.6rem !important;
                flex-wrap: wrap;
            }
            div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
                flex: 1 1 260px !important;
                width: 100% !important;
                min-width: 0 !important;
            }
            .stButton > button,
            .stDownloadButton > button,
            .stFormSubmitButton > button {
                min-height: 2.8rem;
                font-size: 0.92rem;
                padding: 0.46rem 0.72rem;
            }
            div[data-baseweb="select"] > div,
            div[data-baseweb="input"] > div,
            div[data-baseweb="textarea"] > div {
                min-height: 2.9rem;
            }
            div[data-baseweb="tab-list"] {
                flex-wrap: wrap;
            }
            .stCaption {
                font-size: 0.75rem;
            }
            .sg-lesson-rolodex-scroll {
                gap: 0.42rem;
                padding: 0.08rem 0.05rem 0.38rem 0.02rem;
            }
            .sg-lesson-card {
                min-width: 132px;
                max-width: 148px;
                padding: 0.4rem 0.42rem;
            }
            .sg-lesson-card-title {
                font-size: 0.74rem;
            }
            .sg-lesson-card-status {
                font-size: 0.62rem;
                margin-top: 0.2rem;
            }
            .sg-save-required {
                font-size: 0.77rem;
            }
            .sg-lesson-title {
                font-size: 1.08rem;
            }
            .sg-lesson-section {
                padding: 0.52rem 0.58rem;
                margin: 0.3rem 0;
            }
            .sg-lesson-section-title,
            .sg-lesson-subsection-title {
                font-size: 0.67rem;
            }
            .sg-lesson-summary,
            .sg-lesson-big-idea {
                font-size: 0.9rem;
            }
            .sg-tight-link {
                font-size: 0.74rem;
            }
            .sg-outline {
                font-size: 0.86rem;
            }
            .sg-page-header {
                padding: 0.82rem 0.86rem;
            }
            .sg-page-title {
                font-size: 1.28rem;
            }
            .sg-sunday-list {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
            .sg-selection-meta-grid {
                grid-template-columns: 1fr;
            }
            .sg-service-grid {
                grid-template-columns: 1fr;
            }
            .sg-gathering-preview {
                padding: 0.78rem 0.8rem;
            }
            .sg-gathering-preview-lines {
                gap: 0.34rem;
            }
            .sg-gathering-preview-line-label {
                min-width: 74px;
            }
            .sg-schedule-entry {
                padding: 0.72rem 0.76rem;
            }
            .sg-schedule-entry-head {
                flex-direction: column;
                gap: 0.4rem;
            }
            .sg-calendar-month {
                min-width: 100%;
                padding: 0.7rem;
            }
            .sg-calendar-day {
                min-height: 96px;
                padding: 0.32rem 0.36rem;
            }
            .sg-calendar-shell {
                overflow-x: visible;
            }
            .sg-calendar-month-header {
                flex-direction: column;
                align-items: flex-start;
                gap: 0.18rem;
            }
            .sg-calendar-weekdays {
                display: none;
            }
            .sg-calendar-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 0.5rem;
            }
            .sg-calendar-day-outside {
                display: none;
            }
            div[data-testid="stDataFrame"],
            div[data-testid="stDataEditor"] {
                overflow-x: auto;
            }
            section[data-testid="stSidebar"] .stButton > button {
                min-height: 2.12rem;
            }
        }
        @media (max-width: 640px) {
            .main .block-container {
                padding-left: 0.5rem;
                padding-right: 0.5rem;
                padding-bottom: 1.1rem;
            }
            h1 { font-size: 1.26rem; }
            h2 { font-size: 1.12rem; }
            h3 { font-size: 0.95rem; }
            div[data-testid="stMetricLabel"] {
                font-size: 0.66rem;
            }
            div[data-testid="stMetricValue"] {
                font-size: 1.22rem;
            }
            div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
                flex: 1 1 100% !important;
            }
            .sg-lesson-card {
                min-width: 118px;
                max-width: 132px;
            }
            .sg-page-title {
                font-size: 1.16rem;
            }
            .sg-page-subtitle {
                font-size: 0.85rem;
            }
            .sg-meeting-summary {
                padding: 0.78rem 0.8rem;
            }
            .sg-meeting-summary-title {
                font-size: 1.02rem;
            }
            .sg-service-card {
                padding: 0.72rem 0.76rem;
            }
            .sg-gathering-preview-date {
                font-size: 0.94rem;
            }
            .sg-gathering-preview-lesson {
                font-size: 0.8rem;
            }
            .sg-current-selection-banner {
                padding: 0.54rem 0.62rem;
            }
            .sg-current-selection-date {
                font-size: 0.88rem;
            }
            .sg-gathering-preview-line {
                gap: 0.22rem 0.34rem;
            }
            .sg-gathering-preview-line-label {
                min-width: 68px;
                font-size: 0.65rem;
            }
            .sg-gathering-preview-line-value {
                font-size: 0.83rem;
            }
            .sg-schedule-entry {
                padding: 0.68rem 0.72rem;
            }
            .sg-schedule-date {
                font-size: 0.94rem;
            }
            .sg-schedule-activity-value {
                font-size: 0.84rem;
            }
            .sg-schedule-activity-note,
            .sg-schedule-footer {
                font-size: 0.74rem;
            }
            .sg-calendar-month {
                padding: 0.65rem;
            }
            .sg-calendar-grid {
                grid-template-columns: 1fr;
            }
            .sg-calendar-day {
                min-height: auto;
                padding: 0.48rem 0.52rem;
            }
            .sg-calendar-day-number {
                font-size: 0.82rem;
            }
            .sg-calendar-day-detail {
                font-size: 0.66rem;
                line-height: 1.22;
            }
            .sg-calendar-day-caption {
                font-size: 0.66rem;
                line-height: 1.2;
            }
            .sg-calendar-day-pill .sg-pill {
                font-size: 0.66rem;
            }
            .sg-pill {
                white-space: normal;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def safe_link_button(label: str, url: str) -> None:
    if hasattr(st, "link_button"):
        st.link_button(label, url)
    else:
        st.markdown(f"[{label}]({url})")


def parse_question_text(raw_text: str) -> List[str]:
    return [line.strip() for line in raw_text.splitlines() if line.strip()]


def normalize_big_idea_text(raw_big_idea: str, theme: str = "") -> str:
    text = str(raw_big_idea or "").strip()
    theme_value = str(theme or "").strip()

    if not text and theme_value:
        text = theme_value
    if not text:
        return "Because God is at work, let's respond in faith this week."

    if ":" in text:
        prefix, suffix = text.split(":", 1)
        suffix_clean = suffix.strip()
        suffix_lower = suffix_clean.lower()
        if suffix_clean and (
            suffix_lower.startswith("because ")
            or suffix_lower.startswith("since ")
            or suffix_lower.startswith("as ")
            or (theme_value and prefix.strip().lower() == theme_value.lower())
        ):
            text = suffix_clean

    text = text.lstrip(" -:-\u2013\u2014").strip()
    lower = text.lower()
    if lower.startswith("because "):
        text = "Because " + text[8:].lstrip()
    elif lower.startswith("since "):
        text = "Because " + text[6:].lstrip()
    elif lower.startswith("as "):
        text = "Because " + text[3:].lstrip()
    elif lower.startswith("because"):
        text = "Because " + text[7:].lstrip(" :-")
    else:
        text = f"Because {text}"
    return text


def render_templates(templates: List[str], lesson: dict) -> List[str]:
    safe = {
        "theme": str(lesson.get("theme", "")).strip(),
        "big_idea": normalize_big_idea_text(
            str(lesson.get("big_idea", "")).strip(),
            str(lesson.get("theme", "")).strip(),
        ),
        "one_sentence_summary": str(lesson.get("one_sentence_summary", "")).strip(),
        "anchor_verse": str(lesson.get("anchor_verse", "")).strip(),
    }
    out: List[str] = []
    for template in templates:
        try:
            out.append(template.format(**safe))
        except Exception:
            out.append(template)
    return out


def build_default_questions_for_lesson(lesson: dict) -> Dict[str, List[str]]:
    lesson_payload = dict(lesson)
    theme_value = str(lesson_payload.get("theme", "")).strip()
    lesson_payload["big_idea"] = normalize_big_idea_text(
        str(lesson_payload.get("big_idea", "")).strip(),
        theme_value,
    )
    return {
        level: render_templates(DEFAULT_QUESTION_TEMPLATES[level], lesson_payload)
        for level in QUESTION_LEVELS
    }


def get_query_param_int(name: str) -> int | None:
    try:
        if hasattr(st, "query_params"):
            raw_value = st.query_params.get(name)
            if raw_value is None:
                return None
            if isinstance(raw_value, list):
                raw_value = raw_value[0] if raw_value else None
            if raw_value is None:
                return None
            return int(str(raw_value))
        params = st.experimental_get_query_params()
        values = params.get(name, [])
        if not values:
            return None
        return int(values[0])
    except Exception:
        return None


def clear_query_param(name: str) -> None:
    try:
        if hasattr(st, "query_params"):
            if name in st.query_params:
                del st.query_params[name]
            return
        params = st.experimental_get_query_params()
        if name in params:
            params.pop(name, None)
            st.experimental_set_query_params(**params)
    except Exception:
        return


def fetch_small_group_families() -> List[str]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT family_name
            FROM small_group_families
            ORDER BY LOWER(family_name) ASC
            """
        ).fetchall()

    families = [str(row["family_name"]).strip() for row in rows if str(row["family_name"]).strip()]
    return families


def get_person_options() -> List[str]:
    families = fetch_small_group_families()
    if not families:
        families = list(DEFAULT_FAMILY_OPTIONS)
    deduped_families = [name for name in families if name and name != TBD_OPTION]
    return [TBD_OPTION] + deduped_families + ["Other"]


def save_small_group_families(family_names: List[str]) -> int:
    cleaned_names: List[str] = []
    for item in family_names:
        cleaned = str(item).strip()
        if cleaned and cleaned not in cleaned_names:
            cleaned_names.append(cleaned)

    if not cleaned_names:
        return 0

    with get_connection() as conn:
        conn.execute("DELETE FROM small_group_families")
        for position, family_name in enumerate(cleaned_names, start=1):
            conn.execute(
                """
                INSERT INTO small_group_families (family_name, position)
                VALUES (?, ?)
                """,
                (family_name, position),
            )

    return len(cleaned_names)


def resolve_person(selection: str, other_value: str) -> str:
    selection = (selection or "").strip()
    if selection == "Other":
        return (other_value or "").strip()
    return selection


def split_person_for_select(saved_name: str, options: List[str]) -> Tuple[str, str]:
    normalized = (saved_name or "").strip()
    known_people = [name for name in options if name != "Other"]
    if not known_people:
        known_people = list(DEFAULT_FAMILY_OPTIONS)
    if normalized in known_people:
        return normalized, ""
    if normalized:
        return "Other", normalized
    return known_people[0], ""


def format_meeting_date(iso_date: str) -> str:
    try:
        parsed = date.fromisoformat(str(iso_date))
    except (TypeError, ValueError):
        return str(iso_date)
    return parsed.strftime("%m/%d/%y")


def add_meeting_log(
    meeting_date: date,
    lesson_week: int,
    status: str,
    notes: str,
    host_name: str = "",
    facilitator_name: str = "",
) -> None:
    if status not in MEETING_STATUS_OPTIONS:
        raise ValueError(f"Invalid status: {status}")

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO meeting_log (
                meeting_date, lesson_week, status, notes, host_name, facilitator_name
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_date.isoformat(),
                int(lesson_week),
                status,
                notes.strip(),
                host_name.strip(),
                facilitator_name.strip(),
            ),
        )


def update_meeting_record(
    record_id: int,
    status: str,
    notes: str,
    host_name: str,
    facilitator_name: str,
) -> None:
    if status not in MEETING_STATUS_OPTIONS:
        raise ValueError(f"Invalid status: {status}")

    with get_connection() as conn:
        conn.execute(
            """
            UPDATE meeting_log
            SET status = ?, notes = ?, host_name = ?, facilitator_name = ?
            WHERE id = ?
            """,
            (
                status,
                notes.strip(),
                host_name.strip(),
                facilitator_name.strip(),
                int(record_id),
            ),
        )


def delete_meeting_record(record_id: int) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM meeting_log WHERE id = ?", (int(record_id),))


def fetch_meeting_log(lessons_df: pd.DataFrame) -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql_query(
            """
            SELECT id, meeting_date, lesson_week, status, notes, host_name, facilitator_name
            FROM meeting_log
            ORDER BY meeting_date DESC, id DESC
            """,
            conn,
        )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "meeting_date",
                "lesson_week",
                "lesson_theme",
                "status",
                "host_name",
                "facilitator_name",
                "notes",
            ]
        )

    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    df["meeting_date"] = df["meeting_date"].apply(format_meeting_date)
    df["lesson_theme"] = df["lesson_week"].map(theme_lookup).fillna("(Unknown lesson)")
    df["notes"] = df["notes"].fillna("")
    df["host_name"] = df["host_name"].fillna("")
    df["facilitator_name"] = df["facilitator_name"].fillna("")

    return df[
        [
            "id",
            "meeting_date",
            "lesson_week",
            "lesson_theme",
            "status",
            "host_name",
            "facilitator_name",
            "notes",
        ]
    ]


def get_done_weeks() -> set[int]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT lesson_week FROM meeting_log WHERE status = 'Completed'"
        ).fetchall()
    return {int(row["lesson_week"]) for row in rows}


def get_latest_status_by_week() -> Dict[int, str]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT lesson_week, status
            FROM meeting_log
            ORDER BY meeting_date ASC, id ASC
            """
        ).fetchall()

    latest: Dict[int, str] = {}
    for row in rows:
        latest[int(row["lesson_week"])] = row["status"]
    return latest


def derive_status_map(lessons_df: pd.DataFrame) -> Dict[int, str]:
    done_weeks = get_done_weeks()
    latest_status = get_latest_status_by_week()

    status_map: Dict[int, str] = {}
    for week in lessons_df["week"].astype(int).tolist():
        if week in done_weeks:
            status_map[week] = "Done"
        else:
            fallback = latest_status.get(week, "Not done")
            if fallback in ("Skipped", "Postponed"):
                status_map[week] = fallback
            else:
                status_map[week] = "Not done"

    return status_map


def get_lesson_notes(lesson_week: int) -> str:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT notes FROM lesson_notes WHERE lesson_week = ?", (int(lesson_week),)
        ).fetchone()

    return row["notes"] if row else ""


def save_lesson_notes(lesson_week: int, notes: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO lesson_notes (lesson_week, notes)
            VALUES (?, ?)
            ON CONFLICT(lesson_week) DO UPDATE SET notes = excluded.notes
            """,
            (int(lesson_week), notes.strip()),
        )


def get_lesson_verse_text(lesson_week: int) -> str:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT verse_text FROM lesson_verse_text WHERE lesson_week = ?",
            (int(lesson_week),),
        ).fetchone()

    return row["verse_text"] if row else ""


def save_lesson_verse_text(lesson_week: int, verse_text: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO lesson_verse_text (lesson_week, verse_text)
            VALUES (?, ?)
            ON CONFLICT(lesson_week) DO UPDATE SET verse_text = excluded.verse_text
            """,
            (int(lesson_week), verse_text.strip()),
        )


def add_upcoming_meeting(
    meeting_date: date,
    lesson_week: int,
    host_name: str,
    facilitator_name: str,
    notes: str,
    main_meal: str = "",
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO upcoming_meetings (
                meeting_date, lesson_week, host_name, facilitator_name, notes, main_meal
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                meeting_date.isoformat(),
                int(lesson_week),
                host_name.strip(),
                facilitator_name.strip(),
                notes.strip(),
                str(main_meal).strip(),
            ),
        )


def update_upcoming_meeting(
    record_id: int,
    lesson_week: int,
    host_name: str,
    facilitator_name: str,
    notes: str,
    main_meal: str | None = None,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE upcoming_meetings
            SET lesson_week = ?, host_name = ?, facilitator_name = ?, notes = ?,
                main_meal = COALESCE(?, main_meal)
            WHERE id = ?
            """,
            (
                int(lesson_week),
                host_name.strip(),
                facilitator_name.strip(),
                notes.strip(),
                None if main_meal is None else str(main_meal).strip(),
                int(record_id),
            ),
        )


def delete_upcoming_meeting(record_id: int) -> None:
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM upcoming_meal_signups WHERE upcoming_meeting_id = ?",
            (int(record_id),),
        )
        conn.execute("DELETE FROM upcoming_meetings WHERE id = ?", (int(record_id),))


def fetch_upcoming_meetings(lessons_df: pd.DataFrame) -> pd.DataFrame:
    with get_connection() as conn:
        upcoming_df = pd.read_sql_query(
            """
            SELECT
                u.id,
                u.meeting_date,
                u.lesson_week,
                u.host_name,
                u.facilitator_name,
                u.notes,
                u.main_meal,
                COALESCE(m.signup_count, 0) AS meal_signup_count
            FROM upcoming_meetings u
            LEFT JOIN (
                SELECT upcoming_meeting_id, COUNT(*) AS signup_count
                FROM upcoming_meal_signups
                GROUP BY upcoming_meeting_id
            ) m ON m.upcoming_meeting_id = u.id
            WHERE meeting_date >= ?
            ORDER BY meeting_date ASC, id ASC
            """,
            conn,
            params=[date.today().isoformat()],
        )

    if upcoming_df.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "meeting_date",
                "lesson_week",
                "lesson_theme",
                "host_name",
                "facilitator_name",
                "notes",
                "main_meal",
                "meal_signup_count",
            ]
        )

    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    upcoming_df["lesson_theme"] = (
        upcoming_df["lesson_week"].map(theme_lookup).fillna("(Unknown lesson)")
    )

    for col in ["host_name", "facilitator_name", "notes", "main_meal"]:
        upcoming_df[col] = upcoming_df[col].fillna("")
    upcoming_df["meal_signup_count"] = upcoming_df["meal_signup_count"].fillna(0).astype(int)

    return upcoming_df[
        [
            "id",
            "meeting_date",
            "lesson_week",
            "lesson_theme",
            "host_name",
            "facilitator_name",
            "notes",
            "main_meal",
            "meal_signup_count",
        ]
    ]


def fetch_upcoming_meal_signups(upcoming_meeting_id: int) -> pd.DataFrame:
    with get_connection() as conn:
        meal_df = pd.read_sql_query(
            """
            SELECT attendee_name AS Name, dish AS Dish
            FROM upcoming_meal_signups
            WHERE upcoming_meeting_id = ?
            ORDER BY position ASC, id ASC
            """,
            conn,
            params=[int(upcoming_meeting_id)],
        )

    if meal_df.empty:
        return pd.DataFrame(columns=["Name", "Dish"])

    meal_df["Name"] = meal_df["Name"].fillna("")
    meal_df["Dish"] = meal_df["Dish"].fillna("")
    return meal_df[["Name", "Dish"]]


def normalize_meal_rows(rows: List[dict]) -> List[Tuple[str, str]]:
    normalized: List[Tuple[str, str]] = []
    for row in rows:
        attendee = row.get("Name", row.get("attendee_name", ""))
        dish = row.get("Dish", row.get("dish", ""))
        attendee = "" if pd.isna(attendee) else str(attendee).strip()
        dish = "" if pd.isna(dish) else str(dish).strip()
        if not attendee and not dish:
            continue
        normalized.append((attendee, dish))
    return normalized


def is_open_assignment(value: str) -> bool:
    normalized = str(value or "").strip()
    return not normalized or normalized == TBD_OPTION


def summarize_upcoming_meeting(row: dict) -> Dict[str, object]:
    host_name = str(row.get("host_name", "")).strip()
    facilitator_name = str(row.get("facilitator_name", "")).strip()
    lesson_title = str(row.get("lesson_theme", "")).strip() or "(Lesson not assigned)"
    main_meal = str(row.get("main_meal", "")).strip()
    meal_signup_count = int(row.get("meal_signup_count", 0) or 0)

    host_open = is_open_assignment(host_name)
    facilitator_open = is_open_assignment(facilitator_name)

    open_needs: List[str] = []
    if host_open:
        open_needs.append("Host")
    if facilitator_open:
        open_needs.append("Facilitator")
    if not main_meal:
        open_needs.append("Main meal")
    elif meal_signup_count == 0:
        open_needs.append("Meal support")

    if open_needs:
        overall_label = (
            "Open opportunity"
            if len(open_needs) == 1
            else f"Open needs: {', '.join(open_needs[:2])}"
        )
        overall_tone = "open"
    else:
        overall_label = "Ready for the week"
        overall_tone = "ready"

    if host_open:
        host_display = "Open opportunity"
        host_note = "Still needs a host"
        host_tone = "open"
    else:
        host_display = host_name
        host_note = "Hosting is covered"
        host_tone = "filled"

    if facilitator_open:
        facilitator_display = "Open opportunity"
        facilitator_note = "Still needs a facilitator"
        facilitator_tone = "open"
    else:
        facilitator_display = facilitator_name
        facilitator_note = "Facilitator is covered"
        facilitator_tone = "filled"

    if main_meal:
        meal_display = main_meal
        if meal_signup_count > 0:
            meal_note = f"{meal_signup_count} food sign-up{'s' if meal_signup_count != 1 else ''}"
            meal_tone = "filled"
        else:
            meal_note = "Sides, desserts, and drinks are still open"
            meal_tone = "open"
    else:
        meal_display = "Meal plan still open"
        meal_note = "Host can set the main meal and others can support it"
        meal_tone = "open"

    return {
        "host_display": host_display,
        "host_note": host_note,
        "host_tone": host_tone,
        "facilitator_display": facilitator_display,
        "facilitator_note": facilitator_note,
        "facilitator_tone": facilitator_tone,
        "meal_display": meal_display,
        "meal_note": meal_note,
        "meal_tone": meal_tone,
        "lesson_title": lesson_title,
        "main_meal": main_meal,
        "meal_signup_count": meal_signup_count,
        "overall_label": overall_label,
        "overall_tone": overall_tone,
        "open_needs": open_needs,
    }


def render_support_pill(label: str, tone: str = "neutral") -> str:
    return f"<span class='sg-pill sg-pill-{escape(tone)}'>{escape(label)}</span>"


def render_selected_meeting_summary(row: dict) -> None:
    summary = summarize_upcoming_meeting(row)
    date_label = format_meeting_date(str(row.get("meeting_date", "")))
    lesson_week = int(row.get("lesson_week", 0) or 0)
    lesson_label = f"Lesson {lesson_week}" if lesson_week > 0 else "Lesson"
    lesson_title = str(summary["lesson_title"]).strip()

    status_pills = [render_support_pill(str(summary["overall_label"]), str(summary["overall_tone"]))]
    if summary["overall_tone"] == "ready":
        status_pills.append(render_support_pill("Thanks for serving", "soft"))

    st.markdown(
        (
            "<div class='sg-meeting-summary'>"
            "<div class='sg-meeting-summary-head'>"
            "<p class='sg-meeting-summary-kicker'>Selected gathering</p>"
            f"<p class='sg-meeting-summary-title'>{escape(date_label)}</p>"
            f"<p class='sg-meeting-summary-subtitle'>{escape(lesson_label)}"
            f"{escape(' - ' + lesson_title) if lesson_title else ''}</p>"
            f"<div class='sg-pill-row'>{''.join(status_pills)}</div>"
            "</div>"
            "<div class='sg-service-grid'>"
            "<div class='sg-service-card'>"
            "<p class='sg-service-kicker'>Hosting</p>"
            f"<p class='sg-service-title'>{escape(str(summary['host_display']))}</p>"
            f"<p class='sg-service-detail'>{escape(str(summary['host_note']))}</p>"
            f"{render_support_pill('Host', str(summary['host_tone']))}"
            "</div>"
            "<div class='sg-service-card'>"
            "<p class='sg-service-kicker'>Facilitating</p>"
            f"<p class='sg-service-title'>{escape(str(summary['facilitator_display']))}</p>"
            f"<p class='sg-service-detail'>{escape(str(summary['facilitator_note']))}</p>"
            f"{render_support_pill('Facilitator', str(summary['facilitator_tone']))}"
            "</div>"
            "<div class='sg-service-card'>"
            "<p class='sg-service-kicker'>Meal Support</p>"
            f"<p class='sg-service-title'>{escape(str(summary['meal_display']))}</p>"
            f"<p class='sg-service-detail'>{escape(str(summary['meal_note']))}</p>"
            f"{render_support_pill('Meal', str(summary['meal_tone']))}"
            "</div>"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def save_upcoming_meal_signups(upcoming_meeting_id: int, rows: List[dict]) -> int:
    saved_count = 0
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM upcoming_meal_signups WHERE upcoming_meeting_id = ?",
            (int(upcoming_meeting_id),),
        )
        for position, (attendee, dish) in enumerate(normalize_meal_rows(rows), start=1):
            conn.execute(
                """
                INSERT INTO upcoming_meal_signups (
                    upcoming_meeting_id, attendee_name, dish, position
                )
                VALUES (?, ?, ?, ?)
                """,
                (int(upcoming_meeting_id), attendee, dish, position),
            )
            saved_count += 1

    return saved_count


def render_upcoming_calendar(
    upcoming_df: pd.DataFrame,
    selected_date: str = "",
) -> None:
    if upcoming_df.empty:
        st.info("No upcoming dates yet.")
        return

    parsed_series = pd.to_datetime(upcoming_df["meeting_date"], errors="coerce").dropna()
    parsed_dates = parsed_series.dt.date.tolist()
    if not parsed_dates:
        st.info("No upcoming dates yet.")
        return

    selected_date_obj: date | None = None
    try:
        selected_date_obj = date.fromisoformat(str(selected_date))
    except Exception:
        selected_date_obj = None

    grouped_rows: Dict[str, List[dict]] = {}
    for row in upcoming_df.to_dict(orient="records"):
        raw_date = str(row.get("meeting_date", "")).strip()
        try:
            meeting_day = date.fromisoformat(raw_date)
        except Exception:
            continue
        month_label = meeting_day.strftime("%B %Y")
        grouped_rows.setdefault(month_label, []).append(row)

    if not grouped_rows:
        st.info("No upcoming dates yet.")
        return

    if selected_date_obj is not None:
        st.markdown(
            (
                "<div class='sg-current-selection-banner'>"
                "<span class='sg-current-selection-label'>Currently viewing</span>"
                f"<span class='sg-current-selection-date'>{escape(selected_date_obj.strftime('%A, %B %d, %Y'))}</span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    st.caption("Selected date stays highlighted below. Click a date for details or a lesson to open that lesson.")

    for month_label, rows in grouped_rows.items():
        st.markdown(
            f"<p class='sg-schedule-month-title'>{escape(month_label)}</p>",
            unsafe_allow_html=True,
        )

        for row in rows:
            raw_date = str(row.get("meeting_date", "")).strip()
            try:
                meeting_day = date.fromisoformat(raw_date)
            except Exception:
                continue

            summary = summarize_upcoming_meeting(row)
            is_selected = selected_date_obj is not None and meeting_day == selected_date_obj
            host_value = str(summary.get("host_display", "")).strip() or "Open"
            facilitator_value = str(summary.get("facilitator_display", "")).strip() or "Open"
            lesson_value = str(summary.get("lesson_title", "")).strip() or "Lesson not assigned"
            overall_label = str(summary.get("overall_label", "Open opportunity")).strip()
            overall_tone = str(summary.get("overall_tone", "open")).strip()
            row_id = int(row["id"])
            lesson_week = int(row.get("lesson_week", 0) or 0)
            main_meal = str(summary.get("main_meal", "")).strip()
            meal_signup_count = int(summary.get("meal_signup_count", 0) or 0)
            lesson_line = f"Lesson {lesson_week}"
            if lesson_value:
                lesson_line = f"{lesson_line} - {lesson_value}"
            lesson_html = escape(lesson_line)
            if lesson_week > 0:
                lesson_html = (
                    f"<a href='?lesson_pick={lesson_week}' target='_self'>{lesson_html}</a>"
                )
            preview_class = "sg-gathering-preview sg-gathering-preview-selected" if is_selected else "sg-gathering-preview"
            preview_kicker = "Currently viewing" if is_selected else "Upcoming gathering"
            selected_pill = render_support_pill("Viewing now", "ready") if is_selected else ""
            host_line_value = "TBD" if str(summary.get("host_tone", "")) == "open" else host_value
            facilitator_line_value = (
                "TBD" if str(summary.get("facilitator_tone", "")) == "open" else facilitator_value
            )
            if main_meal:
                meal_line_value = main_meal
                if meal_signup_count > 0:
                    meal_line_value = (
                        f"{meal_line_value} ({meal_signup_count} support item"
                        f"{'s' if meal_signup_count != 1 else ''})"
                    )
                else:
                    meal_line_value = f"{meal_line_value} (support open)"
                meal_line_is_open = False
            else:
                meal_line_value = "TBD"
                meal_line_is_open = True

            host_value_class = "sg-gathering-preview-line-value"
            if host_line_value == "TBD":
                host_value_class += " sg-gathering-preview-line-value-open"

            facilitator_value_class = "sg-gathering-preview-line-value"
            if facilitator_line_value == "TBD":
                facilitator_value_class += " sg-gathering-preview-line-value-open"

            meal_value_class = "sg-gathering-preview-line-value"
            if meal_line_is_open:
                meal_value_class += " sg-gathering-preview-line-value-open"

            st.markdown(
                (
                    f"<div class='{preview_class}'>"
                    "<div class='sg-gathering-preview-head'>"
                    f"<p class='sg-gathering-preview-kicker'>{preview_kicker}</p>"
                    f"<p class='sg-gathering-preview-date'><a href='?dashboard_pick={row_id}#meeting-details' target='_self'>"
                    f"{escape(meeting_day.strftime('%m/%d/%Y (%a.)'))}</a></p>"
                    f"<p class='sg-gathering-preview-lesson'>{lesson_html}</p>"
                    f"<div class='sg-pill-row'>{render_support_pill(overall_label, overall_tone)}{selected_pill}</div>"
                    "</div>"
                    "<div class='sg-gathering-preview-lines'>"
                    "<p class='sg-gathering-preview-line'>"
                    "<span class='sg-gathering-preview-line-label'>Hosting</span>"
                    f"<span class='{host_value_class}'>{escape(host_line_value)}</span>"
                    "</p>"
                    "<p class='sg-gathering-preview-line'>"
                    "<span class='sg-gathering-preview-line-label'>Facilitating</span>"
                    f"<span class='{facilitator_value_class}'>{escape(facilitator_line_value)}</span>"
                    "</p>"
                    "<p class='sg-gathering-preview-line'>"
                    "<span class='sg-gathering-preview-line-label'>Meal</span>"
                    f"<span class='{meal_value_class}'>{escape(meal_line_value)}</span>"
                    "</p>"
                    "</div>"
                    "<p class='sg-schedule-footer'>Click the date for gathering details or the lesson to open that lesson</p>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

def get_custom_questions(lesson_week: int) -> Dict[str, List[str]]:
    result = {level: [] for level in QUESTION_LEVELS}

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT level, question
            FROM lesson_custom_questions
            WHERE lesson_week = ?
            ORDER BY level, position
            """,
            (int(lesson_week),),
        ).fetchall()

    for row in rows:
        result[row["level"]].append(row["question"])

    return result


def get_effective_questions(
    lesson_week: int,
    lesson_context: dict | None = None,
) -> Tuple[Dict[str, List[str]], bool]:
    custom_questions = get_custom_questions(lesson_week)
    has_custom = any(custom_questions[level] for level in QUESTION_LEVELS)
    default_questions = build_default_questions_for_lesson(lesson_context or {})

    effective = {}
    for level in QUESTION_LEVELS:
        effective[level] = custom_questions[level] if custom_questions[level] else default_questions[level]

    return effective, has_custom


def save_custom_questions(lesson_week: int, questions_by_level: Dict[str, List[str]]) -> None:
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM lesson_custom_questions WHERE lesson_week = ?",
            (int(lesson_week),),
        )

        for level in QUESTION_LEVELS:
            for position, question in enumerate(questions_by_level.get(level, []), start=1):
                cleaned = question.strip()
                if not cleaned:
                    continue
                conn.execute(
                    """
                    INSERT INTO lesson_custom_questions (lesson_week, level, position, question)
                    VALUES (?, ?, ?, ?)
                    """,
                    (int(lesson_week), level, position, cleaned),
                )


def clear_custom_questions(lesson_week: int) -> None:
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM lesson_custom_questions WHERE lesson_week = ?",
            (int(lesson_week),),
        )


def export_backup_data() -> dict:
    with get_connection() as conn:
        meeting_log = pd.read_sql_query(
            """
            SELECT meeting_date, lesson_week, status, notes, host_name, facilitator_name
            FROM meeting_log
            ORDER BY id
            """,
            conn,
        ).to_dict(orient="records")
        lesson_notes = pd.read_sql_query(
            "SELECT lesson_week, notes FROM lesson_notes ORDER BY lesson_week",
            conn,
        ).to_dict(orient="records")
        lesson_verse_text = pd.read_sql_query(
            "SELECT lesson_week, verse_text FROM lesson_verse_text ORDER BY lesson_week",
            conn,
        ).to_dict(orient="records")
        custom_questions = pd.read_sql_query(
            """
            SELECT lesson_week, level, position, question
            FROM lesson_custom_questions
            ORDER BY lesson_week, level, position
            """,
            conn,
        ).to_dict(orient="records")
        upcoming_meetings = pd.read_sql_query(
            """
            SELECT id, meeting_date, lesson_week, host_name, facilitator_name, notes, main_meal
            FROM upcoming_meetings
            ORDER BY id
            """,
            conn,
        ).to_dict(orient="records")
        upcoming_meal_signups = pd.read_sql_query(
            """
            SELECT upcoming_meeting_id, attendee_name, dish, position
            FROM upcoming_meal_signups
            ORDER BY upcoming_meeting_id, position, id
            """,
            conn,
        ).to_dict(orient="records")
        small_group_families = pd.read_sql_query(
            """
            SELECT family_name, position
            FROM small_group_families
            ORDER BY position, family_name
            """,
            conn,
        ).to_dict(orient="records")

    return {
        "exported_at": datetime.utcnow().isoformat() + "Z",
        "meeting_log": meeting_log,
        "lesson_notes": lesson_notes,
        "lesson_verse_text": lesson_verse_text,
        "lesson_custom_questions": custom_questions,
        "upcoming_meetings": upcoming_meetings,
        "upcoming_meal_signups": upcoming_meal_signups,
        "small_group_families": small_group_families,
    }


def import_backup_data(payload: dict) -> None:
    meeting_rows = payload.get("meeting_log", [])
    note_rows = payload.get("lesson_notes", [])
    verse_rows = payload.get("lesson_verse_text", [])
    question_rows = payload.get("lesson_custom_questions", [])
    upcoming_rows = payload.get("upcoming_meetings", [])
    meal_signup_rows = payload.get("upcoming_meal_signups", [])
    family_rows = payload.get("small_group_families", [])

    if (
        not isinstance(meeting_rows, list)
        or not isinstance(note_rows, list)
        or not isinstance(verse_rows, list)
        or not isinstance(question_rows, list)
        or not isinstance(upcoming_rows, list)
        or not isinstance(meal_signup_rows, list)
        or not isinstance(family_rows, list)
    ):
        raise ValueError("Backup format is invalid.")

    with get_connection() as conn:
        conn.execute("DELETE FROM meeting_log")
        conn.execute("DELETE FROM lesson_notes")
        conn.execute("DELETE FROM lesson_verse_text")
        conn.execute("DELETE FROM lesson_custom_questions")
        conn.execute("DELETE FROM upcoming_meal_signups")
        conn.execute("DELETE FROM upcoming_meetings")
        conn.execute("DELETE FROM small_group_families")

        for row in meeting_rows:
            try:
                lesson_week = int(row.get("lesson_week"))
                meeting_date = str(row.get("meeting_date", "")).strip()
                status = str(row.get("status", "Skipped")).strip()
                notes = str(row.get("notes", "")).strip()
                host_name = str(row.get("host_name", "")).strip()
                facilitator_name = str(row.get("facilitator_name", "")).strip()
            except Exception as exc:
                raise ValueError(f"Invalid meeting_log row: {row}") from exc

            if status not in MEETING_STATUS_OPTIONS:
                status = "Skipped"
            if not meeting_date:
                continue

            conn.execute(
                """
                INSERT INTO meeting_log (
                    meeting_date, lesson_week, status, notes, host_name, facilitator_name
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (meeting_date, lesson_week, status, notes, host_name, facilitator_name),
            )

        for row in note_rows:
            lesson_week = int(row.get("lesson_week"))
            notes = str(row.get("notes", ""))
            conn.execute(
                """
                INSERT INTO lesson_notes (lesson_week, notes)
                VALUES (?, ?)
                ON CONFLICT(lesson_week) DO UPDATE SET notes = excluded.notes
                """,
                (lesson_week, notes),
            )

        for row in verse_rows:
            lesson_week = int(row.get("lesson_week"))
            verse_text = str(row.get("verse_text", ""))
            conn.execute(
                """
                INSERT INTO lesson_verse_text (lesson_week, verse_text)
                VALUES (?, ?)
                ON CONFLICT(lesson_week) DO UPDATE SET verse_text = excluded.verse_text
                """,
                (lesson_week, verse_text),
            )

        for row in question_rows:
            lesson_week = int(row.get("lesson_week"))
            level = str(row.get("level", "")).strip()
            position = int(row.get("position", 0))
            question = str(row.get("question", "")).strip()

            if level not in QUESTION_LEVELS or position <= 0 or not question:
                continue

            conn.execute(
                """
                INSERT INTO lesson_custom_questions (lesson_week, level, position, question)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(lesson_week, level, position) DO UPDATE
                SET question = excluded.question
                """,
                (lesson_week, level, position, question),
            )

        for row in upcoming_rows:
            meeting_date = str(row.get("meeting_date", "")).strip()
            if not meeting_date:
                continue
            record_id = row.get("id")
            lesson_week = int(row.get("lesson_week"))
            host_name = str(row.get("host_name", "")).strip()
            facilitator_name = str(row.get("facilitator_name", "")).strip()
            notes = str(row.get("notes", "")).strip()
            main_meal = str(row.get("main_meal", "")).strip()
            if record_id is None:
                conn.execute(
                    """
                    INSERT INTO upcoming_meetings (
                        meeting_date, lesson_week, host_name, facilitator_name, notes, main_meal
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (meeting_date, lesson_week, host_name, facilitator_name, notes, main_meal),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO upcoming_meetings (
                        id, meeting_date, lesson_week, host_name, facilitator_name, notes, main_meal
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(record_id),
                        meeting_date,
                        lesson_week,
                        host_name,
                        facilitator_name,
                        notes,
                        main_meal,
                    ),
                )

        valid_upcoming_ids = {
            row["id"] for row in conn.execute("SELECT id FROM upcoming_meetings").fetchall()
        }

        for row in meal_signup_rows:
            upcoming_id = int(row.get("upcoming_meeting_id", 0))
            if upcoming_id not in valid_upcoming_ids:
                continue
            attendee = str(row.get("attendee_name", "")).strip()
            dish = str(row.get("dish", "")).strip()
            position = int(row.get("position", 0))
            if not attendee and not dish:
                continue
            if position <= 0:
                position = 1
            conn.execute(
                """
                INSERT INTO upcoming_meal_signups (
                    upcoming_meeting_id, attendee_name, dish, position
                )
                VALUES (?, ?, ?, ?)
                """,
                (upcoming_id, attendee, dish, position),
            )

        imported_family_names: List[str] = []
        for row in family_rows:
            if isinstance(row, dict):
                family_name = str(row.get("family_name", "")).strip()
            else:
                family_name = str(row).strip()
            if family_name and family_name not in imported_family_names:
                imported_family_names.append(family_name)

        if not imported_family_names:
            imported_family_names = list(DEFAULT_FAMILY_OPTIONS)

        for position, family_name in enumerate(imported_family_names, start=1):
            conn.execute(
                """
                INSERT INTO small_group_families (family_name, position)
                VALUES (?, ?)
                """,
                (family_name, position),
            )


def reset_database_data() -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM meeting_log")
        conn.execute("DELETE FROM lesson_notes")
        conn.execute("DELETE FROM lesson_verse_text")
        conn.execute("DELETE FROM lesson_custom_questions")
        conn.execute("DELETE FROM upcoming_meal_signups")
        conn.execute("DELETE FROM upcoming_meetings")
        conn.execute("DELETE FROM small_group_families")
        for position, family_name in enumerate(DEFAULT_FAMILY_OPTIONS, start=1):
            conn.execute(
                """
                INSERT INTO small_group_families (family_name, position)
                VALUES (?, ?)
                """,
                (family_name, position),
            )


def render_status_badge(status: str) -> str:
    class_lookup = {
        "Done": "sg-status-badge-done",
        "Not done": "sg-status-badge-notdone",
        "Skipped": "sg-status-badge-skipped",
        "Postponed": "sg-status-badge-postponed",
    }
    css_class = class_lookup.get(status, "sg-status-badge-notdone")
    return f"<span class='sg-status-badge {css_class}'>{escape(status)}</span>"


def render_page_header(
    title: str,
    subtitle: str,
    eyebrow: str = "",
) -> None:
    eyebrow_html = (
        f"<div class='sg-page-eyebrow'>{escape(eyebrow)}</div>" if str(eyebrow).strip() else ""
    )
    st.markdown(
        (
            "<div class='sg-page-header'>"
            f"{eyebrow_html}"
            f"<h1 class='sg-page-title'>{escape(title)}</h1>"
            f"<p class='sg-page-subtitle'>{escape(subtitle)}</p>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_section_header(
    title: str,
    description: str = "",
) -> None:
    desc_html = (
        f"<p class='sg-section-description'>{escape(description)}</p>"
        if str(description).strip()
        else ""
    )
    st.markdown(
        (
            "<div class='sg-section-header'>"
            f"<h3 class='sg-section-title'>{escape(title)}</h3>"
            f"{desc_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_empty_state(title: str, description: str) -> None:
    st.markdown(
        (
            "<div class='sg-empty-state'>"
            f"<p class='sg-empty-state-title'>{escape(title)}</p>"
            f"<p class='sg-empty-state-description'>{escape(description)}</p>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def get_lesson_unit_overview(
    lessons_df: pd.DataFrame,
) -> List[dict]:
    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    unit_definitions = [
        {
            "title": "Unit 1: Foundations",
            "summary": "Core BibleProject themes that frame the story of Scripture.",
            "weeks": list(range(1, 9)),
        },
        {
            "title": "Unit 2: Sermon on the Mount",
            "summary": "Jesus' Kingdom teaching and practical discipleship.",
            "weeks": list(range(9, 19)),
        },
        {
            "title": "Unit 3: God's Character",
            "summary": "How God describes Himself and forms our character.",
            "weeks": list(range(19, 25)),
        },
    ]

    units: List[dict] = []
    for unit in unit_definitions:
        unit_weeks = [int(week) for week in unit["weeks"] if int(week) in theme_lookup]
        if not unit_weeks:
            continue
        lesson_rows = [
            f"Lesson {int(week)}: {str(theme_lookup.get(int(week), '')).strip()}"
            for week in unit_weeks
        ]
        units.append(
            {
                "title": unit["title"],
                "summary": unit["summary"],
                "lessons": lesson_rows,
            }
        )

    return units


def render_inline_navigation(pages: List[str]) -> None:
    with st.container(border=True):
        st.markdown("<div class='sg-inline-nav-title'>Pages</div>", unsafe_allow_html=True)
        nav_cols = st.columns(len(pages))
        active_page = str(st.session_state.get("active_page", "Home"))
        for idx, nav_page in enumerate(pages):
            if nav_cols[idx].button(
                nav_page,
                key=f"inline_nav_button_{nav_page}",
                type="primary" if active_page == nav_page else "secondary",
                use_container_width=True,
            ):
                st.session_state["active_page"] = nav_page
                st.rerun()


def render_dashboard(lessons_df: pd.DataFrame) -> None:
    status_map = derive_status_map(lessons_df)
    weeks = lessons_df["week"].astype(int).tolist()
    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    suggested_week = next((week for week in weeks if status_map.get(week) != "Done"), None)

    upcoming_df = fetch_upcoming_meetings(lessons_df)
    scheduled_dates_by_week: Dict[int, List[str]] = {}
    for row in upcoming_df.to_dict(orient="records"):
        week = int(row["lesson_week"])
        raw_date = str(row["meeting_date"])
        try:
            date_label = date.fromisoformat(raw_date).strftime("%m/%d/%y")
        except (TypeError, ValueError):
            date_label = raw_date
        if week not in scheduled_dates_by_week:
            scheduled_dates_by_week[week] = []
        if date_label not in scheduled_dates_by_week[week]:
            scheduled_dates_by_week[week].append(date_label)

    scheduled_weeks = set(scheduled_dates_by_week.keys())
    next_available_week = next(
        (week for week in weeks if status_map.get(week) != "Done" and week not in scheduled_weeks),
        None,
    )

    default_new_week = (
        next_available_week
        if next_available_week is not None
        else (suggested_week if suggested_week is not None else weeks[0])
    )

    if upcoming_df.empty:
        with st.container(border=True):
            render_empty_state(
                "No upcoming meetings scheduled",
                "Create your next meeting date in Admin. Once added, it will appear here for assignment and completion tracking.",
            )
        if st.button(
            "Add your first meeting",
            key="dashboard_go_admin_empty",
            type="primary",
            use_container_width=True,
        ):
            st.session_state["active_page"] = "Admin"
            st.rerun()
        return

    date_ids = upcoming_df["id"].astype(int).tolist()
    if (
        "dashboard_selected_upcoming_id" not in st.session_state
        or st.session_state["dashboard_selected_upcoming_id"] not in date_ids
    ):
        st.session_state["dashboard_selected_upcoming_id"] = date_ids[0]

    calendar_picked_id = get_query_param_int("dashboard_pick")
    if calendar_picked_id is not None:
        if int(calendar_picked_id) in date_ids:
            st.session_state["dashboard_selected_upcoming_id"] = int(calendar_picked_id)
        clear_query_param("dashboard_pick")

    selected_id = int(st.session_state["dashboard_selected_upcoming_id"])
    date_rows = upcoming_df.to_dict(orient="records")
    row_by_id = {int(row["id"]): row for row in date_rows}

    with st.container(border=True):
        render_section_header(
            "Upcoming Meeting Dates",
            "Choose a gathering to see who is serving, what meal is planned, and what still needs coverage.",
        )
        ordered_ids = [int(row["id"]) for row in date_rows]
        selected_calendar_date = ""
        if selected_id in row_by_id:
            selected_calendar_date = str(row_by_id[selected_id].get("meeting_date", ""))
        render_upcoming_calendar(
            upcoming_df,
            selected_date=selected_calendar_date,
        )

        def date_selector_label(row_id: int) -> str:
            row = row_by_id.get(int(row_id), {})
            label_prefix = "Next" if ordered_ids and int(row_id) == int(ordered_ids[0]) else "Upcoming"
            lesson_num = int(row.get("lesson_week", 0)) if row else 0
            lesson_theme = str(theme_lookup.get(lesson_num, "")).strip()
            lesson_part = f"Lesson {lesson_num}" if lesson_num else "Lesson"
            if lesson_theme:
                lesson_part = f"{lesson_part} - {lesson_theme}"
            return f"{label_prefix}: {format_meeting_date(str(row.get('meeting_date', '')))} | {lesson_part}"

        selected_index = ordered_ids.index(selected_id) if selected_id in ordered_ids else 0
        picked_id = st.selectbox(
            "Gathering to update",
            options=ordered_ids,
            index=selected_index,
            format_func=date_selector_label,
            key="dashboard_upcoming_picker",
        )
        if int(picked_id) != int(selected_id):
            st.session_state["dashboard_selected_upcoming_id"] = int(picked_id)
            st.rerun()

        if st.button(
            "Manage gathering dates",
            key="dashboard_go_admin_add_meeting",
            use_container_width=True,
        ):
            st.session_state["active_page"] = "Admin"
            st.rerun()

    selected_id = int(st.session_state["dashboard_selected_upcoming_id"])
    selected_row = upcoming_df[upcoming_df["id"] == selected_id].iloc[0]
    selected_summary = summarize_upcoming_meeting(selected_row.to_dict())
    selected_meeting_date = str(selected_row["meeting_date"])
    try:
        meal_date_label = date.fromisoformat(selected_meeting_date).strftime("%m/%d/%y")
    except (TypeError, ValueError):
        meal_date_label = selected_meeting_date
    dashboard_person_options = fetch_small_group_families()
    if not dashboard_person_options:
        dashboard_person_options = list(DEFAULT_FAMILY_OPTIONS)
    dashboard_person_options = [TBD_OPTION] + [
        name for name in dashboard_person_options if name and name != TBD_OPTION
    ]

    saved_host = str(selected_row["host_name"]).strip()
    saved_facilitator = str(selected_row["facilitator_name"]).strip()
    for saved_name in [saved_host, saved_facilitator]:
        if saved_name and saved_name not in dashboard_person_options:
            dashboard_person_options.append(saved_name)

    host_default_selection = (
        saved_host if saved_host in dashboard_person_options else TBD_OPTION
    )
    facilitator_default_selection = (
        saved_facilitator
        if saved_facilitator in dashboard_person_options
        else TBD_OPTION
    )
    saved_main_meal = str(selected_row.get("main_meal", "")).strip()

    lesson_default_week = (
        int(selected_row["lesson_week"])
        if int(selected_row["lesson_week"]) in weeks
        else default_new_week
    )
    lesson_touch_key = f"dashboard_lesson_dropdown_touched_{selected_id}"
    if lesson_touch_key not in st.session_state:
        st.session_state[lesson_touch_key] = False
    original_notes = "" if pd.isna(selected_row["notes"]) else str(selected_row["notes"])

    def lesson_option_label(week: int) -> str:
        base = f"Lesson {week} - {theme_lookup.get(week, '')}"
        markers: List[str] = []
        if status_map.get(week) == "Done":
            markers.append("✅ done")
        if markers:
            return f"{base} ({' | '.join(markers)})"
        return base

    st.markdown("<div id='meeting-details'></div>", unsafe_allow_html=True)
    with st.container(border=True):
        render_section_header(
            f"Meeting Details: {format_meeting_date(selected_meeting_date)}",
            "Start with the serving needs below. Hosting, facilitating, and meal support all update this gathering.",
        )
        tab_signup, tab_details, tab_complete = st.tabs(["Serve", "Details", "Complete"])

        with tab_signup:
            serve_col, meal_col = st.columns([1.0, 1.15])
            with serve_col:
                with st.container(border=True):
                    render_section_header(
                        "Serve this gathering",
                        "Sign up to host or facilitate so everyone can quickly see what is covered.",
                    )
                    if selected_summary["open_needs"]:
                        st.markdown(
                            (
                                "<div class='sg-pill-row'>"
                                + "".join(
                                    render_support_pill(f"Needs {need}", "open")
                                    for need in selected_summary["open_needs"][:3]
                                )
                                + "</div>"
                            ),
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f"<div class='sg-pill-row'>{render_support_pill('All key roles are covered', 'ready')}</div>",
                            unsafe_allow_html=True,
                        )

                    people_left, people_right = st.columns(2)
                    with people_left:
                        host_select = st.selectbox(
                            "Sign up to host",
                            options=dashboard_person_options,
                            index=dashboard_person_options.index(host_default_selection),
                            key=f"dashboard_host_select_{selected_id}",
                        )
                    with people_right:
                        facilitator_select = st.selectbox(
                            "Facilitate this lesson",
                            options=dashboard_person_options,
                            index=dashboard_person_options.index(facilitator_default_selection),
                            key=f"dashboard_facilitator_select_{selected_id}",
                        )

                    role_has_unsaved_changes = (
                        str(host_select) != str(host_default_selection)
                        or str(facilitator_select) != str(facilitator_default_selection)
                    )
                    if role_has_unsaved_changes:
                        st.markdown(
                            "<div class='sg-save-required'>Serving updates are ready to save.</div>",
                            unsafe_allow_html=True,
                        )

                    if st.button(
                        "Save hosting and facilitator",
                        key=f"dashboard_save_roles_{selected_id}",
                        type="primary",
                        use_container_width=True,
                    ):
                        if not role_has_unsaved_changes:
                            notify("Everything is already up to date.", "info")
                        else:
                            current_lesson_week = int(
                                st.session_state.get(
                                    f"dashboard_lesson_select_{selected_id}",
                                    lesson_default_week,
                                )
                            )
                            current_notes = str(
                                st.session_state.get(
                                    f"dashboard_notes_{selected_id}",
                                    original_notes,
                                )
                            )
                            update_upcoming_meeting(
                                selected_id,
                                current_lesson_week,
                                str(host_select),
                                str(facilitator_select),
                                current_notes,
                                saved_main_meal,
                            )
                            queue_message("Thanks for serving. Hosting details were saved.")
                            st.rerun()

            with meal_col:
                with st.container(border=True):
                    meal_intro = (
                        f"Main meal: {saved_main_meal}. Invite sides, desserts, and drinks below."
                        if saved_main_meal
                        else "Add the main meal first, then invite others to bring sides, desserts, or drinks."
                    )
                    render_section_header(
                        f"Bring a meal ({meal_date_label})",
                        meal_intro,
                    )
                    main_meal_input = st.text_input(
                        "Main meal provided by host",
                        value=saved_main_meal,
                        key=f"dashboard_main_meal_{selected_id}",
                        placeholder="Examples: Taco bar, Soup and salad, Baked ziti",
                    )

                    meal_df = fetch_upcoming_meal_signups(selected_id)
                    original_meal_rows = normalize_meal_rows(meal_df.to_dict(orient="records"))
                    meal_editor_df = st.data_editor(
                        meal_df,
                        hide_index=True,
                        use_container_width=True,
                        num_rows="dynamic",
                        height=210,
                        column_order=["Name", "Dish"],
                        column_config={
                            "Name": st.column_config.TextColumn("Who is bringing food", width="medium"),
                            "Dish": st.column_config.TextColumn("Item", width="large"),
                        },
                        key=f"dashboard_meal_editor_{selected_id}",
                    )
                    edited_meal_rows = normalize_meal_rows(meal_editor_df.to_dict(orient="records"))
                    meal_has_unsaved_changes = (
                        edited_meal_rows != original_meal_rows
                        or str(main_meal_input).strip() != str(saved_main_meal)
                    )

                    if meal_has_unsaved_changes:
                        st.markdown(
                            "<div class='sg-save-required'>Meal updates are ready to save.</div>",
                            unsafe_allow_html=True,
                        )

                    if st.button(
                        "Save meal support",
                        key=f"dashboard_save_meal_{selected_id}",
                        type="primary",
                        use_container_width=True,
                    ):
                        if not meal_has_unsaved_changes:
                            notify("Everything is already up to date.", "info")
                        else:
                            update_upcoming_meeting(
                                selected_id,
                                lesson_default_week,
                                saved_host,
                                saved_facilitator,
                                original_notes,
                                str(main_meal_input).strip(),
                            )
                            saved_entries = save_upcoming_meal_signups(
                                selected_id, meal_editor_df.to_dict(orient="records")
                            )
                            queue_message(
                                f"Thanks for serving. Meal support was updated ({saved_entries} item{'s' if saved_entries != 1 else ''})."
                            )
                            st.rerun()

        with tab_details:
            render_section_header(
                "Meeting Details",
                "Update lesson assignment and any notes the group should remember for this date.",
            )
            date_col, lesson_col = st.columns([0.95, 1.45])
            with date_col:
                st.text_input("Date", value=format_meeting_date(selected_meeting_date), disabled=True)
            with lesson_col:
                lesson_week = st.selectbox(
                    "Lesson",
                    options=weeks,
                    index=weeks.index(lesson_default_week),
                    format_func=lesson_option_label,
                    key=f"dashboard_lesson_select_{selected_id}",
                    on_change=mark_state_true,
                    args=(lesson_touch_key,),
                )
                if st.session_state.get(lesson_touch_key, False):
                    selected_lesson_dates = scheduled_dates_by_week.get(int(lesson_week), [])
                    if selected_lesson_dates:
                        preview = ", ".join(selected_lesson_dates[:2])
                        if len(selected_lesson_dates) > 2:
                            preview = f"{preview} (+{len(selected_lesson_dates) - 2})"
                        st.caption(f"Other dates using this lesson: {preview}")

            selected_notes = st.text_area(
                "Notes for this gathering",
                value=selected_row["notes"],
                key=f"dashboard_notes_{selected_id}",
                height=96,
                placeholder="Add prayer focus, reminders, or any details the group should know before meeting.",
            )

            has_unsaved_changes = (
                int(lesson_week) != int(lesson_default_week)
                or str(selected_notes) != str(original_notes)
            )

            if has_unsaved_changes:
                st.markdown(
                    (
                        "<div class='sg-save-required'>"
                        "Gathering details are ready to save."
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )

            save_date_details = st.button(
                "Save meeting details",
                key=f"dashboard_save_date_details_{selected_id}",
                type="primary",
                use_container_width=True,
            )

            if save_date_details:
                if not has_unsaved_changes:
                    notify("No meeting detail changes to save.", "info")
                else:
                    current_host = str(
                        st.session_state.get(
                            f"dashboard_host_select_{selected_id}",
                            host_default_selection,
                        )
                    )
                    current_facilitator = str(
                        st.session_state.get(
                            f"dashboard_facilitator_select_{selected_id}",
                            facilitator_default_selection,
                        )
                    )
                    current_main_meal = str(
                        st.session_state.get(
                            f"dashboard_main_meal_{selected_id}",
                            saved_main_meal,
                        )
                    ).strip()
                    update_upcoming_meeting(
                        selected_id,
                        int(lesson_week),
                        current_host,
                        current_facilitator,
                        selected_notes,
                        current_main_meal,
                    )
                    queue_message("Gathering details were saved.")
                    st.rerun()

        with tab_complete:
            render_section_header(
                "Meeting Completion",
                "When the gathering wraps up, log it here so the group schedule stays current.",
            )
            completion_confirm = st.checkbox(
                "This gathering is complete",
                key=f"dashboard_completion_confirm_{selected_id}",
            )
            if st.button(
                "Log date completed",
                key=f"dashboard_log_date_completed_{selected_id}",
                type="primary",
                disabled=not completion_confirm,
                use_container_width=True,
            ):
                current_lesson_week = int(
                    st.session_state.get(
                        f"dashboard_lesson_select_{selected_id}",
                        lesson_default_week,
                    )
                )
                current_host = str(
                    st.session_state.get(
                        f"dashboard_host_select_{selected_id}",
                        host_default_selection,
                    )
                )
                current_facilitator = str(
                    st.session_state.get(
                        f"dashboard_facilitator_select_{selected_id}",
                        facilitator_default_selection,
                    )
                )
                current_notes = str(
                    st.session_state.get(
                        f"dashboard_notes_{selected_id}",
                        "" if pd.isna(selected_row["notes"]) else str(selected_row["notes"]),
                    )
                )
                try:
                    completion_date = date.fromisoformat(selected_meeting_date)
                except ValueError:
                    notify("Meeting date is invalid; update the date in Admin first.", "error")
                else:
                    add_meeting_log(
                        completion_date,
                        current_lesson_week,
                        "Completed",
                        current_notes,
                        host_name=current_host,
                        facilitator_name=current_facilitator,
                    )
                    st.session_state[f"dashboard_completion_confirm_{selected_id}"] = False
                    queue_message("Thanks for serving. This gathering was logged as complete.")
                    st.rerun()


def render_lessons_page(lessons_df: pd.DataFrame) -> None:
    status_map = derive_status_map(lessons_df)
    weeks = lessons_df["week"].astype(int).tolist()
    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    unit_overview = get_lesson_unit_overview(lessons_df)
    upcoming_df = fetch_upcoming_meetings(lessons_df)
    scheduled_date_by_week: Dict[int, str] = {}
    for row in upcoming_df.to_dict(orient="records"):
        week = int(row["lesson_week"])
        if week in scheduled_date_by_week:
            continue
        raw_date = str(row["meeting_date"])
        try:
            parsed = date.fromisoformat(raw_date)
            scheduled_date_by_week[week] = parsed.strftime("%m/%d/%y")
        except (TypeError, ValueError):
            scheduled_date_by_week[week] = raw_date

    with st.container(border=True):
        render_section_header(
            "Lesson Plans",
            "Narrow by progress status or search by lesson theme.",
        )
        f1, f2 = st.columns([1, 1.6])
        with f1:
            status_filter = st.selectbox(
                "Status",
                options=["All", "Needs planning", "Scheduled", "Completed"],
                key="lessons_status_filter",
            )
        with f2:
            theme_search = st.text_input(
                "Search theme",
                key="lessons_theme_search",
                placeholder="Try: Sabbath, Kingdom, Forgiveness...",
            ).strip()

    filtered_weeks: List[int] = []
    for week in weeks:
        status = status_map.get(week, "Not done")
        is_done = status == "Done"
        is_scheduled = week in scheduled_date_by_week
        status_match = True
        if status_filter == "Needs planning":
            status_match = (not is_done) and (not is_scheduled)
        elif status_filter == "Scheduled":
            status_match = (not is_done) and is_scheduled
        elif status_filter == "Completed":
            status_match = is_done

        theme_value = str(theme_lookup.get(week, "")).strip().lower()
        search_match = (not theme_search) or (theme_search.lower() in theme_value)
        if status_match and search_match:
            filtered_weeks.append(int(week))

    if not filtered_weeks:
        with st.container(border=True):
            render_empty_state(
                "No lessons match your filters",
                "Try clearing your search term or selecting a different status.",
            )
        return

    next_not_done_week = next((week for week in weeks if status_map.get(week) != "Done"), None)
    if (
        "lessons_selected_week" not in st.session_state
        or st.session_state["lessons_selected_week"] not in filtered_weeks
    ):
        preferred_week = (
            next_not_done_week
            if next_not_done_week is not None and next_not_done_week in filtered_weeks
            else filtered_weeks[0]
        )
        st.session_state["lessons_selected_week"] = preferred_week

    picked_week = get_query_param_int("lesson_pick")
    if picked_week is not None and picked_week in filtered_weeks:
        st.session_state["lessons_selected_week"] = int(picked_week)
    if hasattr(st, "query_params"):
        if "lesson_pick" in st.query_params:
            clear_query_param("lesson_pick")
    else:
        legacy_params = st.experimental_get_query_params()
        if "lesson_pick" in legacy_params:
            clear_query_param("lesson_pick")

    selected_week = int(st.session_state["lessons_selected_week"])

    with st.container(border=True):
        render_section_header(
            "Lesson Selector",
            "Scroll sideways and choose a lesson card to open its details.",
        )
        cards_html = ['<div class="sg-lesson-rolodex-scroll">']
        for week in filtered_weeks:
            is_selected = int(week) == int(selected_week)
            theme = str(theme_lookup.get(week, "(Untitled lesson)")).strip()
            short_theme = theme if len(theme) <= 24 else theme[:21].rstrip() + "..."
            status_text = status_map.get(week, "Not done")
            if status_text == "Not done" and int(week) in scheduled_date_by_week:
                status_text = f"Scheduled {scheduled_date_by_week[int(week)]}"
            selected_class = " selected" if is_selected else ""
            cards_html.append(
                f'<a class="sg-lesson-card{selected_class}" href="?lesson_pick={int(week)}" target="_self">'
                f"<div class='sg-lesson-card-title'>Lesson {int(week)}: {escape(short_theme)}</div>"
                f"<div class='sg-lesson-card-status'>{escape(status_text)}</div>"
                "</a>"
            )
        cards_html.append("</div>")
        st.markdown("".join(cards_html), unsafe_allow_html=True)

    lesson = lessons_df[lessons_df["week"] == selected_week].iloc[0]
    lesson_status = status_map.get(selected_week, "Not done")
    verse_ref = str(lesson["anchor_verse"])
    verse_url = f"https://www.biblegateway.com/passage/?search={quote_plus(verse_ref)}"
    video_url = str(lesson["video_url"])
    lesson_theme = str(lesson["theme"]).strip()
    big_idea_core = normalize_big_idea_text(
        str(lesson.get("big_idea", "")).strip(),
        lesson_theme,
    )
    big_idea = f"Tonight's big idea: {big_idea_core}"
    lesson_question_context = {
        "theme": lesson_theme,
        "big_idea": big_idea_core,
        "one_sentence_summary": str(lesson["one_sentence_summary"]).strip(),
        "anchor_verse": verse_ref,
    }
    effective_questions, _ = get_effective_questions(int(selected_week), lesson_question_context)
    scheduled_date_label = scheduled_date_by_week.get(int(selected_week), "")

    with st.container(border=True):
        left_col, right_col = st.columns([1.55, 1.0])
        with left_col:
            st.markdown(
                f"<div class='sg-lesson-title'>Lesson {selected_week}: {escape(lesson_theme)}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='sg-lesson-status-line'>{render_status_badge(lesson_status)}</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                (
                    "<div class='sg-lesson-section'>"
                    "<div class='sg-lesson-section-title'>Summary</div>"
                    f"<div class='sg-lesson-summary'>{escape(str(lesson['one_sentence_summary']))}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                (
                    "<div class='sg-lesson-section'>"
                    "<div class='sg-lesson-section-title'>Big Idea</div>"
                    f"<div class='sg-lesson-big-idea'>{escape(big_idea)}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                (
                    "<div class='sg-lesson-section'>"
                    "<div class='sg-lesson-section-title'>Anchor Verse</div>"
                    f"<div class='sg-anchor-ref'>{escape(verse_ref)}</div>"
                    f"<div class='sg-tight-link'><a href='{escape(verse_url + '&version=NIV', quote=True)}' target='_blank'>Open verse in BibleGateway</a></div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                (
                    "<div class='sg-lesson-section'>"
                    "<div class='sg-lesson-section-title'>Video</div>"
                    f"<div class='sg-lesson-summary'>{escape(str(lesson['video_name']))}</div>"
                    f"<div class='sg-tight-link'><a href='{escape(video_url, quote=True)}' target='_blank'>{escape(video_url)}</a></div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

            discussion_blocks: List[str] = []
            for level in QUESTION_LEVELS:
                questions_html = "".join(
                    f"<li>{escape(question)}</li>" for question in effective_questions[level]
                )
                discussion_blocks.append(
                    f"<li><span class='sg-outline-level'>{escape(QUESTION_LABELS[level])}</span><ul>{questions_html}</ul></li>"
                )

            outline_html = (
                "<div class='sg-lesson-section'>"
                "<div class='sg-lesson-section-title'>Discussion Outline</div>"
                "<div class='sg-outline'><ul>"
                f"<li>{escape(big_idea)}</li>"
                f"<li>Video: <a href='{escape(video_url, quote=True)}' target='_blank'>{escape(str(lesson['video_name']))}</a></li>"
                f"<li>Anchor Verse: {escape(verse_ref)}</li>"
                "<li>Questions<ul>"
                f"{''.join(discussion_blocks)}"
                "</ul></li>"
                "</ul></div>"
                "</div>"
            )
            st.markdown(outline_html, unsafe_allow_html=True)

        with right_col:
            with st.container(border=True):
                render_section_header("Lesson Status")
                st.markdown(render_status_badge(lesson_status), unsafe_allow_html=True)
                if scheduled_date_label and lesson_status != "Done":
                    st.caption(f"Scheduled date: {scheduled_date_label}")

            with st.container(border=True):
                render_section_header("Facilitator Notes")
                with st.form(f"facilitator_notes_form_{selected_week}"):
                    notes_text = st.text_area(
                        "Facilitator notes",
                        value=get_lesson_notes(int(selected_week)),
                        height=180,
                        label_visibility="collapsed",
                        placeholder="Capture prayer requests, context, and follow-up ideas.",
                    )
                    save_notes = st.form_submit_button(
                        "Save notes",
                        type="primary",
                        use_container_width=True,
                    )

                if save_notes:
                    save_lesson_notes(int(selected_week), notes_text)
                    queue_message(f"Facilitator notes saved for lesson {selected_week}.")
                    st.rerun()

            with st.container(border=True):
                render_section_header(
                    "Mark Complete",
                    "Use this when the group has finished this lesson.",
                )
                if st.button(
                    "Mark lesson complete",
                    type="primary",
                    use_container_width=True,
                    key=f"lesson_completed_{selected_week}",
                ):
                    add_meeting_log(
                        date.today(),
                        int(selected_week),
                        "Completed",
                        "Marked completed from Lessons page",
                    )

                    updated_done_weeks = get_done_weeks()
                    updated_upcoming_df = fetch_upcoming_meetings(lessons_df)
                    scheduled_weeks = set(updated_upcoming_df["lesson_week"].astype(int).tolist())
                    if int(selected_week) in scheduled_weeks:
                        scheduled_weeks.discard(int(selected_week))

                    next_available_week = next(
                        (
                            week
                            for week in weeks
                            if week not in updated_done_weeks and week not in scheduled_weeks
                        ),
                        None,
                    )
                    if next_available_week is None:
                        next_available_week = next(
                            (week for week in weeks if week not in updated_done_weeks),
                            None,
                        )

                    if next_available_week is not None:
                        st.session_state["lessons_selected_week"] = int(next_available_week)

                        refreshed_upcoming_df = fetch_upcoming_meetings(lessons_df)
                        same_lesson_upcoming = refreshed_upcoming_df[
                            refreshed_upcoming_df["lesson_week"] == int(selected_week)
                        ]
                        if not same_lesson_upcoming.empty:
                            row = same_lesson_upcoming.iloc[0]
                            host_name = "" if pd.isna(row["host_name"]) else str(row["host_name"])
                            facilitator_name = (
                                "" if pd.isna(row["facilitator_name"]) else str(row["facilitator_name"])
                            )
                            notes = "" if pd.isna(row["notes"]) else str(row["notes"])
                            update_upcoming_meeting(
                                int(row["id"]),
                                int(next_available_week),
                                host_name,
                                facilitator_name,
                                notes,
                            )
                            queue_message(
                                f"Lesson {selected_week} marked complete. Advanced one upcoming meeting to lesson {next_available_week}."
                            )
                        else:
                            queue_message(f"Lesson {selected_week} marked complete.")
                    else:
                        st.session_state["lessons_selected_week"] = int(selected_week)
                        queue_message(
                            f"Lesson {selected_week} marked complete. All lessons are now done."
                        )
                    st.rerun()

    if unit_overview:
        with st.container(border=True):
            render_section_header(
                "Curriculum Overview",
                "Three units move the group from biblical foundations to practical discipleship and trust in God's character.",
            )
            for unit in unit_overview:
                st.markdown(f"**{unit['title']}**")
                st.caption(unit["summary"])
                with st.expander(f"View lessons in {unit['title']}", expanded=False):
                    lesson_lines = "\n".join([f"- {line}" for line in unit["lessons"]])
                    st.markdown(lesson_lines)


def render_meeting_log_page(lessons_df: pd.DataFrame) -> None:
    weeks = lessons_df["week"].astype(int).tolist()
    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    person_options = [name for name in get_person_options() if name != "Other"]
    if not person_options:
        person_options = [TBD_OPTION]
    status_map = derive_status_map(lessons_df)
    suggested_week = next((week for week in weeks if status_map.get(week) != "Done"), None)
    upcoming_df = fetch_upcoming_meetings(lessons_df)
    done_weeks = get_done_weeks()
    completed_count = len([week for week in done_weeks if 1 <= week <= TOTAL_LESSONS])
    remaining_count = max(TOTAL_LESSONS - completed_count, 0)
    render_page_header(
        "Admin & Operations",
        "Set gathering dates, update family lists, and keep the group's plan organized.",
        "Admin",
    )
    m1, m2, m3 = st.columns(3)
    m1.metric("Completed", f"{completed_count}/{TOTAL_LESSONS}")
    m2.metric("Remaining", str(remaining_count))
    m3.metric("Upcoming dates", str(len(upcoming_df)))

    scheduled_dates_by_week: Dict[int, List[str]] = {}
    for row in upcoming_df.to_dict(orient="records"):
        week = int(row["lesson_week"])
        date_label = format_meeting_date(str(row["meeting_date"]))
        if week not in scheduled_dates_by_week:
            scheduled_dates_by_week[week] = []
        if date_label not in scheduled_dates_by_week[week]:
            scheduled_dates_by_week[week].append(date_label)

    scheduled_weeks = set(scheduled_dates_by_week.keys())
    next_available_week = next(
        (week for week in weeks if status_map.get(week) != "Done" and week not in scheduled_weeks),
        None,
    )
    default_week = (
        next_available_week
        if next_available_week is not None
        else (suggested_week if suggested_week is not None else weeks[0])
    )

    def lesson_dropdown_label(week: int) -> str:
        label = f"Lesson {week} - {theme_lookup.get(week, '')}"
        extras: List[str] = []
        if status_map.get(week) == "Done":
            extras.append("completed")
        scheduled_dates = scheduled_dates_by_week.get(week, [])
        if scheduled_dates:
            preview = ", ".join(scheduled_dates[:2])
            if len(scheduled_dates) > 2:
                preview = f"{preview} (+{len(scheduled_dates) - 2})"
            extras.append(preview)
        if extras:
            return f"{label} ({' | '.join(extras)})"
        return label

    schedule_tab, families_tab, history_tab = st.tabs(
        ["Schedule Dates", "Families", "Meeting History"]
    )

    with schedule_tab:
        with st.container(border=True):
            render_section_header(
                "Add Meeting Date",
                "Create an upcoming meeting date. Host and facilitator default to TBD.",
            )
            with st.form("meeting_log_add_meeting_date_form", clear_on_submit=True):
                new_meeting_date = st.date_input(
                    "Meeting date",
                    value=date.today(),
                    format="MM/DD/YYYY",
                )
                new_meeting_week = st.selectbox(
                    "Lesson",
                    options=weeks,
                    index=weeks.index(default_week),
                    format_func=lesson_dropdown_label,
                )
                if next_available_week is not None:
                    st.caption(f"Next available lesson: {lesson_dropdown_label(next_available_week)}")
                else:
                    st.caption("All unfinished lessons are already scheduled.")
                add_meeting_date = st.form_submit_button(
                    "Add meeting date",
                    type="primary",
                    use_container_width=True,
                )

        if add_meeting_date:
            add_upcoming_meeting(
                new_meeting_date,
                int(new_meeting_week),
                TBD_OPTION,
                TBD_OPTION,
                "",
            )
            queue_message("Meeting date added. It now appears on Home.")
            st.rerun()

        with st.container(border=True):
            render_section_header(
                "Active Meeting Dates",
                "Select a date to edit assigned lesson, host, facilitator, or notes.",
            )

            if upcoming_df.empty:
                render_empty_state(
                    "No upcoming dates yet",
                    "Add your first meeting date above to begin planning.",
                )
            else:
                upcoming_ids = upcoming_df["id"].astype(int).tolist()
                if (
                    "meeting_log_selected_upcoming_id" not in st.session_state
                    or st.session_state["meeting_log_selected_upcoming_id"] not in upcoming_ids
                ):
                    st.session_state["meeting_log_selected_upcoming_id"] = upcoming_ids[0]

                selected_upcoming_id = int(st.session_state["meeting_log_selected_upcoming_id"])
                rolodex_rows = upcoming_df.to_dict(orient="records")
                rolodex_cols = 4
                for row_idx, row in enumerate(rolodex_rows):
                    if row_idx % rolodex_cols == 0:
                        cols = st.columns(rolodex_cols)
                    row_id = int(row["id"])
                    is_selected = row_id == selected_upcoming_id
                    with cols[row_idx % rolodex_cols]:
                        label_prefix = "Next" if row_idx == 0 else "Upcoming"
                        label = f"{label_prefix}: {format_meeting_date(row['meeting_date'])}"
                        if st.button(
                            label,
                            key=f"meeting_log_select_upcoming_{row_id}",
                            type="primary" if is_selected else "secondary",
                            use_container_width=True,
                        ):
                            st.session_state["meeting_log_selected_upcoming_id"] = row_id
                            st.rerun()

                selected_row = upcoming_df[upcoming_df["id"] == selected_upcoming_id].iloc[0]
                selected_week = (
                    int(selected_row["lesson_week"])
                    if int(selected_row["lesson_week"]) in weeks
                    else default_week
                )
                edit_person_options = list(person_options)
                saved_host_name = str(selected_row["host_name"]).strip()
                saved_facilitator_name = str(selected_row["facilitator_name"]).strip()
                if saved_host_name and saved_host_name not in edit_person_options:
                    edit_person_options.append(saved_host_name)
                if saved_facilitator_name and saved_facilitator_name not in edit_person_options:
                    edit_person_options.append(saved_facilitator_name)
                host_default_selection = (
                    saved_host_name if saved_host_name in edit_person_options else edit_person_options[0]
                )
                facilitator_default_selection = (
                    saved_facilitator_name
                    if saved_facilitator_name in edit_person_options
                    else edit_person_options[0]
                )

                with st.container(border=True):
                    render_section_header(
                        f"Edit Meeting: {format_meeting_date(selected_row['meeting_date'])}",
                    )
                    st.text_input(
                        "Date",
                        value=format_meeting_date(selected_row["meeting_date"]),
                        disabled=True,
                    )
                    edit_week = st.selectbox(
                        "Lesson",
                        options=weeks,
                        index=weeks.index(selected_week),
                        format_func=lesson_dropdown_label,
                        key=f"meeting_log_edit_upcoming_lesson_{selected_upcoming_id}",
                    )
                    c1, c2 = st.columns(2)
                    edit_host_select = c1.selectbox(
                        "Host",
                        options=edit_person_options,
                        index=edit_person_options.index(host_default_selection),
                        key=f"meeting_log_edit_upcoming_host_{selected_upcoming_id}",
                    )
                    edit_facilitator_select = c2.selectbox(
                        "Facilitator",
                        options=edit_person_options,
                        index=edit_person_options.index(facilitator_default_selection),
                        key=f"meeting_log_edit_upcoming_facilitator_{selected_upcoming_id}",
                    )
                    edit_notes = st.text_area(
                        "Notes",
                        value=str(selected_row["notes"]),
                        height=92,
                        key=f"meeting_log_edit_upcoming_notes_{selected_upcoming_id}",
                        placeholder="Optional reminders, agenda notes, or logistics.",
                    )

                    original_edit_notes = (
                        "" if pd.isna(selected_row["notes"]) else str(selected_row["notes"])
                    )
                    edit_has_unsaved_changes = (
                        int(edit_week) != int(selected_week)
                        or str(edit_host_select) != str(host_default_selection)
                        or str(edit_facilitator_select) != str(facilitator_default_selection)
                        or str(edit_notes) != str(original_edit_notes)
                    )

                    if edit_has_unsaved_changes:
                        st.markdown(
                            (
                                "<div class='sg-save-required'>"
                                "Unsaved changes detected. Click <b>Save meeting date changes</b>."
                                "</div>"
                            ),
                            unsafe_allow_html=True,
                        )

                    if st.button(
                        "Save meeting date changes",
                        key=f"meeting_log_save_upcoming_{selected_upcoming_id}",
                        type="primary",
                        use_container_width=True,
                    ):
                        if not edit_has_unsaved_changes:
                            notify("No meeting date changes to save.", "info")
                        else:
                            update_upcoming_meeting(
                                selected_upcoming_id,
                                int(edit_week),
                                str(edit_host_select),
                                str(edit_facilitator_select),
                                edit_notes,
                            )
                            queue_message("Selected meeting date updated.")
                            st.rerun()

                    delete_pending_key = f"meeting_log_delete_pending_upcoming_{selected_upcoming_id}"
                    if st.button(
                        "Delete this meeting date",
                        key=f"meeting_log_delete_upcoming_{selected_upcoming_id}",
                        use_container_width=True,
                    ):
                        st.session_state[delete_pending_key] = True
                        st.rerun()

                    if st.session_state.get(delete_pending_key, False):
                        st.markdown(
                            "<div class='sg-action-alert'>Delete this meeting date?</div>",
                            unsafe_allow_html=True,
                        )
                        d1, d2 = st.columns(2)
                        if d1.button(
                            "Confirm delete",
                            key=f"meeting_log_confirm_delete_upcoming_{selected_upcoming_id}",
                            type="primary",
                            use_container_width=True,
                        ):
                            delete_upcoming_meeting(selected_upcoming_id)
                            st.session_state.pop(delete_pending_key, None)
                            queue_message("Meeting date deleted.")
                            st.rerun()
                        if d2.button(
                            "Cancel",
                            key=f"meeting_log_cancel_delete_upcoming_{selected_upcoming_id}",
                            use_container_width=True,
                        ):
                            st.session_state.pop(delete_pending_key, None)
                            notify("Delete canceled.", "info")
                            st.rerun()

    with families_tab:
        with st.container(border=True):
            render_section_header(
                "Small Group Families",
                "This list powers Host and Facilitator dropdowns.",
            )
            families_df = pd.DataFrame({"Family": fetch_small_group_families()})
            if families_df.empty:
                families_df = pd.DataFrame({"Family": list(DEFAULT_FAMILY_OPTIONS)})

            if "admin_edit_families" not in st.session_state:
                st.session_state["admin_edit_families"] = False

            if not st.session_state["admin_edit_families"]:
                family_cards = ['<div class="sg-simple-list">']
                for family_name in families_df["Family"].tolist():
                    family_cards.append(
                        "<div class='sg-simple-row'>"
                        f"<p class='sg-simple-row-title'>{escape(str(family_name))}</p>"
                        "<p class='sg-simple-row-meta'>Available for host and facilitator sign-up.</p>"
                        "</div>"
                    )
                family_cards.append("</div>")
                st.markdown("".join(family_cards), unsafe_allow_html=True)
                if st.button("Edit family list", use_container_width=True):
                    st.session_state["admin_edit_families"] = True
                    st.rerun()
            else:
                st.info("Editing mode is enabled. Add/remove rows, then save or cancel.")
                with st.form("admin_edit_families_form"):
                    family_editor_df = st.data_editor(
                        families_df,
                        hide_index=True,
                        use_container_width=True,
                        num_rows="dynamic",
                        column_order=["Family"],
                        column_config={
                            "Family": st.column_config.TextColumn("Family", width="large"),
                        },
                        key="admin_small_group_families_editor",
                    )
                    c1, c2 = st.columns(2)
                    save_family_changes = c1.form_submit_button(
                        "Save family list",
                        type="primary",
                        use_container_width=True,
                    )
                    cancel_family_changes = c2.form_submit_button(
                        "Cancel",
                        use_container_width=True,
                    )

                if save_family_changes:
                    saved_count = save_small_group_families(
                        [str(value) for value in family_editor_df["Family"].tolist()]
                    )
                    if saved_count == 0:
                        notify("Add at least one family name before saving.", "warning")
                    else:
                        st.session_state["admin_edit_families"] = False
                        queue_message(f"Saved {saved_count} small group families.")
                        st.rerun()

                if cancel_family_changes:
                    st.session_state["admin_edit_families"] = False
                    notify("Family edit canceled.", "info")
                    st.rerun()

    with history_tab:
        with st.container(border=True):
            render_section_header(
                "Logged Meetings",
                "Review historical records, adjust status/notes, or remove an incorrect entry.",
            )
            log_df = fetch_meeting_log(lessons_df)
            if log_df.empty:
                render_empty_state(
                    "No meetings logged yet",
                    "Completed meetings will appear here once they are logged.",
                )
            else:
                edited_df = st.data_editor(
                    log_df,
                    hide_index=True,
                    use_container_width=True,
                    disabled=["id", "meeting_date", "lesson_week", "lesson_theme"],
                    column_order=[
                        "id",
                        "meeting_date",
                        "lesson_week",
                        "lesson_theme",
                        "status",
                        "host_name",
                        "facilitator_name",
                        "notes",
                    ],
                    column_config={
                        "id": st.column_config.NumberColumn("ID", width="small"),
                        "meeting_date": st.column_config.TextColumn("Meeting date", width="small"),
                        "lesson_week": st.column_config.NumberColumn("Lesson week", width="small"),
                        "lesson_theme": st.column_config.TextColumn("Lesson theme", width="medium"),
                        "status": st.column_config.SelectboxColumn(
                            "Status",
                            options=MEETING_STATUS_OPTIONS,
                            required=True,
                            width="small",
                        ),
                        "host_name": st.column_config.TextColumn("Host", width="small"),
                        "facilitator_name": st.column_config.TextColumn("Facilitator", width="small"),
                        "notes": st.column_config.TextColumn("Notes", width="large"),
                    },
                    key="meeting_log_editor",
                )

                if st.button(
                    "Save meeting log edits",
                    type="primary",
                    use_container_width=True,
                ):
                    original = log_df.set_index("id")
                    changes = 0
                    for row in edited_df.to_dict(orient="records"):
                        record_id = int(row["id"])
                        new_status = str(row.get("status", "Skipped"))
                        if new_status not in MEETING_STATUS_OPTIONS:
                            new_status = "Skipped"

                        new_notes = row.get("notes", "")
                        new_notes = "" if pd.isna(new_notes) else str(new_notes)
                        new_host = row.get("host_name", "")
                        new_host = "" if pd.isna(new_host) else str(new_host)
                        new_facilitator = row.get("facilitator_name", "")
                        new_facilitator = "" if pd.isna(new_facilitator) else str(new_facilitator)

                        old_row = original.loc[record_id]
                        old_notes = old_row["notes"] if not pd.isna(old_row["notes"]) else ""
                        old_host = old_row["host_name"] if not pd.isna(old_row["host_name"]) else ""
                        old_facilitator = (
                            old_row["facilitator_name"]
                            if not pd.isna(old_row["facilitator_name"])
                            else ""
                        )

                        if (
                            new_status != old_row["status"]
                            or str(new_notes) != str(old_notes)
                            or str(new_host) != str(old_host)
                            or str(new_facilitator) != str(old_facilitator)
                        ):
                            update_meeting_record(
                                record_id,
                                new_status,
                                new_notes,
                                new_host,
                                new_facilitator,
                            )
                            changes += 1

                    if changes:
                        queue_message(f"Saved {changes} meeting log update(s).")
                        st.rerun()
                    else:
                        notify("No changes to save.", "info")

                render_section_header("Delete Record")
                delete_options = {}
                for row in log_df.to_dict(orient="records"):
                    label = (
                        f"#{row['id']} | {row['meeting_date']} | Week {row['lesson_week']} - "
                        f"{row['lesson_theme']} | {row['status']}"
                    )
                    delete_options[label] = int(row["id"])

                selected_label = st.selectbox("Select record", options=list(delete_options.keys()))
                confirm_delete = st.checkbox("I confirm this delete cannot be undone.")
                if st.button("Delete selected record", use_container_width=True):
                    if not confirm_delete:
                        notify("Please confirm deletion before continuing.", "warning")
                    else:
                        delete_meeting_record(delete_options[selected_label])
                        queue_message("Meeting record deleted.")
                        st.rerun()


def render_settings_page(source_msg: str, lessons_count: int) -> None:
    render_page_header(
        "Settings",
        "Take care of backups and local app data for this device.",
        "System",
    )

    with st.container(border=True):
        render_section_header("Lesson Data")
        st.caption(source_msg)
        st.caption(f"Lessons loaded: {lessons_count}")

    with st.container(border=True):
        render_section_header(
            "Export Backup",
            "Download a full JSON backup of local application data.",
        )
        backup_payload = export_backup_data()
        backup_json = json.dumps(backup_payload, indent=2)
        st.download_button(
            "Export backup JSON",
            data=backup_json,
            file_name=f"smallgroup_backup_{date.today().isoformat()}.json",
            mime="application/json",
            use_container_width=True,
        )

    with st.container(border=True):
        render_section_header(
            "Import Backup",
            "Restore data from a previously exported JSON backup.",
        )
        uploaded_file = st.file_uploader("Upload backup JSON", type=["json"])
        import_confirm = st.checkbox("Replace existing local data with imported backup.")

        if st.button(
            "Import backup",
            disabled=uploaded_file is None,
            type="primary",
            use_container_width=True,
        ):
            if uploaded_file is None:
                notify("Upload a backup JSON file first.", "warning")
            elif not import_confirm:
                notify("Confirm replacement before importing.", "warning")
            else:
                try:
                    payload = json.load(uploaded_file)
                    import_backup_data(payload)
                except Exception as exc:
                    notify(f"Import failed: {exc}", "error")
                else:
                    queue_message("Backup imported successfully.")
                    st.rerun()

    with st.container(border=True):
        render_section_header(
            "Reset Database",
            "Clear all local data and return to default setup.",
        )
        reset_confirm = st.checkbox(
            "I understand this will erase all meeting logs, upcoming dates, meal signups, families, notes, NIV verse text, and custom questions."
        )
        if st.button("Reset database", use_container_width=True):
            if not reset_confirm:
                notify("Confirm reset before continuing.", "warning")
            else:
                reset_database_data()
                queue_message("Database reset complete.")
                st.rerun()


def main() -> None:
    st.set_page_config(
        page_title="Small-Group Devotional Manager",
        page_icon=":book:",
        layout="wide",
    )
    inject_global_styles()

    init_db()

    try:
        lessons_df, source_msg, warning_msg = load_lessons()
    except Exception as exc:
        st.error(f"Could not load lesson data: {exc}")
        st.stop()

    lessons_count = lessons_df["week"].nunique()
    if lessons_count == 0:
        st.error("No valid lessons were found in the lesson data file.")
        st.stop()

    pages = ["Home", "Lessons", "Admin", "Settings"]
    if "active_page" not in st.session_state or st.session_state["active_page"] not in pages:
        st.session_state["active_page"] = "Home"
    lesson_pick_param = get_query_param_int("lesson_pick")
    if lesson_pick_param is not None:
        st.session_state["active_page"] = "Lessons"

    page = str(st.session_state.get("active_page", "Home"))

    show_queued_message()
    render_inline_navigation(pages)

    if warning_msg:
        st.warning(warning_msg)

    if lessons_count < TOTAL_LESSONS:
        st.warning(
            f"Loaded {lessons_count} lesson(s). Progress still targets {TOTAL_LESSONS} total lessons."
        )

    if page == "Home":
        render_dashboard(lessons_df)
    elif page == "Lessons":
        render_lessons_page(lessons_df)
    elif page == "Admin":
        render_meeting_log_page(lessons_df)
    else:
        render_settings_page(source_msg, lessons_count)


if __name__ == "__main__":
    main()
