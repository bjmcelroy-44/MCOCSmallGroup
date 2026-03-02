from __future__ import annotations

import json
import sqlite3
from calendar import month_name, monthcalendar
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

STATUS_COLORS = {
    "Done": ("#dbe8f2", "#3f5f87"),
    "Not done": ("#e8eef4", "#2f394d"),
    "Skipped": ("#fde5db", "#a6533c"),
    "Postponed": ("#dbe8ea", "#40606d"),
}


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
            --sg-blue-dark: #45658c;
            --sg-blue-mid: #8eb6cd;
            --sg-blue-light: #bdd9df;
            --sg-orange: #e96b47;
            --sg-navy: #2f394d;
            --sg-bg: #e8eef4;
            --sg-surface: #f7f9fb;
            --sg-surface-soft: #edf3f7;
            --sg-border: #b4c4d2;
            --sg-border-strong: #95abc0;
            --sg-text: #273447;
            --sg-muted: #596d88;
        }
        .stApp {
            background: var(--sg-bg);
            color: var(--sg-text);
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
        }
        section[data-testid="stSidebar"] {
            background: #dce8f1 !important;
            border-right: 1px solid var(--sg-border);
        }
        section[data-testid="stSidebar"] div[data-testid="stSidebarContent"] {
            background: #dce8f1 !important;
            color: var(--sg-navy);
        }
        .main .block-container {
            max-width: 1260px;
            padding-top: 0.9rem;
            padding-bottom: 1.8rem;
        }
        h1, h2, h3 {
            color: var(--sg-navy);
            letter-spacing: 0.01em;
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
            font-weight: 700;
        }
        h1 { font-size: 1.85rem; }
        h2 { font-size: 1.45rem; }
        h3 { font-size: 1.12rem; }
        .stMarkdown p {
            margin-bottom: 0.45rem;
        }
        hr {
            border-color: var(--sg-border);
            margin: 0.45rem 0 0.85rem 0;
        }
        div[data-testid="stVerticalBlockBorderWrapper"] {
            border: 1px solid var(--sg-border);
            border-radius: 14px;
            background: var(--sg-surface);
            box-shadow: 0 1px 0 rgba(47, 57, 77, 0.06);
        }
        div[data-testid="stMetric"] {
            background: var(--sg-surface-soft);
            border: 1px solid var(--sg-border);
            border-radius: 12px;
            padding: 0.4rem 0.65rem;
        }
        div[data-testid="stMetricLabel"] {
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--sg-muted);
            font-weight: 700;
        }
        div[data-testid="stMetricValue"] {
            color: var(--sg-blue-dark);
            font-weight: 700;
        }
        div[data-testid="stDataEditor"] {
            border: 1px solid var(--sg-border);
            border-radius: 12px;
            padding: 0.2rem;
            background: var(--sg-surface);
        }
        div[data-testid="stDateInput"] label,
        div[data-testid="stSelectbox"] label,
        div[data-testid="stTextInput"] label,
        div[data-testid="stTextArea"] label,
        div[data-testid="stRadio"] label {
            font-size: 0.74rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted);
            font-weight: 700;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div,
        div[data-baseweb="textarea"] > div {
            background: var(--sg-surface) !important;
            border: 1px solid var(--sg-border) !important;
            border-radius: 10px !important;
            box-shadow: none !important;
        }
        div[data-baseweb="select"] > div:hover,
        div[data-baseweb="input"] > div:hover,
        div[data-baseweb="textarea"] > div:hover {
            border-color: var(--sg-border-strong) !important;
        }
        div[data-baseweb="select"] > div:focus-within,
        div[data-baseweb="input"] > div:focus-within,
        div[data-baseweb="textarea"] > div:focus-within {
            border-color: var(--sg-blue-dark) !important;
            box-shadow: 0 0 0 1px var(--sg-blue-dark) !important;
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
            background: var(--sg-blue-light) !important;
        }
        div[data-baseweb="popover"] [role="option"]:hover {
            background: #dce8f1 !important;
        }
        .stButton > button, .stDownloadButton > button, .stFormSubmitButton > button {
            border-radius: 10px;
            border: 1px solid var(--sg-border);
            padding: 0.34rem 0.62rem;
            font-weight: 600;
            background: var(--sg-surface-soft);
            color: var(--sg-text);
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
        }
        .stButton > button[kind="primary"], .stFormSubmitButton > button[kind="primary"] {
            background: var(--sg-blue-dark) !important;
            border-color: var(--sg-blue-dark) !important;
            color: #ffffff !important;
        }
        .stButton > button[kind="primary"]:hover, .stFormSubmitButton > button[kind="primary"]:hover {
            background: var(--sg-navy) !important;
            border-color: var(--sg-navy) !important;
            color: #ffffff !important;
        }
        .stButton > button:hover, .stFormSubmitButton > button:hover {
            border-color: var(--sg-blue-dark);
            color: var(--sg-navy);
        }
        .stCaption {
            color: var(--sg-muted);
            font-size: 0.78rem;
        }
        .sg-lesson-rolodex-scroll {
            display: flex;
            gap: 0.48rem;
            overflow-x: auto;
            overflow-y: hidden;
            padding: 0.16rem 0.1rem 0.45rem 0.05rem;
            scrollbar-width: thin;
        }
        .sg-lesson-rolodex-scroll::-webkit-scrollbar {
            height: 8px;
        }
        .sg-lesson-rolodex-scroll::-webkit-scrollbar-thumb {
            background: var(--sg-blue-mid);
            border-radius: 999px;
        }
        .sg-lesson-card {
            min-width: 148px;
            max-width: 166px;
            border: 1px solid var(--sg-border);
            border-radius: 11px;
            background: var(--sg-surface-soft);
            color: var(--sg-text) !important;
            text-decoration: none !important;
            padding: 0.42rem 0.52rem;
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.28);
            flex: 0 0 auto;
        }
        .sg-lesson-card:hover {
            border-color: var(--sg-border-strong);
            background: #e2ebf2;
        }
        .sg-lesson-card.selected {
            background: var(--sg-navy);
            border-color: var(--sg-navy);
            color: #f2f6fb !important;
        }
        .sg-lesson-card-title {
            font-size: 0.82rem;
            font-weight: 700;
            line-height: 1.2;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .sg-lesson-card-status {
            font-size: 0.7rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-top: 0.28rem;
            opacity: 0.82;
        }
        .sg-save-required {
            background: #fde6dc;
            border: 1px solid #e1a793;
            border-radius: 10px;
            color: #7a3f2d;
            font-size: 0.82rem;
            font-weight: 700;
            padding: 0.42rem 0.58rem;
            margin: 0.15rem 0 0.52rem 0;
        }
        .sg-action-alert {
            background: #fde6dc;
            border: 1px solid #e1a793;
            border-radius: 10px;
            color: #7a3f2d;
            font-size: 0.8rem;
            font-weight: 700;
            padding: 0.34rem 0.52rem;
            margin: 0.2rem 0 0.45rem 0;
        }
        .sg-inline-nav-title {
            font-size: 0.73rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted);
            font-weight: 700;
            margin: 0 0 0.22rem 0;
        }
        .sg-lesson-status-line {
            margin: -0.18rem 0 0.55rem 0;
        }
        .sg-lesson-title {
            font-size: 1.26rem;
            font-weight: 700;
            line-height: 1.14;
            color: var(--sg-navy);
            margin: 0.04rem 0 0.2rem 0;
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
        }
        .sg-lesson-section {
            border: 1px solid var(--sg-border);
            border-radius: 10px;
            background: var(--sg-surface);
            padding: 0.44rem 0.58rem;
            margin: 0.28rem 0;
        }
        .sg-lesson-section-title {
            font-size: 0.71rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted);
            font-weight: 700;
            margin: 0 0 0.18rem 0;
        }
        .sg-lesson-subsection-title {
            font-size: 0.74rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--sg-muted);
            font-weight: 700;
            margin: 0.42rem 0 0.2rem 0;
        }
        .sg-lesson-summary {
            color: var(--sg-text);
            font-size: 1rem;
            line-height: 1.35;
            margin-bottom: 0.14rem;
        }
        .sg-lesson-big-idea {
            color: var(--sg-text);
            font-size: 1rem;
            line-height: 1.35;
            margin-bottom: 0.02rem;
        }
        .sg-anchor-ref {
            font-size: 0.99rem;
            color: var(--sg-navy);
            margin-bottom: 0.05rem;
        }
        .sg-tight-link {
            font-size: 0.77rem;
            margin: 0.02rem 0 0.05rem 0;
        }
        .sg-tight-link a {
            color: var(--sg-blue-dark);
            text-decoration: underline;
        }
        .sg-outline {
            line-height: 1.31;
            color: var(--sg-text);
            font-size: 0.9rem;
        }
        .sg-outline ul {
            margin: 0.04rem 0 0.2rem 0.9rem;
            padding-left: 0.5rem;
        }
        .sg-outline li {
            margin-bottom: 0.1rem;
        }
        .sg-outline-level {
            font-weight: 600;
            color: var(--sg-navy);
        }
        div[data-testid="stRadio"] div[role="radiogroup"] {
            gap: 0.5rem 0.85rem;
            flex-wrap: wrap;
        }
        @media (max-width: 960px) {
            .main .block-container {
                max-width: 100%;
                padding-top: 0.55rem;
                padding-left: 0.6rem;
                padding-right: 0.6rem;
                padding-bottom: 1.25rem;
            }
            h1 { font-size: 1.45rem; }
            h2 { font-size: 1.22rem; }
            h3 { font-size: 1.0rem; }
            .stMarkdown p { margin-bottom: 0.36rem; }
            div[data-testid="stVerticalBlockBorderWrapper"] {
                border-radius: 11px;
            }
            div[data-testid="stHorizontalBlock"] {
                gap: 0.48rem !important;
                flex-wrap: wrap;
            }
            div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
                flex: 1 1 260px !important;
                width: 100% !important;
                min-width: 0 !important;
            }
            .stButton > button, .stDownloadButton > button, .stFormSubmitButton > button {
                min-height: 2.2rem;
                font-size: 0.92rem;
                padding: 0.36rem 0.58rem;
            }
            .stCaption {
                font-size: 0.74rem;
            }
            .sg-lesson-rolodex-scroll {
                gap: 0.36rem;
                padding: 0.08rem 0.05rem 0.34rem 0.03rem;
            }
            .sg-lesson-card {
                min-width: 130px;
                max-width: 145px;
                padding: 0.34rem 0.4rem;
            }
            .sg-lesson-card-title {
                font-size: 0.76rem;
            }
            .sg-lesson-card-status {
                font-size: 0.62rem;
                margin-top: 0.2rem;
            }
            .sg-save-required {
                font-size: 0.77rem;
            }
            .sg-lesson-title {
                font-size: 1.12rem;
            }
            .sg-lesson-section {
                padding: 0.38rem 0.48rem;
                margin: 0.22rem 0;
            }
            .sg-lesson-section-title,
            .sg-lesson-subsection-title {
                font-size: 0.67rem;
            }
            .sg-lesson-summary,
            .sg-lesson-big-idea {
                font-size: 0.94rem;
            }
            .sg-tight-link {
                font-size: 0.75rem;
            }
            .sg-outline {
                font-size: 0.88rem;
            }
            section[data-testid="stSidebar"] .stButton > button {
                min-height: 2.05rem;
            }
        }
        @media (max-width: 640px) {
            .main .block-container {
                padding-left: 0.45rem;
                padding-right: 0.45rem;
                padding-bottom: 1rem;
            }
            h1 { font-size: 1.3rem; }
            h2 { font-size: 1.12rem; }
            h3 { font-size: 0.95rem; }
            div[data-testid="stMetricLabel"] {
                font-size: 0.66rem;
            }
            div[data-testid="stMetricValue"] {
                font-size: 1.35rem;
            }
            div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
                flex: 1 1 100% !important;
            }
            .sg-lesson-card {
                min-width: 118px;
                max-width: 132px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def inject_title_font_styles() -> None:
    font_stack = '"Helvetica Neue", Helvetica, Arial, sans-serif'

    st.markdown(
        (
            "<style>"
            f":root {{ --sg-title-font: {font_stack}; }}"
            "h1, h2, h3, .sg-lesson-title { "
            "font-family: var(--sg-title-font) !important; "
            "font-weight: 700 !important; "
            "letter-spacing: 0.012em; "
            "}"
            "</style>"
        ),
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
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO upcoming_meetings (
                meeting_date, lesson_week, host_name, facilitator_name, notes
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                meeting_date.isoformat(),
                int(lesson_week),
                host_name.strip(),
                facilitator_name.strip(),
                notes.strip(),
            ),
        )


def update_upcoming_meeting(
    record_id: int,
    lesson_week: int,
    host_name: str,
    facilitator_name: str,
    notes: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE upcoming_meetings
            SET lesson_week = ?, host_name = ?, facilitator_name = ?, notes = ?
            WHERE id = ?
            """,
            (
                int(lesson_week),
                host_name.strip(),
                facilitator_name.strip(),
                notes.strip(),
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
            SELECT id, meeting_date, lesson_week, host_name, facilitator_name, notes
            FROM upcoming_meetings
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
            ]
        )

    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
    upcoming_df["lesson_theme"] = (
        upcoming_df["lesson_week"].map(theme_lookup).fillna("(Unknown lesson)")
    )

    for col in ["host_name", "facilitator_name", "notes"]:
        upcoming_df[col] = upcoming_df[col].fillna("")

    return upcoming_df[
        [
            "id",
            "meeting_date",
            "lesson_week",
            "lesson_theme",
            "host_name",
            "facilitator_name",
            "notes",
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


def render_upcoming_calendar(upcoming_df: pd.DataFrame) -> None:
    if upcoming_df.empty:
        st.info("No upcoming dates yet.")
        return

    parsed_dates = pd.to_datetime(upcoming_df["meeting_date"], errors="coerce").dropna()
    if parsed_dates.empty:
        st.info("No upcoming dates yet.")
        return

    upcoming_days = set(parsed_dates.dt.date.tolist())
    first_date = parsed_dates.min().date().replace(day=1)
    last_date = parsed_dates.max().date().replace(day=1)

    year_month_pairs: List[Tuple[int, int]] = []
    current_year, current_month = first_date.year, first_date.month
    while (current_year, current_month) <= (last_date.year, last_date.month):
        year_month_pairs.append((current_year, current_month))
        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    st.caption("Bold dates have at least one upcoming meeting.")
    headers = "| Mon | Tue | Wed | Thu | Fri | Sat | Sun |"
    separator = "|---|---|---|---|---|---|---|"

    for year, month in year_month_pairs:
        st.markdown(f"**{month_name[month]} {year}**")
        rows = monthcalendar(year, month)
        lines = [headers, separator]
        for row in rows:
            formatted_cells = []
            for day in row:
                if day == 0:
                    formatted_cells.append(" ")
                    continue
                cell_date = date(year, month, day)
                if cell_date in upcoming_days:
                    formatted_cells.append(f"**{day}**")
                else:
                    formatted_cells.append(str(day))
            lines.append("| " + " | ".join(formatted_cells) + " |")
        st.markdown("\n".join(lines))
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
            SELECT id, meeting_date, lesson_week, host_name, facilitator_name, notes
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
            if record_id is None:
                conn.execute(
                    """
                    INSERT INTO upcoming_meetings (
                        meeting_date, lesson_week, host_name, facilitator_name, notes
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (meeting_date, lesson_week, host_name, facilitator_name, notes),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO upcoming_meetings (
                        id, meeting_date, lesson_week, host_name, facilitator_name, notes
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(record_id),
                        meeting_date,
                        lesson_week,
                        host_name,
                        facilitator_name,
                        notes,
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
    bg, fg = STATUS_COLORS.get(status, STATUS_COLORS["Not done"])
    return (
        f"<span style='background-color:{bg};color:{fg};"
        "padding:0.2rem 0.6rem;border-radius:999px;font-weight:600;font-size:0.8rem;'>"
        f"{status}</span>"
    )


def render_inline_navigation(pages: List[str]) -> None:
    with st.container(border=True):
        st.markdown("<div class='sg-inline-nav-title'>Navigation</div>", unsafe_allow_html=True)
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
    st.header("MCOC Small Group")

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
        st.info("No upcoming meeting dates yet. Add dates from Admin.")
        if st.button("Add a new Meeting", key="dashboard_go_admin_empty", type="primary"):
            st.session_state["active_page"] = "Admin"
            st.rerun()
        return

    date_ids = upcoming_df["id"].astype(int).tolist()
    if (
        "dashboard_selected_upcoming_id" not in st.session_state
        or st.session_state["dashboard_selected_upcoming_id"] not in date_ids
    ):
        st.session_state["dashboard_selected_upcoming_id"] = date_ids[0]

    selected_id = int(st.session_state["dashboard_selected_upcoming_id"])

    with st.container(border=True):
        st.markdown("#### Active Meeting Dates")
        st.caption("Click a date to switch meeting details below.")
        date_rows = upcoming_df.to_dict(orient="records")
        card_columns = 4
        for row_idx, row in enumerate(date_rows):
            if row_idx % card_columns == 0:
                cols = st.columns(card_columns)
            row_id = int(row["id"])
            is_selected = row_id == selected_id
            with cols[row_idx % card_columns]:
                label_prefix = "Next" if row_idx == 0 else "Upcoming"
                button_label = f"{label_prefix}: {format_meeting_date(row['meeting_date'])}"
                if st.button(
                    button_label,
                    key=f"dashboard_select_date_{row_id}",
                    type="primary" if is_selected else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["dashboard_selected_upcoming_id"] = row_id
                    st.rerun()

        if st.button(
            "Add a new Meeting",
            key="dashboard_go_admin_add_meeting",
            use_container_width=True,
        ):
            st.session_state["active_page"] = "Admin"
            st.rerun()

    selected_row = upcoming_df[upcoming_df["id"] == selected_id].iloc[0]
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

    lesson_default_week = (
        int(selected_row["lesson_week"])
        if int(selected_row["lesson_week"]) in weeks
        else default_new_week
    )
    lesson_touch_key = f"dashboard_lesson_dropdown_touched_{selected_id}"
    if lesson_touch_key not in st.session_state:
        st.session_state[lesson_touch_key] = False

    def lesson_option_label(week: int) -> str:
        base = f"Lesson {week} - {theme_lookup.get(week, '')}"
        markers: List[str] = []
        if status_map.get(week) == "Done":
            markers.append("✅ done")
        if markers:
            return f"{base} ({' | '.join(markers)})"
        return base

    with st.container(border=True):
        st.markdown("#### Meeting Details")
        st.caption("Meeting date admin (add/delete) is on the Admin page.")
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
                    st.caption(f"Lesson date: {preview}")

        people_left, people_right = st.columns(2)
        with people_left:
            host_select = st.selectbox(
                "Host",
                options=dashboard_person_options,
                index=dashboard_person_options.index(host_default_selection),
                key=f"dashboard_host_select_{selected_id}",
            )
        with people_right:
            facilitator_select = st.selectbox(
                "Facilitator",
                options=dashboard_person_options,
                index=dashboard_person_options.index(facilitator_default_selection),
                key=f"dashboard_facilitator_select_{selected_id}",
            )

        selected_notes = st.text_area(
            "Notes",
            value=selected_row["notes"],
            key=f"dashboard_notes_{selected_id}",
            height=62,
        )

        original_notes = "" if pd.isna(selected_row["notes"]) else str(selected_row["notes"])
        has_unsaved_changes = (
            int(lesson_week) != int(lesson_default_week)
            or str(host_select) != str(host_default_selection)
            or str(facilitator_select) != str(facilitator_default_selection)
            or str(selected_notes) != str(original_notes)
        )

        if has_unsaved_changes:
            st.markdown(
                (
                    "<div class='sg-save-required'>"
                    "Changes are not stored automatically. Click <b>Save Meeting Details</b> to keep updates."
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

        save_date_details = st.button(
            "Save Meeting Details",
            key=f"dashboard_save_date_details_{selected_id}",
            type="primary",
            use_container_width=True,
        )

        if save_date_details:
            if not has_unsaved_changes:
                notify("No meeting detail changes to save.", "info")
            else:
                update_upcoming_meeting(
                    selected_id,
                    int(lesson_week),
                    str(host_select),
                    str(facilitator_select),
                    selected_notes,
                )
                queue_message("Meeting details saved.")
                st.rerun()

        st.markdown(f"##### Meal: Add Name and Dish for the {meal_date_label} meal.")
        meal_df = fetch_upcoming_meal_signups(selected_id)
        original_meal_rows = normalize_meal_rows(meal_df.to_dict(orient="records"))
        meal_editor_df = st.data_editor(
            meal_df,
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            height=144,
            column_order=["Name", "Dish"],
            column_config={
                "Name": st.column_config.TextColumn("Name", width="medium"),
                "Dish": st.column_config.TextColumn("Dish", width="large"),
            },
            key=f"dashboard_meal_editor_{selected_id}",
        )
        edited_meal_rows = normalize_meal_rows(meal_editor_df.to_dict(orient="records"))
        meal_has_unsaved_changes = edited_meal_rows != original_meal_rows

        if meal_has_unsaved_changes:
            st.markdown(
                (
                    "<div class='sg-save-required'>"
                    "Meal list changes are not saved yet. Click <b>Save meal list</b>."
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

        if st.button(
            "Save meal list",
            key=f"dashboard_save_meal_{selected_id}",
            use_container_width=True,
        ):
            if not meal_has_unsaved_changes:
                notify("No meal list changes to save.", "info")
            else:
                saved_entries = save_upcoming_meal_signups(
                    selected_id, meal_editor_df.to_dict(orient="records")
                )
                queue_message(f"Meal list saved ({saved_entries} entries).")
                st.rerun()

        st.markdown("##### Meeting Completion")
        completion_confirm = st.checkbox(
            "This meeting has been completed",
            key=f"dashboard_completion_confirm_{selected_id}",
        )
        if st.button(
            "Log Date Completed",
            key=f"dashboard_log_date_completed_{selected_id}",
            type="primary",
            disabled=not completion_confirm,
            use_container_width=True,
        ):
            add_meeting_log(
                date.fromisoformat(selected_meeting_date),
                int(lesson_week),
                "Completed",
                selected_notes,
                host_name=str(host_select),
                facilitator_name=str(facilitator_select),
            )
            st.session_state[f"dashboard_completion_confirm_{selected_id}"] = False
            queue_message("Meeting logged as completed.")
            st.rerun()
def render_lessons_page(lessons_df: pd.DataFrame) -> None:
    status_map = derive_status_map(lessons_df)
    weeks = lessons_df["week"].astype(int).tolist()
    theme_lookup = lessons_df.set_index("week")["theme"].to_dict()
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

    filtered_weeks = list(weeks)

    if not filtered_weeks:
        st.info("No lessons match this filter.")
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

    current_week = int(st.session_state["lessons_selected_week"])
    selected_week = current_week

    with st.container(border=True):
        st.markdown("#### Lesson Plans")
        st.caption("Scroll sideways and click a card. Dark card is selected.")
        cards_html = ['<div class="sg-lesson-rolodex-scroll">']
        for week in filtered_weeks:
            is_selected = int(week) == int(selected_week)
            theme = str(theme_lookup.get(week, "(Untitled lesson)")).strip()
            short_theme = theme if len(theme) <= 22 else theme[:19].rstrip() + "..."
            status_text = status_map.get(week, "Not done")
            if status_text == "Not done" and int(week) in scheduled_date_by_week:
                status_text = scheduled_date_by_week[int(week)]
            selected_class = " selected" if is_selected else ""
            cards_html.append(
                f'<a class="sg-lesson-card{selected_class}" href="?lesson_pick={int(week)}" target="_self">'
                f'<div class="sg-lesson-card-title">#{int(week)} {escape(short_theme)}</div>'
                f'<div class="sg-lesson-card-status">{escape(status_text)}</div>'
                "</a>"
            )
        cards_html.append("</div>")
        st.markdown("".join(cards_html), unsafe_allow_html=True)

    lesson = lessons_df[lessons_df["week"] == selected_week].iloc[0]
    lesson_status = status_map.get(selected_week, "Not done")
    verse_ref = lesson["anchor_verse"]
    verse_url = f"https://www.biblegateway.com/passage/?search={quote_plus(verse_ref)}"
    video_url = lesson["video_url"]
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

    with st.container(border=True):
        st.markdown(
            f"<div class='sg-lesson-title'>Lesson {selected_week} - {escape(lesson_theme)}</div>",
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
                f"<div class='sg-tight-link'><a href='{escape(verse_url + '&version=NIV', quote=True)}' target='_blank'>View verse</a></div>"
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
            "<div class='sg-lesson-section-title'>Quick Outline</div>"
            "<div class='sg-outline'><ul>"
            f"<li>{escape(big_idea)}</li>"
            f"<li>Video: <a href='{escape(video_url, quote=True)}' target='_blank'>{escape(str(lesson['video_name']))}</a></li>"
            f"<li>Anchor Verse: {escape(verse_ref)}</li>"
            "<li>Discussion<ul>"
            f"{''.join(discussion_blocks)}"
            "</ul></li>"
            "</ul></div>"
            "</div>"
        )

        st.markdown(outline_html, unsafe_allow_html=True)

        st.markdown("<div class='sg-lesson-subsection-title'>Facilitator notes</div>", unsafe_allow_html=True)
        with st.form(f"facilitator_notes_form_{selected_week}"):
            notes_text = st.text_area(
                "Facilitator notes",
                value=get_lesson_notes(int(selected_week)),
                height=150,
                label_visibility="collapsed",
            )
            save_notes = st.form_submit_button("Save facilitator notes")

        if save_notes:
            save_lesson_notes(int(selected_week), notes_text)
            queue_message(f"Facilitator notes saved for lesson {selected_week}.")
            st.rerun()

        st.markdown("<div class='sg-lesson-subsection-title'>Lesson completion</div>", unsafe_allow_html=True)
        if st.button("Completed", type="primary", use_container_width=True, key=f"lesson_completed_{selected_week}"):
            add_meeting_log(date.today(), int(selected_week), "Completed", "Marked completed from Lessons page")

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

                upcoming_df = fetch_upcoming_meetings(lessons_df)
                same_lesson_upcoming = upcoming_df[upcoming_df["lesson_week"] == int(selected_week)]
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
                        f"Lesson {selected_week} marked completed. Advanced an upcoming meeting to lesson {next_available_week}."
                    )
                else:
                    queue_message(f"Lesson {selected_week} marked completed.")
            else:
                st.session_state["lessons_selected_week"] = int(selected_week)
                queue_message(f"Lesson {selected_week} marked completed. All lessons are now done.")
            st.rerun()


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

    with st.container(border=True):
        st.markdown("#### Add New Meeting Date")
        st.caption("Create a new upcoming date. It will appear in Active Meeting Dates below.")
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
                st.caption("Next available lesson: all uncompleted lessons are already scheduled.")
            st.caption("Host and Facilitator default to TBD until assigned.")
            add_button_label = "Add Meeting to Active Dates"
            add_meeting_date = st.form_submit_button(add_button_label, use_container_width=True)

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
        st.markdown("#### Active Meeting Dates")
        st.caption("These dates are already created and can be edited below.")

        if upcoming_df.empty:
            st.info("No upcoming meeting dates set yet.")
        else:
            upcoming_ids = upcoming_df["id"].astype(int).tolist()
            if (
                "meeting_log_selected_upcoming_id" not in st.session_state
                or st.session_state["meeting_log_selected_upcoming_id"] not in upcoming_ids
            ):
                st.session_state["meeting_log_selected_upcoming_id"] = upcoming_ids[0]

            selected_upcoming_id = int(st.session_state["meeting_log_selected_upcoming_id"])

            st.caption("Select a date to edit lesson, host, facilitator, or notes.")
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

            st.markdown("**Edit Selected Meeting Date**")
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
                height=80,
                key=f"meeting_log_edit_upcoming_notes_{selected_upcoming_id}",
            )

            original_edit_notes = "" if pd.isna(selected_row["notes"]) else str(selected_row["notes"])
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
                        "Changes are not stored automatically. Click <b>Save selected meeting date</b> to keep updates."
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )

            if st.button("Save selected meeting date", use_container_width=True):
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
                "Delete selected meeting date",
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

    st.markdown("---")

    st.markdown("### Small Group Families")
    st.caption("Used for Host and Facilitator dropdowns. The list is locked until you click Edit.")
    families_df = pd.DataFrame({"Family": fetch_small_group_families()})
    if families_df.empty:
        families_df = pd.DataFrame({"Family": list(DEFAULT_FAMILY_OPTIONS)})

    if "admin_edit_families" not in st.session_state:
        st.session_state["admin_edit_families"] = False

    if not st.session_state["admin_edit_families"]:
        st.dataframe(families_df, hide_index=True, use_container_width=True)
        if st.button("Edit Small Group Families", use_container_width=True):
            st.session_state["admin_edit_families"] = True
            st.rerun()
    else:
        st.info("Editing mode is on. Add/remove rows, then save or cancel.")
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
            save_family_changes = c1.form_submit_button("Save Small Group Families")
            cancel_family_changes = c2.form_submit_button("Cancel")

        if save_family_changes:
            saved_count = save_small_group_families(
                [str(value) for value in family_editor_df["Family"].tolist()]
            )
            if saved_count == 0:
                notify("Add at least one family name before saving.", "warning")
            else:
                st.session_state["admin_edit_families"] = False
                queue_message(f"Saved {saved_count} Small Group Families.")
                st.rerun()

        if cancel_family_changes:
            st.session_state["admin_edit_families"] = False
            notify("Family edit canceled.", "info")
            st.rerun()

    st.markdown("---")
    st.markdown("### Logged meetings")

    log_df = fetch_meeting_log(lessons_df)
    if log_df.empty:
        st.info("No meetings logged yet.")
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

        if st.button("Save edited status/notes"):
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

        st.markdown("### Delete a meeting record")
        delete_options = {}
        for row in log_df.to_dict(orient="records"):
            label = (
                f"#{row['id']} | {row['meeting_date']} | Week {row['lesson_week']} - "
                f"{row['lesson_theme']} | {row['status']}"
            )
            delete_options[label] = int(row["id"])

        selected_label = st.selectbox("Select record", options=list(delete_options.keys()))
        confirm_delete = st.checkbox("I confirm this delete cannot be undone.")

        if st.button("Delete selected record"):
            if not confirm_delete:
                notify("Please confirm deletion before continuing.", "warning")
            else:
                delete_meeting_record(delete_options[selected_label])
                queue_message("Meeting record deleted.")
                st.rerun()

    st.markdown("---")
    c1, c2, c3 = st.columns(3)
    c1.metric("Completed", f"{completed_count}/{TOTAL_LESSONS}")
    c2.metric("Remaining", str(remaining_count))
    c3.metric("Upcoming dates", str(len(upcoming_df)))


def render_settings_page(source_msg: str, lessons_count: int) -> None:
    st.header("Settings")

    st.markdown("### Lesson data")
    st.caption(source_msg)
    st.caption(f"Lessons loaded: {lessons_count}")

    st.markdown("### Export backup")
    backup_payload = export_backup_data()
    backup_json = json.dumps(backup_payload, indent=2)

    st.download_button(
        "Export backup JSON",
        data=backup_json,
        file_name=f"smallgroup_backup_{date.today().isoformat()}.json",
        mime="application/json",
    )

    st.markdown("### Import backup")
    uploaded_file = st.file_uploader("Upload backup JSON", type=["json"])
    import_confirm = st.checkbox("Replace existing local data with imported backup.")

    if st.button("Import backup", disabled=uploaded_file is None):
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

    st.markdown("### Reset database")
    reset_confirm = st.checkbox(
        "I understand this will erase all meeting logs, upcoming dates, meal signups, Small Group Families, notes, NIV verse text, and custom questions."
    )

    if st.button("Reset database"):
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
    inject_title_font_styles()

    init_db()

    try:
        lessons_df, _, warning_msg = load_lessons()
    except Exception as exc:
        st.error(f"Could not load lesson data: {exc}")
        st.stop()

    lessons_count = lessons_df["week"].nunique()
    if lessons_count == 0:
        st.error("No valid lessons were found in the lesson data file.")
        st.stop()

    pages = ["Home", "Lessons", "Admin"]
    if "active_page" not in st.session_state or st.session_state["active_page"] not in pages:
        st.session_state["active_page"] = "Home"
    lesson_pick_param = get_query_param_int("lesson_pick")
    if lesson_pick_param is not None:
        st.session_state["active_page"] = "Lessons"

    with st.sidebar:
        st.markdown("### Navigation")
        for nav_page in pages:
            if st.button(
                nav_page,
                key=f"nav_button_{nav_page}",
                type="primary" if st.session_state["active_page"] == nav_page else "secondary",
                use_container_width=True,
            ):
                st.session_state["active_page"] = nav_page
                st.rerun()

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
    else:
        render_meeting_log_page(lessons_df)


if __name__ == "__main__":
    main()
