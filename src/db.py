"""
SQLite database layer for activities and signups.
"""

import sqlite3
from typing import List, Dict, Optional, Tuple
from pathlib import Path

DB_PATH = Path(__file__).parent / "activities.db"


def init_db() -> None:
    """Initialize the database schema."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        name TEXT PRIMARY KEY,
        description TEXT,
        schedule TEXT,
        max_participants INTEGER
    )
    """)
    
    c.execute("""
    CREATE TABLE IF NOT EXISTS signups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        activity_name TEXT,
        email TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(activity_name) REFERENCES activities(name),
        UNIQUE(activity_name, email)
    )
    """)
    
    conn.commit()
    conn.close()


def seed_activities(activities_data: Dict) -> None:
    """Seed the database with initial activities if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    for name, activity in activities_data.items():
        c.execute(
            "INSERT OR IGNORE INTO activities (name, description, schedule, max_participants) VALUES (?, ?, ?, ?)",
            (name, activity["description"], activity["schedule"], activity["max_participants"])
        )
        
        # Seed existing participants from in-memory data
        for email in activity.get("participants", []):
            c.execute(
                "INSERT OR IGNORE INTO signups (activity_name, email) VALUES (?, ?)",
                (name, email)
            )
    
    conn.commit()
    conn.close()


def get_activities() -> Dict:
    """Retrieve all activities with participant counts."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT name, description, schedule, max_participants FROM activities")
    rows = c.fetchall()
    
    activities = {}
    for name, description, schedule, max_participants in rows:
        c.execute("SELECT email FROM signups WHERE activity_name = ?", (name,))
        participants = [row[0] for row in c.fetchall()]
        
        activities[name] = {
            "description": description,
            "schedule": schedule,
            "max_participants": max_participants,
            "participants": participants
        }
    
    conn.close()
    return activities


def signup_student(activity_name: str, email: str) -> Tuple[bool, Optional[str]]:
    """Sign up a student for an activity. Returns (success, error_message)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Check if activity exists
    c.execute("SELECT max_participants FROM activities WHERE name = ?", (activity_name,))
    result = c.fetchone()
    
    if not result:
        conn.close()
        return False, "Activity not found"
    
    max_participants = result[0]
    
    # Check if student is already signed up
    c.execute("SELECT COUNT(*) FROM signups WHERE activity_name = ? AND email = ?", (activity_name, email))
    if c.fetchone()[0] > 0:
        conn.close()
        return False, "Student is already signed up"
    
    # Check if activity is full
    c.execute("SELECT COUNT(*) FROM signups WHERE activity_name = ?", (activity_name,))
    current_count = c.fetchone()[0]
    
    if max_participants > 0 and current_count >= max_participants:
        conn.close()
        return False, "Activity is full"
    
    # Add signup
    try:
        c.execute("INSERT INTO signups (activity_name, email) VALUES (?, ?)", (activity_name, email))
        conn.commit()
        conn.close()
        return True, None
    except sqlite3.IntegrityError as e:
        conn.close()
        return False, str(e)


def unregister_student(activity_name: str, email: str) -> Tuple[bool, Optional[str]]:
    """Unregister a student from an activity. Returns (success, error_message)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Check if activity exists
    c.execute("SELECT name FROM activities WHERE name = ?", (activity_name,))
    if not c.fetchone():
        conn.close()
        return False, "Activity not found"
    
    # Check if student is signed up
    c.execute("SELECT id FROM signups WHERE activity_name = ? AND email = ?", (activity_name, email))
    if not c.fetchone():
        conn.close()
        return False, "Student is not signed up for this activity"
    
    # Remove signup
    c.execute("DELETE FROM signups WHERE activity_name = ? AND email = ?", (activity_name, email))
    conn.commit()
    conn.close()
    return True, None
