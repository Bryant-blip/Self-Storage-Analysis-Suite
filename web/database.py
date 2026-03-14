"""
Database models and helpers — SQLAlchemy + SQLite (local) or PostgreSQL (production).

Set DATABASE_URL env var to switch:
  Local:  sqlite:///./storage_tools.db  (default)
  Prod:   postgresql://user:pass@host/dbname
"""

import os
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, text, inspect
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./storage_tools.db")

# Fix Render's postgres:// → postgresql:// (Render uses old format)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# ── Models ───────────────────────────────────────────────────────────────────
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    api_key_encrypted = Column(String, nullable=True)       # legacy — no longer used
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    subscription_tier = Column(String, nullable=True, default="free")   # free, pro, enterprise
    subscription_expires = Column(DateTime, nullable=True)              # null = no expiry


class UsageLog(Base):
    __tablename__ = "usage_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    action = Column(String, nullable=False)       # "comps", "quick_estimate", "accurate_estimate"
    location = Column(String, nullable=True)       # city or search location
    created_at = Column(DateTime, default=datetime.utcnow)


# ── Create tables & migrate missing columns ─────────────────────────────────
def init_db():
    Base.metadata.create_all(bind=engine)

    # Add columns that may be missing from older database schemas
    insp = inspect(engine)
    if insp.has_table("users"):
        existing = {col["name"] for col in insp.get_columns("users")}
        migrations = []
        if "is_active" not in existing:
            migrations.append("ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE")
        if "subscription_tier" not in existing:
            migrations.append("ALTER TABLE users ADD COLUMN subscription_tier VARCHAR DEFAULT 'free'")
        if "subscription_expires" not in existing:
            migrations.append("ALTER TABLE users ADD COLUMN subscription_expires TIMESTAMP")
        if "api_key_encrypted" not in existing:
            migrations.append("ALTER TABLE users ADD COLUMN api_key_encrypted VARCHAR")
        if migrations:
            with engine.begin() as conn:
                for sql in migrations:
                    conn.execute(text(sql))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
