from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
INSTANCE_DIR.mkdir(exist_ok=True)


class Config:
    SECRET_KEY = "dev-secret-key-change-me"
    DATABASE = INSTANCE_DIR / "drone_operator.db"