import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://orion_user:orion_password@localhost:5435/orion_db"
)