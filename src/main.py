"""Application entry point.

Loads environment variables, configures logging, and runs the main logic.
"""
import os
from dotenv import load_dotenv
from loguru import logger


def main():
    """Run the application.

    Steps:
    1. Load environment variables from .env.
    2. Configure structured logging.
    3. Execute business logic.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Read runtime environment (defaults to "Local")
    env = os.getenv("ENV_NAME", "Local")

    # Structured log — prefer this over print in production
    logger.info(f"Starting bluesky-streamhouse in {env} mode...")

    # Your code here
    print("Hello from the High-Level Template!")


if __name__ == "__main__":
    main()
