import os


def pytest_configure(config):
    """Set APP_ENV=test before any imports so config.py loads .env.test."""
    os.environ.setdefault('APP_ENV', 'test')
