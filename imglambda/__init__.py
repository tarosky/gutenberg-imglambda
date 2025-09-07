from pathlib import Path


def get_version() -> str:
  return Path(__file__).parent.resolve().with_name('VERSION').read_text().strip()


version = get_version()
