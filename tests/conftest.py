import sys
from pathlib import Path

# Добавляем корень проекта в sys.path, чтобы тесты могли импортировать app и analyzers
sys.path.insert(0, str(Path(__file__).parent.parent))
