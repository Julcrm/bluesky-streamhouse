# bluesky-streamhouse

streamhouse

## Quick start

```bash
make install      # install dependencies
make pre-commit   # install pre-commit hooks (run once after cloning)
make run          # run the application
make test         # run tests
make lint         # lint and auto-fix with ruff
make clean        # remove virtualenv, caches and build artifacts
```

## uv cheatsheet

| Command | Description |
|---|---|
| `uv sync` | Install all dependencies from lockfile |
| `uv sync --no-dev` | Install without dev dependencies |
| `uv add <package>` | Add a dependency — updates pyproject.toml and uv.lock automatically |
| `uv add --dev <package>` | Add a dev dependency |
| `uv remove <package>` | Remove a dependency |
| `uv lock --check` | Verify lockfile is up to date with pyproject.toml |
| `uv run <command>` | Run a command inside the virtual environment |
| `uv python install <version>` | Install a specific Python version |

---

Author: Julien
