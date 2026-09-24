.PHONY: all install add add-dev run test lint pre-commit clean
# Check that uv is available
UV := $(shell command -v uv 2> /dev/null)

check_uv:
ifndef UV
	$(error "uv is not installed")
endif

install: check_uv
	uv sync

add: check_uv
	@test -n "$(lib)" || (echo "Usage: make add lib=pandas" && exit 1)
	uv add $(lib)

add-dev: check_uv
	@test -n "$(lib)" || (echo "Usage: make add-dev lib=pytest" && exit 1)
	uv add --dev $(lib)

run: check_uv
	uv run bluesky-streamhouse

test: check_uv
	uv run pytest tests/

lint: check_uv
	uv run ruff check --fix .

pre-commit:
	uv run pre-commit install

clean:
	rm -rf .venv
	rm -rf build dist
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache
