PKG:=aiosqlite

install:
	uv sync --locked --all-extras

.venv:
	uv venv .venv

venv: .venv
	source .venv/bin/activate && make install
	echo 'run `source .venv/bin/activate` to activate virtualenv'

test:
	uv run coverage run -m tests
	uv run coverage report
	uv run mypy -p $(PKG)

lint:
	uv run ruff check $(PKG)
	uv run format --check $(PKG)

format:
	uv run format $(PKG)

perf:
	uv run python -m unittest -v tests.perf
