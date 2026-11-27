PKG:=aiosqlite
EXTRAS:=dev

UV:=$(shell uv --version)
VENV:=uv venv
PIP:=uv pip

install:
	$(PIP) install -Ue .[$(EXTRAS)]

.venv:
	$(VENV) .venv

venv: .venv
	source .venv/bin/activate && make install
	echo 'run `source .venv/bin/activate` to activate virtualenv'

test:
	python -m coverage run -m $(PKG).tests
	python -m coverage report
	python -m mypy -p $(PKG)

lint:
	uv run ruff check $(PKG)
	uv run format --check $(PKG)

format:
	uv run format $(PKG)

perf:
	python -m unittest -v $(PKG).tests.perf
