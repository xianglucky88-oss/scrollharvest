PY=python3
run:
	PYTHONPATH=src $(PY) -m scrollharvest.cli ingest
demo:
	PYTHONPATH=src $(PY) -m scrollharvest.cli harvest
test:
	PYTHONPATH=src $(PY) -m pytest -q
clean:
	rm -rf .pytest_cache **/__pycache__ *.db
