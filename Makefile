.PHONY: help

help: ## Print this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the project.
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
	python -m poetry install

install: ## Install the project.
	pip install git+ssh://git@github.com/beedata-analytics/beemeteo.git

sample-meteogalicia: ## Sample meteogalicia.
	python -m beemeteo --name meteogalicia --filename config.json --latitude 41.29 --longitude 2.19 --timezone Europe/Madrid --date-from 2021-01-01 --date-to 2021-01-02 --hbase-table meteo_meteogalicia

sample-cams: ## Sample cams.
	python -m beemeteo --name cams --filename config.json --latitude 41.29 --longitude 2.19 --timezone Europe/Madrid --date-from 2021-01-01 --date-to 2021-01-02 --hbase-table meteo_cams

sample-darksky: ## Sample darksky.
	python -m beemeteo --name darksky --filename config.json --latitude 41.29 --longitude 2.19 --timezone Europe/Madrid --date-from 2021-01-01 --date-to 2021-01-02 --hbase-table meteo_darksky

test: ## Test the project.
	python -m poetry run python -m pytest -v tests
