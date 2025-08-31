.PHONY: setup data features train simulate eval docs

setup:
	python -m pip install -U pip
	pip install -e .
	pre-commit install || true

data:
	python -m nflsim.pipelines.build_data

features:
	python -m nflsim.pipelines.build_features

train:
	python -m nflsim.pipelines.train_models

simulate:
	python -m nflsim.pipelines.simulate_week

eval:
	python -m nflsim.pipelines.evaluate

docs:
	mkdocs serve
