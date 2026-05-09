install:
	pip install -r requirements.txt

ingest:
	python cli.py ingest --input data/raw/matches.csv

standings:
	python cli.py query --stat standings

top:
	python cli.py query --stat top --n 5

high:
	python cli.py query --stat high --min-goals 4

referee:
	python cli.py query --stat referee

clean:
	rm -rf data/lake/ __pycache__ pipeline/__pycache__

.PHONY: install ingest standings top high referee clean
