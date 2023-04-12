BASE_DIR = $(CURDIR)

VENV = . $(BASE_DIR)/.venv/bin/activate; cd src;

FMT = printf "\033[34m%-20s\033[0m %s\n"
RGX = /^[0-9a-zA-Z_-]+:.*?\#/

help :: # Show this message
	@awk '{FS=": #"} $(RGX) {$(FMT),$$1,$$2}' $(MAKEFILE_LIST)

pip: # Install python dependencies
	$(VENV) pip install -r $(BASE_DIR)/requirements.txt --upgrade

run: # Run Tradis market_data
	$(VENV) python market_data.py

history: # Run Tradis market_data in history mode
	$(VENV) python market_data.py --history
