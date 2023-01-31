.ONESHELL:

SHELL = /bin/sh
CONDA_ACTIVATE = source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate

## Eight different machines are defined below,(2 windows										
## and 6 Unix). Use the machine variable to build your process							
## differently by machine type.																							
# adapted (slightly) from
# https://gist.github.com/trosendal/d4646812a43920bfe94e

ifeq ($(OS),Windows_NT)
		MACHINE = win
		ifeq ($(PROCESSOR_ARCHITECTURE),AMD64)
				MACHINE := $(MACHINE)-amd64
		endif
		ifeq ($(PROCESSOR_ARCHITECTURE),x86)
				# FIXME we are assuming 64!
				MACHINE := $(MACHINE)-ia64
		endif
else
		UNAME_S := $(shell uname -s)
		ifeq ($(UNAME_S),Linux)
				# FIXME we are assuming 64!
				MACHINE = linux64
		endif
		ifeq ($(UNAME_S),Darwin)
				MACHINE = osx
		endif
		UNAME_P := $(shell uname -p)
		ifeq ($(UNAME_P),x86_64)
				MACHINE := $(MACHINE)-amd64
		endif
		ifneq ($(filter %86,$(UNAME_P)),)
				MACHINE := $(MACHINE)-ia32
		endif
		ifneq ($(filter arm%,$(UNAME_P)),)
				# FIXME we are assuming 64!
				MACHINE := $(MACHINE)-arm64
		endif
endif

CONDA_LOCK_FILE = conda_platform_locks/conda-$(MACHINE).lock

# Create conda env from env.yml and install poetry packages
pythonenv:
		conda create --name foresight --file $(CONDA_LOCK_FILE)
		$(CONDA_ACTIVATE) foresight & $$(conda info --base)/envs/foresight/bin/poetry install
