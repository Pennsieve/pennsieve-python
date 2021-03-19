# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS  =
SPHINXBUILD = python -msphinx -W
SPHINXPROJ  = Pennsieve
SOURCEDIR   = .
BUILDDIR    = _build
PS_VERSION  = $(shell python -c 'from __future__ import print_function; import pennsieve; print(pennsieve.__version__)')

# default
.PHONY: clean html
.DEFAULT_GOAL := all

install:
	pip install -r requirements.txt

html:
	$(SPHINXBUILD) $(SOURCEDIR) -b html "$(BUILDDIR)/html/$(PS_VERSION)"

html-latest:
	rm -rf $(BUILDDIR)/html/latest; cp -r $(BUILDDIR)/html/$(PS_VERSION) $(BUILDDIR)/html/latest

clean:
	rm -rf $(BUILDDIR)/*

all: clean html
