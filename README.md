# Olympia Format

![Olympia Logo](https://github.com/olympiaformat/olympia/blob/main/docs/logo/wide.png?raw=true)


Olympia is a storage-only open catalog format for big data analytics, ML & AI.
See [olympiaformat.org](https://olympiaformat.org) for full documentation.

# Development Setup

## Website

The website is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).
The easiest way to setup is to create a Python virtual environment
and install the necessary dependencies:

```bash
python3 -m venv .env
source .env/bin/activate
pip install mkdocs-material
pip install mkdocs-awesome-pages-plugin
```

Then you can start the server at `http://localhost:8000` by:

```bash
source .env/bin/activate
mkdocs serve
```
