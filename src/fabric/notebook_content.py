"""
Fabric Notebook Content Generator.

Generates .ipynb notebook definitions for each medallion layer of the
Confluence ETL pipeline, ready to be deployed to a Fabric workspace
via the Fabric REST API.

Each notebook is self-contained: it installs its own dependencies and
reads/writes data via the lakehouse mount point (/lakehouse/default/).
"""

from __future__ import annotations

import json
import base64


def _make_notebook(
    cells: list[dict],
    lakehouse_id: str,
    workspace_id: str,
    lakehouse_name: str = "confluencelakehouse",
) -> str:
    """
    Build a Fabric-compatible .ipynb notebook and return the base64-encoded
    payload string ready for the Fabric REST API.
    """
    notebook = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": cells,
        "metadata": {
            "language_info": {"name": "python"},
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": lakehouse_name,
                    "default_lakehouse_workspace_id": workspace_id,
                    "known_lakehouses": [
                        {
                            "id": lakehouse_id,
                        }
                    ],
                }
            },
        },
    }
    raw = json.dumps(notebook, indent=2)
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


def _code_cell(source: str) -> dict:
    """Build a code cell."""
    return {
        "cell_type": "code",
        "source": [source],
        "execution_count": None,
        "outputs": [],
        "metadata": {},
    }


def _markdown_cell(source: str) -> dict:
    """Build a markdown cell."""
    return {
        "cell_type": "markdown",
        "source": [source],
        "metadata": {},
    }


# ── Parameters cell (Fabric injects overrides here) ─────────────────


def _parameters_cell() -> dict:
    """Designate a parameters cell for pipeline parameterization."""
    return {
        "cell_type": "code",
        "source": [
            "# Parameters (overridden by pipeline at runtime)\n",
            'confluence_url = ""\n',
            'confluence_email = ""\n',
            'confluence_api_token = ""\n',
        ],
        "execution_count": None,
        "outputs": [],
        "metadata": {"tags": ["parameters"]},
    }


# ── Bronze Notebook ──────────────────────────────────────────────────


def bronze_notebook(lakehouse_id: str, workspace_id: str) -> str:
    """
    Generate the Bronze notebook that extracts Confluence data and writes
    raw Parquet files to the lakehouse.
    """
    cells = [
        _markdown_cell(
            "# Bronze Layer - Confluence Data Extraction\n\n"
            "Extracts all spaces, pages, and comments from Confluence Cloud\n"
            "and writes them as raw Parquet files into the Bronze zone."
        ),
        _parameters_cell(),
        _code_cell(
            "# Install Confluence dependencies\n"
            "import subprocess, sys\n"
            "subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q',\n"
            "    'atlassian-python-api>=3.41.0'])\n"
            "print('Dependencies installed.')"
        ),
        _code_cell(
            "import pandas as pd\n"
            "from datetime import datetime, timezone\n"
            "from atlassian import Confluence\n"
            "import os\n"
            "\n"
            "# ── Connect to Confluence ────────────────────────────────────────\n"
            "api = Confluence(\n"
            "    url=confluence_url,\n"
            "    username=confluence_email,\n"
            "    password=confluence_api_token,\n"
            "    cloud=True,\n"
            ")\n"
            "print(f'Connected to Confluence: {confluence_url}')\n"
            "\n"
            "# Lakehouse mount point (auto-mounted by Fabric runtime)\n"
            "BASE = '/lakehouse/default/Files'"
        ),
        _code_cell(
            "# ── Extract Spaces ───────────────────────────────────────────────\n"
            "spaces = []\n"
            "start, limit = 0, 50\n"
            "while True:\n"
            "    batch = api.get_all_spaces(start=start, limit=limit, expand='description.plain')\n"
            "    results = batch.get('results', [])\n"
            "    if not results:\n"
            "        break\n"
            "    for s in results:\n"
            "        spaces.append({\n"
            "            'space_id': s.get('id'),\n"
            "            'space_key': s.get('key'),\n"
            "            'space_name': s.get('name'),\n"
            "            'space_type': s.get('type'),\n"
            "            'description': s.get('description', {}).get('plain', {}).get('value', ''),\n"
            "        })\n"
            "    if batch.get('size', 0) < limit:\n"
            "        break\n"
            "    start += limit\n"
            "\n"
            "spaces_df = pd.DataFrame(spaces)\n"
            "print(f'Extracted {len(spaces_df)} spaces')"
        ),
        _code_cell(
            "# ── Extract Pages ────────────────────────────────────────────────\n"
            "pages = []\n"
            "for sk in spaces_df['space_key']:\n"
            "    start, limit = 0, 50\n"
            "    while True:\n"
            "        results = api.get_all_pages_from_space(\n"
            "            sk, start=start, limit=limit,\n"
            "            expand='body.storage,version,history',\n"
            "        )\n"
            "        if not results:\n"
            "            break\n"
            "        for page in results:\n"
            "            body_html = page.get('body', {}).get('storage', {}).get('value', '')\n"
            "            version = page.get('version', {})\n"
            "            history = page.get('history', {})\n"
            "            pages.append({\n"
            "                'page_id': page.get('id'),\n"
            "                'space_key': sk,\n"
            "                'title': page.get('title'),\n"
            "                'status': page.get('status'),\n"
            "                'body_html': body_html,\n"
            "                'version_number': version.get('number'),\n"
            "                'created_by': history.get('createdBy', {}).get('displayName', ''),\n"
            "                'created_date': history.get('createdDate', ''),\n"
            "                'last_updated_by': version.get('by', {}).get('displayName', ''),\n"
            "                'last_updated_date': version.get('when', ''),\n"
            "            })\n"
            "        if len(results) < limit:\n"
            "            break\n"
            "        start += limit\n"
            "\n"
            "pages_df = pd.DataFrame(pages)\n"
            "print(f'Extracted {len(pages_df)} pages')"
        ),
        _code_cell(
            "# ── Extract Comments ─────────────────────────────────────────────\n"
            "all_comments = []\n"
            "for page_id in pages_df['page_id']:\n"
            "    start, limit = 0, 50\n"
            "    while True:\n"
            "        result = api.get_page_comments(\n"
            "            str(page_id), start=start, limit=limit,\n"
            "            expand='body.storage,version',\n"
            "        )\n"
            "        items = result.get('results', [])\n"
            "        if not items:\n"
            "            break\n"
            "        for c in items:\n"
            "            all_comments.append({\n"
            "                'comment_id': c.get('id'),\n"
            "                'page_id': str(page_id),\n"
            "                'body_html': c.get('body', {}).get('storage', {}).get('value', ''),\n"
            "                'author': c.get('version', {}).get('by', {}).get('displayName', ''),\n"
            "                'created_date': c.get('version', {}).get('when', ''),\n"
            "            })\n"
            "        if len(items) < limit:\n"
            "            break\n"
            "        start += limit\n"
            "\n"
            "comments_df = pd.DataFrame(all_comments) if all_comments else pd.DataFrame()\n"
            "print(f'Extracted {len(comments_df)} comments')"
        ),
        _code_cell(
            "# ── Write to Bronze Layer ───────────────────────────────────────\n"
            "import os\n"
            "now = datetime.now(timezone.utc).isoformat()\n"
            "\n"
            "bronze_data = {\n"
            "    'confluence_spaces': spaces_df,\n"
            "    'confluence_pages': pages_df,\n"
            "    'confluence_comments': comments_df,\n"
            "}\n"
            "\n"
            "for table_name, df in bronze_data.items():\n"
            "    if df.empty:\n"
            "        print(f'  Skipping empty table: {table_name}')\n"
            "        continue\n"
            "    df = df.copy()\n"
            "    df['_ingested_at'] = now\n"
            "    df['_source_file'] = 'confluence_api'\n"
            "\n"
            "    out_dir = f'{BASE}/bronze/{table_name}'\n"
            "    os.makedirs(out_dir, exist_ok=True)\n"
            "    path = f'{out_dir}/data.parquet'\n"
            "    df.to_parquet(path, index=False)\n"
            "    print(f'  Wrote {len(df)} rows to {path}')\n"
            "\n"
            "print('Bronze layer complete.')"
        ),
    ]
    return _make_notebook(cells, lakehouse_id, workspace_id)


# ── Silver Notebook ──────────────────────────────────────────────────


def silver_notebook(lakehouse_id: str, workspace_id: str) -> str:
    """
    Generate the Silver notebook that cleanses bronze Confluence data:
    strips HTML, parses dates, computes word counts, deduplicates.
    Writes both Parquet (analytics) and CSV (AI Search indexing).
    """
    cells = [
        _markdown_cell(
            "# Silver Layer - Confluence Data Transformation\n\n"
            "Reads Bronze Parquet files, cleanses data:\n"
            "- Strips HTML from page/comment bodies\n"
            "- Parses and normalizes dates\n"
            "- Computes word count and content length\n"
            "- Deduplicates by primary key\n\n"
            "Writes both Parquet and CSV (CSV required for AI Search indexing)."
        ),
        _code_cell(
            "# Install dependencies\n"
            "import subprocess, sys\n"
            "subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q',\n"
            "    'beautifulsoup4>=4.12.0'])\n"
            "print('Dependencies installed.')"
        ),
        _code_cell(
            "import os\n"
            "import pandas as pd\n"
            "from datetime import datetime, timezone\n"
            "from bs4 import BeautifulSoup\n"
            "\n"
            "BASE = '/lakehouse/default/Files'\n"
            "\n"
            "def html_to_text(html):\n"
            "    if not html or not isinstance(html, str):\n"
            "        return ''\n"
            "    return BeautifulSoup(html, 'html.parser').get_text(separator=' ', strip=True)\n"
            "\n"
            "def read_bronze(table):\n"
            "    path = f'{BASE}/bronze/{table}/data.parquet'\n"
            "    if os.path.exists(path):\n"
            "        return pd.read_parquet(path)\n"
            "    return pd.DataFrame()\n"
            "\n"
            "def write_silver(df, table):\n"
            "    out_dir = f'{BASE}/silver/{table}'\n"
            "    os.makedirs(out_dir, exist_ok=True)\n"
            "    df.to_parquet(f'{out_dir}/data.parquet', index=False)\n"
            "    df.to_csv(f'{out_dir}/data.csv', index=False)\n"
            "    print(f'  Wrote {len(df)} rows to silver/{table} (Parquet + CSV)')"
        ),
        _code_cell(
            "# ── Transform Spaces ─────────────────────────────────────────────\n"
            "spaces = read_bronze('confluence_spaces')\n"
            "if not spaces.empty:\n"
            "    meta = [c for c in spaces.columns if c.startswith('_')]\n"
            "    spaces = spaces.drop(columns=meta, errors='ignore')\n"
            "    spaces['space_name'] = spaces['space_name'].str.strip()\n"
            "    spaces['space_key'] = spaces['space_key'].str.upper().str.strip()\n"
            "    spaces['space_type'] = spaces['space_type'].str.lower().str.strip()\n"
            "    spaces['description'] = spaces['description'].apply(html_to_text)\n"
            "    spaces['_processed_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_silver(spaces, 'confluence_spaces')\n"
            "else:\n"
            "    print('No spaces data found in bronze.')"
        ),
        _code_cell(
            "# ── Transform Pages ──────────────────────────────────────────────\n"
            "pages = read_bronze('confluence_pages')\n"
            "if not pages.empty:\n"
            "    meta = [c for c in pages.columns if c.startswith('_')]\n"
            "    pages = pages.drop(columns=meta, errors='ignore')\n"
            "    pages['body_text'] = pages['body_html'].apply(html_to_text)\n"
            "    pages['word_count'] = pages['body_text'].apply(\n"
            "        lambda x: len(x.split()) if isinstance(x, str) else 0\n"
            "    )\n"
            "    pages['content_length'] = pages['body_text'].str.len().fillna(0).astype(int)\n"
            "    pages['created_date'] = pd.to_datetime(pages['created_date'], errors='coerce')\n"
            "    pages['last_updated_date'] = pd.to_datetime(pages['last_updated_date'], errors='coerce')\n"
            "    pages['title'] = pages['title'].str.strip()\n"
            "    pages['space_key'] = pages['space_key'].str.upper().str.strip()\n"
            "    pages['status'] = pages['status'].str.lower().str.strip()\n"
            "    before = len(pages)\n"
            "    pages = pages.drop_duplicates(subset=['page_id'], keep='last')\n"
            "    print(f'  Deduped pages: removed {before - len(pages)} duplicates')\n"
            "    pages['_processed_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_silver(pages, 'confluence_pages')\n"
            "else:\n"
            "    print('No pages data found in bronze.')"
        ),
        _code_cell(
            "# ── Transform Comments ───────────────────────────────────────────\n"
            "comments = read_bronze('confluence_comments')\n"
            "if not comments.empty:\n"
            "    meta = [c for c in comments.columns if c.startswith('_')]\n"
            "    comments = comments.drop(columns=meta, errors='ignore')\n"
            "    comments['comment_text'] = comments['body_html'].apply(html_to_text)\n"
            "    comments['word_count'] = comments['comment_text'].apply(\n"
            "        lambda x: len(x.split()) if isinstance(x, str) else 0\n"
            "    )\n"
            "    comments['created_date'] = pd.to_datetime(comments['created_date'], errors='coerce')\n"
            "    comments['author'] = comments['author'].str.strip()\n"
            "    before = len(comments)\n"
            "    comments = comments.drop_duplicates(subset=['comment_id'], keep='last')\n"
            "    print(f'  Deduped comments: removed {before - len(comments)} duplicates')\n"
            "    comments['_processed_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_silver(comments, 'confluence_comments')\n"
            "else:\n"
            "    print('No comments data found in bronze.')\n"
            "\n"
            "print('Silver layer complete.')"
        ),
    ]
    return _make_notebook(cells, lakehouse_id, workspace_id)


# ── Gold Notebook ────────────────────────────────────────────────────


def gold_notebook(lakehouse_id: str, workspace_id: str) -> str:
    """
    Generate the Gold notebook that aggregates silver Confluence data into
    business-ready tables: content by space, author activity, content
    timeline, and most discussed pages.
    """
    cells = [
        _markdown_cell(
            "# Gold Layer - Confluence Business Aggregations\n\n"
            "Reads Silver Parquet files and produces 4 gold tables:\n"
            "1. `confluence_content_by_space` - metrics per space\n"
            "2. `confluence_author_activity` - contributions per author\n"
            "3. `confluence_content_timeline` - daily content creation\n"
            "4. `confluence_most_discussed` - pages ranked by comments\n\n"
            "Writes both Parquet and CSV."
        ),
        _code_cell(
            "import os\n"
            "import pandas as pd\n"
            "from datetime import datetime, timezone\n"
            "\n"
            "BASE = '/lakehouse/default/Files'\n"
            "\n"
            "def read_silver(table):\n"
            "    path = f'{BASE}/silver/{table}/data.parquet'\n"
            "    if os.path.exists(path):\n"
            "        return pd.read_parquet(path)\n"
            "    return pd.DataFrame()\n"
            "\n"
            "def write_gold(df, table):\n"
            "    out_dir = f'{BASE}/gold/{table}'\n"
            "    os.makedirs(out_dir, exist_ok=True)\n"
            "    df.to_parquet(f'{out_dir}/data.parquet', index=False)\n"
            "    df.to_csv(f'{out_dir}/data.csv', index=False)\n"
            "    print(f'  Wrote {len(df)} rows to gold/{table} (Parquet + CSV)')\n"
            "\n"
            "pages = read_silver('confluence_pages')\n"
            "comments = read_silver('confluence_comments')\n"
            "print(f'Loaded silver data: {len(pages)} pages, {len(comments)} comments')"
        ),
        _code_cell(
            "# ── Content By Space ─────────────────────────────────────────────\n"
            "if not pages.empty:\n"
            "    cbs = (\n"
            "        pages.groupby('space_key')\n"
            "        .agg(\n"
            "            page_count=('page_id', 'nunique'),\n"
            "            total_words=('word_count', 'sum'),\n"
            "            avg_word_count=('word_count', 'mean'),\n"
            "            total_content_length=('content_length', 'sum'),\n"
            "            latest_update=('last_updated_date', 'max'),\n"
            "        )\n"
            "        .reset_index()\n"
            "        .sort_values('page_count', ascending=False)\n"
            "    )\n"
            "    cbs['avg_word_count'] = cbs['avg_word_count'].round(0).astype(int)\n"
            "    cbs['_aggregated_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_gold(cbs, 'confluence_content_by_space')\n"
            "else:\n"
            "    print('No pages data — skipping content_by_space.')"
        ),
        _code_cell(
            "# ── Author Activity ──────────────────────────────────────────────\n"
            "if not pages.empty:\n"
            "    page_agg = (\n"
            "        pages.groupby('created_by')\n"
            "        .agg(pages_created=('page_id', 'nunique'), pages_total_words=('word_count', 'sum'))\n"
            "        .reset_index()\n"
            "        .rename(columns={'created_by': 'author'})\n"
            "    )\n"
            "    if not comments.empty:\n"
            "        comment_agg = (\n"
            "            comments.groupby('author')\n"
            "            .agg(comments_made=('comment_id', 'nunique'), comments_total_words=('word_count', 'sum'))\n"
            "            .reset_index()\n"
            "        )\n"
            "        agg = page_agg.merge(comment_agg, on='author', how='outer').fillna(0)\n"
            "    else:\n"
            "        agg = page_agg.copy()\n"
            "        agg['comments_made'] = 0\n"
            "        agg['comments_total_words'] = 0\n"
            "    for col in ['pages_created', 'pages_total_words', 'comments_made', 'comments_total_words']:\n"
            "        agg[col] = agg[col].astype(int)\n"
            "    agg['total_contributions'] = agg['pages_created'] + agg['comments_made']\n"
            "    agg = agg.sort_values('total_contributions', ascending=False)\n"
            "    agg['_aggregated_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_gold(agg, 'confluence_author_activity')\n"
            "else:\n"
            "    print('No pages data — skipping author_activity.')"
        ),
        _code_cell(
            "# ── Content Timeline ─────────────────────────────────────────────\n"
            "rows = []\n"
            "if not pages.empty and 'created_date' in pages.columns:\n"
            "    p = pages.copy()\n"
            "    p['day'] = p['created_date'].dt.date\n"
            "    rows.append(p.groupby('day').agg(pages_created=('page_id', 'nunique')).reset_index())\n"
            "\n"
            "if not comments.empty and 'created_date' in comments.columns:\n"
            "    c = comments.copy()\n"
            "    c['day'] = c['created_date'].dt.date\n"
            "    rows.append(c.groupby('day').agg(comments_created=('comment_id', 'nunique')).reset_index())\n"
            "\n"
            "if rows:\n"
            "    if len(rows) == 2:\n"
            "        tl = rows[0].merge(rows[1], on='day', how='outer').fillna(0)\n"
            "    else:\n"
            "        tl = rows[0]\n"
            "        if 'pages_created' not in tl.columns: tl['pages_created'] = 0\n"
            "        if 'comments_created' not in tl.columns: tl['comments_created'] = 0\n"
            "    tl = tl.sort_values('day')\n"
            "    for col in ['pages_created', 'comments_created']:\n"
            "        if col in tl.columns: tl[col] = tl[col].astype(int)\n"
            "    tl['total_activity'] = tl.get('pages_created', 0) + tl.get('comments_created', 0)\n"
            "    tl['cumulative_pages'] = tl.get('pages_created', pd.Series([0])).cumsum()\n"
            "    tl['_aggregated_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_gold(tl, 'confluence_content_timeline')\n"
            "else:\n"
            "    print('No timeline data available.')"
        ),
        _code_cell(
            "# ── Most Discussed Pages ────────────────────────────────────────\n"
            "if not pages.empty:\n"
            "    if not comments.empty:\n"
            "        cc = (\n"
            "            comments.groupby('page_id')\n"
            "            .agg(comment_count=('comment_id', 'nunique'))\n"
            "            .reset_index()\n"
            "        )\n"
            "        cc['page_id'] = cc['page_id'].astype(str)\n"
            "        pm = pages.copy()\n"
            "        pm['page_id'] = pm['page_id'].astype(str)\n"
            "        md = pm[['page_id', 'title', 'space_key', 'word_count']].merge(\n"
            "            cc, on='page_id', how='left'\n"
            "        )\n"
            "        md['comment_count'] = md['comment_count'].fillna(0).astype(int)\n"
            "    else:\n"
            "        md = pages[['page_id', 'title', 'space_key', 'word_count']].copy()\n"
            "        md['comment_count'] = 0\n"
            "    md = md.sort_values('comment_count', ascending=False)\n"
            "    md['_aggregated_at'] = datetime.now(timezone.utc).isoformat()\n"
            "    write_gold(md, 'confluence_most_discussed')\n"
            "else:\n"
            "    print('No pages data — skipping most_discussed.')\n"
            "\n"
            "print('Gold layer complete.')"
        ),
    ]
    return _make_notebook(cells, lakehouse_id, workspace_id)
