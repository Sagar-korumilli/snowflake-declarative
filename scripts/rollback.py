#!/usr/bin/env python3
import argparse
import os
import sys
import tempfile
import base64
import requests
import re
from datetime import datetime, timezone
import snowflake.connector

class GitHubPRAnalyzer:
    def __init__(self, repo, token, branch=None, path_filter='snowflake/'):
        self.owner, self.repo = self.parse_repo_url(repo)
        self.token = token
        self.branch = branch
        self.path_filter = path_filter.rstrip('/') + '/'
        self.base_url = f'https://api.github.com/repos/{self.owner}/{self.repo}'

    def parse_repo_url(self, url):
        if url.startswith('https://github.com/'):
            parts = url.replace('https://github.com/', '').rstrip('.git').split('/')
            return parts[0], parts[1]
        elif '/' in url:
            parts = url.split('/')
            return parts[0], parts[1]
        raise ValueError('Invalid GitHub repo format')

    def make_request(self, endpoint, params=None):
        headers = {
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        url = f"{self.base_url}{endpoint}"
        resp = requests.get(url, headers=headers, params=params or {})
        if resp.status_code != 200:
            raise Exception(f"GitHub API error {resp.status_code}: {resp.text}")
        return resp.json()

    def get_latest_pr_number(self):
        prs = self.make_request('/pulls', {'state': 'closed', 'sort': 'updated', 'direction': 'desc', 'per_page': 1})
        if prs:
            return prs[0]['number']
        raise Exception('No pull requests found')

    def get_pr_files(self, pr_number):
        files, page = [], 1
        while True:
            batch = self.make_request(f'/pulls/{pr_number}/files', {'page': page, 'per_page': 100})
            if not batch:
                break
            files.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return files

    def fetch_file_content(self, path):
        params = {'ref': self.branch} if self.branch else {}
        data = self.make_request(f'/contents/{path}', params)
        content = data.get('content')
        return base64.b64decode(content).decode('utf-8', errors='ignore') if content else None

    def get_pr_merged_time(self, pr_number):
        pr = self.make_request(f'/pulls/{pr_number}')
        merged_at = pr.get('merged_at')
        if not merged_at:
            raise Exception('PR not merged yet')
        dt = datetime.strptime(merged_at, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def parse_sql_metadata(self, sql_text):
        pattern = re.compile(
            r"\b(CREATE|ALTER|INSERT)\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW)\b[\s\w\.\(\)]+?\b((?:[\w]+\.){0,2}[\w]+)\b",
            re.IGNORECASE
        )
        m = pattern.search(sql_text or '')
        if not m:
            return {}
        obj_type = m.group(2).upper()
        parts = m.group(3).split('.')
        db = schema = None
        name = parts[-1]
        if len(parts) == 3:
            db, schema, name = parts
        elif len(parts) == 2:
            schema, name = parts
        return {'database': db, 'schema': schema, 'object_type': obj_type, 'object_name': name}

    def enrich_pr_files(self, pr_files):
        objs = []
        for f in pr_files:
            path = f['filename']
            if not path.startswith(self.path_filter):
                continue
            status = f['status']
            default_schema = path.split('/')[1] if len(path.split('/')) > 2 else None
            content = None if status == 'removed' else self.fetch_file_content(path)
            meta = self.parse_sql_metadata(content)
            schema = meta.get('schema') or default_schema
            objs.append({
                'database': meta.get('database'),
                'schema': schema,
                'object_type': meta.get('object_type'),
                'object_name': meta.get('object_name'),
                'change_type': status
            })
        return objs


def get_snowflake_connection():
    user = os.getenv('SNOWFLAKE_USER')
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    role = os.getenv('SNOWFLAKE_ROLE')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    private_key_str = os.getenv('SNOWFLAKE_PRIVATE_KEY')
    key_passphrase = os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')

    if not all([user, account, role, warehouse, database, private_key_str, key_passphrase]):
        raise EnvironmentError('Missing one or more Snowflake env vars')

    with tempfile.NamedTemporaryFile('w+', delete=False, suffix='.pem') as key_file:
        key_file.write(private_key_str)
        key_path = key_file.name

    return snowflake.connector.connect(
        user=user,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database,
        private_key_file=key_path,
        private_key_file_pwd=key_passphrase
    )


def get_column_names(cursor, database, schema, table):
    cursor.execute(f"SHOW COLUMNS IN TABLE {database}.{schema}.{table};")
    return [row[2].upper() for row in cursor.fetchall()]


def clone_table_at_point(cursor, database, schema, table, ts_literal, tag):
    backup_name = f"{table}_backup_{tag}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    sql = (
        f"CREATE TABLE {database}.{schema}.{backup_name} "
        f"CLONE {database}.{schema}.{table} AT (TIMESTAMP => '{ts_literal}');"
    )
    print(f"[BACKUP] {sql}")
    cursor.execute(sql)
    return backup_name


def rollback_data_inplace(cursor, database, schema, table, ts_literal):
    sql = (
        f"INSERT OVERWRITE INTO {database}.{schema}.{table} "
        f"SELECT * FROM {database}.{schema}.{table} AT (TIMESTAMP => '{ts_literal}');"
    )
    print(f"[ROLLBACK DATA] {sql}")
    cursor.execute(sql)


def rollback_added_columns(cursor, database, schema, table, ts_literal):
    backup = clone_table_at_point(cursor, database, schema, table, ts_literal, 'schema_rollback')
    current = set(get_column_names(cursor, database, schema, table))
    old = set(get_column_names(cursor, database, schema, backup))
    for col in current - old:
        sql = f"ALTER TABLE {database}.{schema}.{table} DROP COLUMN {col};"
        print(f"[ROLLBACK SCHEMA] {sql}")
        try:
            cursor.execute(sql)
        except Exception as e:
            print(f"⚠️ Failed to drop {col}: {e}")


def rollback_dropped_columns(cursor, database, schema, table, ts_literal):
    print("⚠️ Cannot auto-restore dropped columns; creating recovery backup.")
    clone_table_at_point(cursor, database, schema, table, ts_literal, 'recover')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--repo', required=True)
    p.add_argument('--token', required=True)
    p.add_argument('--branch', help='Branch for SQL files')
    p.add_argument('--path', default='snowflake/')
    args = p.parse_args()

    analyzer = GitHubPRAnalyzer(args.repo, args.token, args.branch, args.path)
    pr = analyzer.get_latest_pr_number()
    files = analyzer.get_pr_files(pr)
    objs = analyzer.enrich_pr_files(files)
    if not objs:
        print('No SQL objects to rollback.')
        sys.exit(0)

    ts = analyzer.get_pr_merged_time(pr)
    print(f"[INFO] Rollback timestamp: {ts}")

    conn = get_snowflake_connection()
    cur = conn.cursor()

    for o in objs:
        db = o.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        sch = o.get('schema')
        tbl = o.get('object_name')
        typ = (o.get('object_type') or '').upper()
        ch = o.get('change_type')
        if not (sch and tbl and typ == 'TABLE'):
            print(f"[SKIP] {sch}.{tbl} ({typ})")
            continue
        print(f"[PROCESS] {db}.{sch}.{tbl} ({ch})")
        clone_table_at_point(cur, db, sch, tbl, datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), 'pre')
        if ch in ('modified','changed','updated'):
            rollback_data_inplace(cur, db, sch, tbl, ts)
        elif ch == 'added':
            rollback_added_columns(cur, db, sch, tbl, ts)
        elif ch == 'removed':
            rollback_dropped_columns(cur, db, sch, tbl, ts)
        else:
            print(f"[INFO] No action for change type '{ch}'")

    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
