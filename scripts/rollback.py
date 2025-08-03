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
        headers = {'Authorization': f'token {self.token}', 'Accept': 'application/vnd.github.v3+json'}
        resp = requests.get(f"{self.base_url}{endpoint}", headers=headers, params=params or {})
        if resp.status_code != 200:
            raise Exception(f"GitHub API error {resp.status_code}: {resp.text}")
        return resp.json()

    def get_latest_pr_number(self):
        prs = self.make_request('/pulls', {'state':'closed','sort':'updated','direction':'desc','per_page':1})
        if not prs:
            raise Exception('No pull requests found')
        return prs[0]['number']

    def get_pr_files(self, pr_number):
        files, page = [], 1
        while True:
            batch = self.make_request(f'/pulls/{pr_number}/files', {'page':page,'per_page':100})
            if not batch:
                break
            files.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return files

    def fetch_file_content(self, path):
        params = {'ref':self.branch} if self.branch else {}
        data = self.make_request(f'/contents/{path}', params)
        content = data.get('content')
        return base64.b64decode(content).decode('utf-8',errors='ignore') if content else None

    def get_pr_merged_time(self, pr_number):
        pr = self.make_request(f'/pulls/{pr_number}')
        merged = pr.get('merged_at')
        if not merged:
            raise Exception('PR not merged yet')
        dt = datetime.strptime(merged,'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def parse_sql_metadata(self, text):
        if not text:
            return {}
        ins = re.search(r"\bINSERT\s+INTO\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
        if ins:
            parts = ins.group(1).split('.')
            db = schema = None
            name = parts[-1]
            if len(parts)==3:
                db,schema,name = parts
            elif len(parts)==2:
                schema,name = parts
            return {'database':db,'schema':schema,'object_type':'INSERT','object_name':name}
        m = re.search(r"\b(CREATE|ALTER)\s+(?:OR\s+REPLACE\s+)?(TABLE|VIEW)\s+((?:[\w]+\.){0,2}[\w]+)", text, re.IGNORECASE)
        if not m:
            return {}
        obj = m.group(2).upper()
        parts = m.group(3).split('.')
        db = schema = None; name = parts[-1]
        if len(parts)==3:
            db,schema,name = parts
        elif len(parts)==2:
            schema,name = parts
        return {'database':db,'schema':schema,'object_type':obj,'object_name':name}

    def enrich_pr_files(self, files):
        objs=[]
        for f in files:
            if not f['filename'].startswith(self.path_filter):
                continue
            status = f['status']
            content = None if status=='removed' else self.fetch_file_content(f['filename'])
            meta = self.parse_sql_metadata(content)
            if not meta:
                continue
            objs.append({**meta,'change_type':status})
        return objs


def get_snowflake_connection():
    creds = {k:os.getenv(k) for k in ['SNOWFLAKE_USER','SNOWFLAKE_ACCOUNT','SNOWFLAKE_ROLE','SNOWFLAKE_WAREHOUSE','SNOWFLAKE_DATABASE','SNOWFLAKE_PRIVATE_KEY','SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']}
    if not all(creds.values()):
        missing=[k for k,v in creds.items() if not v]
        raise EnvironmentError(f"Missing Snowflake vars: {missing}")
    with tempfile.NamedTemporaryFile('w+',delete=False,suffix='.pem') as f:
        f.write(creds['SNOWFLAKE_PRIVATE_KEY'])
        keypath=f.name
    return snowflake.connector.connect(
        user=creds['SNOWFLAKE_USER'], account=creds['SNOWFLAKE_ACCOUNT'], role=creds['SNOWFLAKE_ROLE'],
        warehouse=creds['SNOWFLAKE_WAREHOUSE'], database=creds['SNOWFLAKE_DATABASE'],
        private_key_file=keypath, private_key_file_pwd=creds['SNOWFLAKE_PRIVATE_KEY_PASSPHRASE']
    )

def get_column_names(cur,db,sch,tbl):
    cur.execute(f"SHOW COLUMNS IN TABLE {db}.{sch}.{tbl};")
    return [r[2].upper() for r in cur.fetchall()]

def clone_table_at_point(cur,db,sch,tbl,ts,tag):
    name=f"{tbl}_backup_{tag}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    sql=f"CREATE TABLE {db}.{sch}.{name} CLONE {db}.{sch}.{tbl} AT (TIMESTAMP=>'{ts}');"
    print(f"[BACKUP] {sql}")
    cur.execute(sql)
    return name

def rollback_data_inplace(cur,db,sch,tbl,ts):
    sql=f"INSERT OVERWRITE INTO {db}.{sch}.{tbl} SELECT * FROM {db}.{sch}.{tbl} AT (TIMESTAMP=>'{ts}');"
    print(f"[ROLLBACK DATA] {sql}")
    cur.execute(sql)

def rollback_added_columns(cur,db,sch,tbl,ts):
    b=clone_table_at_point(cur,db,sch,tbl,ts,'schema')
    curr=set(get_column_names(cur,db,sch,tbl))
    old=set(get_column_names(cur,db,sch,b))
    for c in curr-old:
        sql=f"ALTER TABLE {db}.{sch}.{tbl} DROP COLUMN {c};"
        print(f"[ROLLBACK SCHEMA] {sql}")
        cur.execute(sql)

def rollback_dropped_columns(cur,db,sch,tbl,ts):
    print("⚠️ Cannot auto-restore dropped columns; backup clone created.")
    clone_table_at_point(cur,db,sch,tbl,ts,'recover')


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--repo',required=True)
    p.add_argument('--token',required=True)
    p.add_argument('--branch')
    p.add_argument('--path',default='snowflake/')
    args=p.parse_args()

    gh=GitHubPRAnalyzer(args.repo,args.token,args.branch,args.path)
    pr=gh.get_latest_pr_number()
    files=gh.get_pr_files(pr)
    objs=gh.enrich_pr_files(files)
    if not objs:
        print('No SQL objects to rollback.'); sys.exit(0)
    ts=gh.get_pr_merged_time(pr)
    print(f"[INFO] Rollback ts: {ts}")

    conn=get_snowflake_connection(); cur=conn.cursor()
    now_ts=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    for o in objs:
        db=o['database'] or os.getenv('SNOWFLAKE_DATABASE')
        sch=o['schema']; tbl=o['object_name']; typ=o['object_type'].upper(); ch=o['change_type']
        if not ((sch and tbl and typ=='TABLE') or typ=='INSERT'):
            print(f"[SKIP] {sch}.{tbl} ({typ})"); continue
        print(f"[PROCESS] {db}.{sch}.{tbl} ({ch})")
        clone_table_at_point(cur,db,sch,tbl,now_ts,'pre')
        if typ=='INSERT' or ch in ('modified','changed','updated'):
            rollback_data_inplace(cur,db,sch,tbl,ts)
        elif ch=='added':
            rollback_added_columns(cur,db,sch,tbl,ts)
        elif ch=='removed':
            rollback_dropped_columns(cur,db,sch,tbl,ts)
    cur.close(); conn.close()

if __name__=='__main__':
    main()
