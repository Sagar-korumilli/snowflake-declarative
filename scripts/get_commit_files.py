#!/usr/bin/env python3
"""
GitHub Repository Monitor Script
Tracks file changes and PR timestamps directly from GitHub API
"""

import json
import argparse
import sys
from typing import Dict, List, Optional

class GitHubRepoMonitor:
    def __init__(self, owner: str, repo: str, github_token: str, branch: str = None):
        """
        Initialize the GitHub repository monitor
        
        Args:
            owner: GitHub repository owner/organization
            repo: Repository name
            github_token: GitHub personal access token
            branch: Branch to check (default: repository's default branch)
        """
        self.owner = owner
        self.repo = repo
        self.github_token = github_token
        self.branch = branch
        self.base_url = f'https://api.github.com/repos/{owner}/{repo}'
        
        try:
            import requests
            self.requests = requests
        except ImportError:
            raise ImportError("requests library is required. Install with: pip install requests")
    
    def make_github_request(self, endpoint: str, params: dict = None) -> dict:
        """Make a request to GitHub API"""
        headers = {
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        url = f"{self.base_url}{endpoint}"
        response = self.requests.get(url, headers=headers, params=params or {})
        
        if response.status_code == 401:
            raise Exception("Invalid GitHub token or insufficient permissions")
        elif response.status_code == 404:
            raise Exception(f"Repository {self.owner}/{self.repo} not found or not accessible")
        elif response.status_code != 200:
            raise Exception(f"GitHub API error: {response.status_code} - {response.text}")
        
        return response.json()
    
    def get_latest_commit_changes(self) -> Dict:
        """Get files changed in the latest commit"""
        # Prepare parameters for the API call
        params = {'per_page': 1}
        if self.branch:
            params['sha'] = self.branch
        
        # Get the latest commit
        commits = self.make_github_request('/commits', params)
        
        if not commits:
            return {}
        
        latest_commit = commits[0]
        commit_sha = latest_commit['sha']
        
        # Get detailed commit info with file changes
        commit_details = self.make_github_request(f'/commits/{commit_sha}')
        
        # Parse file changes
        changes = {'added': [], 'modified': [], 'deleted': []}
        
        if 'files' in commit_details:
            for file_info in commit_details['files']:
                filename = file_info['filename']
                status = file_info['status']
                
                if status == 'added':
                    changes['added'].append(filename)
                elif status == 'modified':
                    changes['modified'].append(filename)
                elif status == 'removed':
                    changes['deleted'].append(filename)
                elif status == 'renamed':
                    changes['modified'].append(filename)  # Treat rename as modification
        
        return {
            'commit_hash': commit_details['sha'],
            'author': commit_details['commit']['author']['name'],
            'date': commit_details['commit']['author']['date'],
            'message': commit_details['commit']['message'],
            'branch': self.branch or 'default',
            'changes': changes,
            'url': commit_details['html_url']
        }
    
    def get_latest_pr_timestamp(self) -> Optional[str]:
        """Get timestamp of the latest PR"""
        try:
            # Get latest PRs (all states, sorted by updated)
            prs = self.make_github_request('/pulls', {
                'state': 'all',
                'sort': 'updated',
                'direction': 'desc',
                'per_page': 1
            })
            
            if prs:
                return prs[0]['updated_at']
            
        except Exception as e:
            print(f"Error fetching PR info: {e}")
        
        return None
    
    def get_latest_pr_info(self) -> Optional[Dict]:
        """Get detailed information about the latest PR"""
        try:
            # Get latest PRs (all states, sorted by updated)
            prs = self.make_github_request('/pulls', {
                'state': 'all',
                'sort': 'updated',
                'direction': 'desc',
                'per_page': 1
            })
            
            if prs:
                latest_pr = prs[0]
                return {
                    'number': latest_pr['number'],
                    'title': latest_pr['title'],
                    'state': latest_pr['state'],
                    'created_at': latest_pr['created_at'],
                    'updated_at': latest_pr['updated_at'],
                    'merged_at': latest_pr.get('merged_at'),
                    'author': latest_pr['user']['login'],
                    'url': latest_pr['html_url']
                }
            
        except Exception as e:
            print(f"Error fetching PR info: {e}")
        
        return None

def parse_repo_url(repo_input: str) -> tuple:
    """Parse repository input to extract owner and repo name"""
    # Handle different input formats
    if repo_input.startswith('https://github.com/'):
        # Full GitHub URL
        path = repo_input.replace('https://github.com/', '').replace('.git', '')
        parts = path.split('/')
        if len(parts) >= 2:
            return parts[0], parts[1]
    elif '/' in repo_input:
        # owner/repo format
        parts = repo_input.split('/')
        if len(parts) >= 2:
            return parts[0], parts[1]
    
    raise ValueError("Invalid repository format. Use 'owner/repo' or full GitHub URL")

def main():
    parser = argparse.ArgumentParser(
        description='Get last commit changes and latest PR timestamp from GitHub',
        epilog="""
Examples:
  python script.py owner/repo --token ghp_xxxxx
  python script.py owner/repo --token ghp_xxxxx --branch develop
  python script.py https://github.com/owner/repo --token ghp_xxxxx --branch feature-branch
        """
    )
    parser.add_argument('repo', help='GitHub repository (owner/repo or full URL)')
    parser.add_argument('--token', required=True, help='GitHub personal access token')
    parser.add_argument('--branch', help='Branch to check (default: repository default branch)')
    parser.add_argument('--format', choices=['json', 'text'], default='text', help='Output format')
    parser.add_argument('--pr-details', action='store_true', help='Show detailed PR info instead of just timestamp')
    
    args = parser.parse_args()
    
    try:
        # Parse repository
        owner, repo = parse_repo_url(args.repo)
        
        # Initialize monitor
        monitor = GitHubRepoMonitor(owner, repo, args.token, args.branch)
        
        # Get latest commit changes
        latest_commit = monitor.get_latest_commit_changes()
        
        # Get PR information
        if args.pr_details:
            pr_info = monitor.get_latest_pr_info()
        else:
            pr_timestamp = monitor.get_latest_pr_timestamp()
        
        if args.format == 'json':
            output = {
                'repository': f"{owner}/{repo}",
                'latest_commit': latest_commit,
            }
            
            if args.pr_details:
                output['latest_pr'] = pr_info
            else:
                output['latest_pr_timestamp'] = pr_timestamp
            
            print(json.dumps(output, indent=2))
        else:
            print("=" * 60)
            print(f"GITHUB REPO: {owner}/{repo} (branch: {args.branch or 'default'})")
            print("=" * 60)
            
            # Latest commit info
            print(f"\n📝 LAST COMMIT:")
            print(f"Hash: {latest_commit.get('commit_hash', 'N/A')[:8]}...")
            print(f"Author: {latest_commit.get('author', 'N/A')}")
            print(f"Date: {latest_commit.get('date', 'N/A')}")
            print(f"Branch: {latest_commit.get('branch', 'N/A')}")
            print(f"Message: {latest_commit.get('message', 'N/A')}")
            print(f"URL: {latest_commit.get('url', 'N/A')}")
            
            changes = latest_commit.get('changes', {})
            if changes.get('added'):
                print(f"➕ Added: {', '.join(changes['added'])}")
            if changes.get('modified'):
                print(f"📝 Modified: {', '.join(changes['modified'])}")
            if changes.get('deleted'):
                print(f"🗑️ Deleted: {', '.join(changes['deleted'])}")
            
            # PR information
            if args.pr_details and pr_info:
                print(f"\n🔀 LATEST PULL REQUEST:")
                print(f"Number: #{pr_info['number']}")
                print(f"Title: {pr_info['title']}")
                print(f"State: {pr_info['state']}")
                print(f"Author: {pr_info['author']}")
                print(f"Created: {pr_info['created_at']}")
                print(f"Updated: {pr_info['updated_at']}")
                if pr_info['merged_at']:
                    print(f"Merged: {pr_info['merged_at']}")
                print(f"URL: {pr_info['url']}")
            elif not args.pr_details:
                if pr_timestamp:
                    print(f"\n🔀 LATEST PR TIMESTAMP: {pr_timestamp}")
                else:
                    print(f"\n🔀 LATEST PR: No PR found")
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
