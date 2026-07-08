import os
import httpx

class GitHubActionsClient:
    """إطلاق مسارات العمل (Workflows) برمجياً"""
    
    def __init__(self):
        self.token = os.getenv("GITHUB_TOKEN")
        self.repo = os.getenv("GITHUB_REPO")
        if not self.token or not self.repo:
            raise ValueError("GITHUB_TOKEN or GITHUB_REPO missing.")
        self.headers = {"Authorization": f"token {self.token}", "Accept": "application/vnd.github.v3+json"}
        self.base_url = f"https://api.github.com/repos/{self.repo}/actions/workflows"

    async def dispatch_workflow(self, workflow_filename: str) -> bool:
        url = f"{self.base_url}/{workflow_filename}/dispatches"
        payload = {"ref": "main"}
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=self.headers, json=payload, timeout=10.0)
                if response.status_code == 204:
                    print(f"[GitHub API] Dispatched {workflow_filename}")
                    return True
                return False
        except Exception as e:
            print(f"[GitHub API Error] Exception: {e}")
            return False