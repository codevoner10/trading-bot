import os
import httpx

class GitHubActionsClient:
    """إطلاق مسارات العمل (Workflows) برمجياً عبر GitHub API"""
    
    def __init__(self):
        self.token = os.getenv("PAT_TOKEN")
        self.repo = os.getenv("REPO_NAME") 
        if not self.token or not self.repo:
            raise ValueError("PAT_TOKEN or REPO_NAME missing in environment variables.")
            
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.base_url = f"https://api.github.com/repos/{self.repo}/actions/workflows"

    async def dispatch_workflow(self, workflow_filename: str, trigger_source: str = "Proactive_Handover") -> bool:
        """إرسال طلب تشغيل مع تمرير مصدر التشغيل كمدخل (inputs)"""
        url = f"{self.base_url}/{workflow_filename}/dispatches"
        payload = {
            "ref": "main",
            "inputs": {
                "trigger_source": trigger_source
            }
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=self.headers, json=payload, timeout=10.0)
                if response.status_code == 204:
                    print(f"[GitHub API] Successfully dispatched {workflow_filename}")
                    return True
                else:
                    print(f"[GitHub API Error] Failed to dispatch {workflow_filename}: {response.text}")
                    return False
        except Exception as e:
            print(f"[GitHub API Error] Exception during dispatch: {e}")
            return False