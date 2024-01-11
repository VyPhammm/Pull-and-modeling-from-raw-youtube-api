from utils.data_extract.http_requests import HTTPRequestsDataExtract
from airflow.models import Variable


class ClickUp:
    space_id = "26343267"
    bug_list_id = "164102746"
    api_key = Variable.get(f"clickup_api")
    base_url = "https://api.clickup.com/api/v2"

    def __init__(self):
        self.headers = {"Authorization": self.api_key}

    def get_list_members(self, list_id: str):
        """
        https://clickup.com/api/clickupreference/operation/GetListMembers/
        """
        url = f"https://api.clickup.com/api/v2/list/{list_id}/member"
        return self.pull_data(url=url)

    def get_folders(self, space_id: str = None):
        """
        https://clickup.com/api/clickupreference/operation/GetFolders/
        """
        space_id = space_id or self.space_id
        url = f"{self.base_url}/space/{space_id}/folder"
        return self.pull_data(url)

    def get_lists(self, folder_id: str):
        """
        https://clickup.com/api/clickupreference/operation/GetLists/
        """
        url = f"{self.base_url}/folder/{folder_id}/list"
        return self.pull_data(url)

    def get_tasks(self, list_id: str):
        """
        https://clickup.com/api/clickupreference/operation/GetTasks/
        """
        url =  f"{self.base_url}/list/{list_id}/task"
        return self.pull_data(url)

    def get_task(self, task_id: str):
        """
        https://clickup.com/api/clickupreference/operation/GetTask/
        """
        url =  f"{self.base_url}/task/{task_id}"
        return self.pull_data(url)

    def pull_data(self, url, headers=None, params=None):
        headers = headers or self.headers
        extractor = HTTPRequestsDataExtract(url, headers=headers, params=params)
        response = extractor.make_request()
        return response.json()

    def create_tasks(self, list_id: str, payload: dict, params : dict = None):
        """
            Create tasks on ClickUp
            https://clickup.com/api/clickupreference/operation/CreateTask/
        """
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
        url = "https://api.clickup.com/api/v2/list/" + list_id + "/task"

        extractor = HTTPRequestsDataExtract(
            url, method="POST", headers=headers, json=payload, params=params
        )
        return extractor.make_request()