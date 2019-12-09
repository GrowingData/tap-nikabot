import requests
import json
import logging
from datetime import datetime, timedelta, date


class NikabotClient:
    def __init__(self, team_id, auth_token):
        self.auth_token = auth_token
        self.team = team_id
        self.header = {"Authorization": f"Bearer {self.auth_token}"}
        self.base_url = "https://api.nikabot.com/api/v1"
        self.PAGE_SIZE = 100

    def create_url(self, path):
        return f"{self.base_url}/{path}"

    def get_url(self, path, params):
        url = self.create_url(path)
        logging.info(f"NikabotClient.get_url: {url}")
        response = requests.get(url, params=params, headers=self.header)

        if response.text == None or len(response.text) == 0:
            raise RuntimeError("Nikabot returned no rows!")

        decoded = json.loads(response.text)
        if "result" not in decoded:
            logging.error(f"{response.request.url}")
            logging.error(f"NikabotClient.get_url: Failed: \n{response.text}")
            raise RuntimeError(f"No result was provided in the response from Nikabot: {url}")

        return decoded

    def get_paged(self, path, params={}):
        current_page = 0

        # Setup paging properly
        params["limit"] = self.PAGE_SIZE
        params["page"] = 0

        data = None
        while data == None or len(data["result"]) == self.PAGE_SIZE:
            data = self.get_url(path, params)
            for record in data["result"]:
                yield record

            params["page"] = params["page"] + 1
