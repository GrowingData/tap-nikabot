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

    def get_paged(self, path, params = {}):
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

    def get_users(self):
        params = {"page": 0, "limit": 100}

        dataframe = self.get_url("users", params)
        return dataframe

    def get_timesheets(self, start_date, end_date):
        dateEnd = end_date.strftime("%Y%m%d")
        dateStart = start_date.strftime("%Y%m%d")

        params = {"dateStart": dateStart, "dateEnd": dateEnd, "page": 0, "limit": 100}
        current_page = 0
        request_df = None
        complete_df = None
        while request_df is None or len(request_df.index) == 100:
            logging.info(f"NikabotClient.get_timesheets: {dateStart}-{dateEnd}")
            params["page"] = current_page
            request_df = self.get_url("records", params)

            # No more results on this page
            if len(request_df.index) == 0:
                break

            if complete_df is None:
                complete_df = request_df
            else:
                complete_df = pd.concat([complete_df, request_df])

            current_page += 1
        return complete_df
