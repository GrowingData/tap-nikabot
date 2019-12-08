from datetime import datetime, timedelta, date
import pandas as pd
from hyper_model import HyperModel
from hyper_model import DataPipeHost
from nikabot_client import NikabotClient


class nikabot_timesheets(HyperModel):

    def __init__(self):
        pass

    def initialize(self, request):
        self.auth_token = self.config["nikabot:auth_token"]
        self.nikabot = NikabotClient(self.auth_token)
        return True

    def execute(self, request):
        today = date.today()
        history_days = 180
        request_interval = 30
        if "historyDays" in self.yaml_settings:
            history_days = int(self.yaml_settings["historyDays"])
        if "requestInterval" in self.yaml_settings:
            request_interval = int(self.yaml_settings["requestInterval"])

        start = today - timedelta(days=history_days)
        current = start

        # We need to increment the start date by the minimum step
        # so that we aren't requesting the enddate twice (once as the end, and once as the start)
        request_increment_delta = timedelta(days=1)
        request_interval_delta = timedelta(days=request_interval)

        merged = None

        while current < today:
            if merged is None:
                merged = self.nikabot.get_timesheets(current+request_increment_delta, current + request_interval_delta)
            else:
                stepper = self.nikabot.get_timesheets(current+request_increment_delta, current + request_interval_delta)
                merged = pd.concat([merged, stepper])

            current = current + request_interval_delta

        return self.dataframe_result(request, merged)


if __name__ == '__main__':
    DataPipeHost().connect()
