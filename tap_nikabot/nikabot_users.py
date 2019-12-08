from src.nikabot_client import NikabotClient
from hyper_model import HyperModel
import datetime


class nikabot_users(HyperModel):

    def __init__(self):
        self.nikabot = NikabotClient()

    def initialize(self, request):
        # This model does not require any initialization
        return True

    def execute(self, request):
        dataframe = self.nikabot.get_users()
        return self.dataframe_result(request, dataframe)
