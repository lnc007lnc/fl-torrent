from messages.message import Message

class Command(Message):
    def __init__(self, command, extra_information):

        super().__init__()
        self.command=command
        self.extra_information=extra_information