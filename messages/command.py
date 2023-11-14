from messages.message import Message

class Command(Message):
    def __init__(self, command, extra_information, flag=None, recv=None):

        super().__init__()
        self.command=command
        self.extra_information=extra_information
        self.flag=flag
        self.recv=recv