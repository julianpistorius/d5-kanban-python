# Get rid of this horrible module

from infrastructure.message_hub import MessageHub

_hub = MessageHub()

def hub():
    return _hub
