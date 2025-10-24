from apps.agents.supervisor import handle
from apps.agents.registry import TOOL_REGISTRY
from apps.agents.blackboard import Blackboard

if __name__ == "__main__":
    bb = Blackboard()
    res = handle("Give me a 5-day forecast and latest news for the first available symbol", TOOL_REGISTRY, bb)
    print(res)
