from abc import ABC, abstractmethod


class BaseAgent:
    def __init__(self, name): self.name = name

    @abstractmethod
    def run(self, **kwargs):
        raise NotImplementedError