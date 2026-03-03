from dataclasses import dataclass


@dataclass
class Person:
    """人员信息数据模型"""
    name: str
    role: str

    def to_dict(self):
        return {"name": self.name, "role": self.role}

    @classmethod
    def from_dict(cls, data: dict):
        return cls(name=data["name"], role=data["role"])

    def __str__(self):
        return f"{self.role}:{self.name}"
