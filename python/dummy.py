from typing import TypedDict, ParamSpecKwargs


class UserParams(TypedDict):
    name: str
    age: int
    email: str

def create_user(**kwargs: UserParams) -> None:
    # kwargs will be a dict with specific keys and types
    pass

create_user(name = "a")
