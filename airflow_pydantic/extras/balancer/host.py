from logging import getLogger
from typing import TYPE_CHECKING

from pydantic import Field

from ...core import BaseModel
from ...utils import Pool, Variable

if TYPE_CHECKING:
    from airflow_pydantic.airflow import SSHHook


__all__ = ("Host",)

_log = getLogger(__name__)

# Stand-in for a Variable password that cannot be resolved offline. Rendering always
# replaces it with a Variable.get call, so it must never reach a generated DAG.
UNRESOLVED_PASSWORD = "<unresolved airflow-pydantic variable>"


class Host(BaseModel):
    name: str
    username: str | None = None

    # Password
    password: str | Variable | None = None

    # Or get key file
    key_file: str | None = None

    os: str | None = None

    # Airflow / balance
    pool: Pool | None = None
    size: int | None = None
    queues: list[str] = Field(default_factory=list)

    tags: list[str] = Field(default_factory=list)

    def override(self, **kwargs) -> "Host":
        return Host(**{**self.model_dump(), **kwargs})

    def hook(self, username: str | None = None, use_local: bool = True, **hook_kwargs) -> "SSHHook":
        from airflow_pydantic.airflow import SSHHook

        def _hook(**kwargs) -> "SSHHook":
            hook = SSHHook(**kwargs)
            if self.key_file is None and "key_file" not in hook_kwargs:
                hook.key_file = None
            return hook

        if use_local and not self.name.count(".") > 0:
            name = f"{self.name}.local"
        else:
            name = self.name
        username = username or self.username
        if username and self.password:
            if isinstance(self.password, str):
                return _hook(remote_host=name, username=username, password=self.password, **hook_kwargs)
            try:
                password = self.password.get()
            except Exception:  # noqa: BLE001
                # No Airflow metadata database available, e.g. when generating DAG files offline.
                # Rendering rewrites this into a Variable.get call, so the value itself is unused.
                _log.info("Could not resolve variable %s, falling back to an unresolved password", self.password.key)
                password = UNRESOLVED_PASSWORD
            if isinstance(password, dict):
                # TODO
                # Assume "password"
                password = password["password"]
            return _hook(remote_host=name, username=username, password=password, **hook_kwargs)
        elif username and self.key_file:
            return _hook(remote_host=name, username=username, key_file=self.key_file, **hook_kwargs)
        elif username:
            return _hook(remote_host=name, username=username, **hook_kwargs)
        else:
            return _hook(remote_host=name, **hook_kwargs)

    def __lt__(self, other):
        return self.name < other.name

    def __le__(self, other):
        return self.name <= other.name

    def __hash__(self):
        return hash(self.name)
