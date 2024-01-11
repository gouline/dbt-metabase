from typing import Optional, Sequence


class MetabaseArgumentError(ValueError):
    """Invalid Metabase arguments supplied."""


class MetabaseRuntimeError(RuntimeError):
    """Metabase execution failed."""


class Filter:
    """Inclusion/exclusion filtering."""

    def __init__(
        self,
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
    ):
        """
        Args:
            include (Optional[Sequence[str]], optional): Optional inclusions (i.e. include only these). Defaults to None.
            exclude (Optional[Sequence[str]], optional): Optional exclusion list (i.e. exclude these, even if in inclusion list). Defaults to None.
        """
        self.include = [self._norm(x) for x in include or []]
        self.exclude = [self._norm(x) for x in exclude or []]

    def selected(self, item: str) -> bool:
        item = self._norm(item)
        included = not self.include or item in self.include
        excluded = self.exclude and item in self.exclude
        return included and not excluded

    @classmethod
    def _norm(cls, x: str) -> str:
        return x.upper()
