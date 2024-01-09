import re
from typing import Optional


def safe_name(text: Optional[str]) -> str:
    """Sanitizes a human-readable "friendly" name to a safe string.

    For example, "Joe's Collection" becomes "joe_s_collection".

    Args:
        text (Optional[str]): Unsafe text with non-underscore symbols and spaces.

    Returns:
        str: Sanitized lowercase string with underscores.
    """
    return re.sub(r"[^\w]", "_", text or "").lower()


def safe_description(text: Optional[str]) -> str:
    """Sanitizes a human-readable long text, such as description.

    Args:
        text (Optional[str]): Unsafe long text with Jinja syntax.

    Returns:
        str: Sanitized string with escaped Jinja syntax.
    """
    return re.sub(r"{{(.*)}}", r"\1", text or "")
