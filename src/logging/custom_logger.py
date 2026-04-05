# ...existing code...
import json
import logging

from rich.logging import RichHandler


class JSONPrettyFilter(logging.Filter):
    """
    Logging filter that pretty-prints JSON-like messages (dict/list or JSON strings).
    Replaces record.msg with an indented JSON string for nicer console output.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.msg
        try:
            if isinstance(msg, (dict, list)):
                record.msg = json.dumps(msg, indent=2, sort_keys=True)
            elif isinstance(msg, str):
                s = msg.strip()
                if (s.startswith("{") and s.endswith("}")) or (
                    s.startswith("[") and s.endswith("]")
                ):
                    try:
                        obj = json.loads(s)
                        record.msg = json.dumps(obj, indent=2, sort_keys=True)
                    except Exception:
                        # not valid JSON; leave as-is
                        pass
        except Exception:
            # on any unexpected error, don't block logging
            pass
        return True


def _configured() -> bool:
    return any(isinstance(h, RichHandler) for h in logging.root.handlers)


def get_logger(
    name: str = None, log_level: str = "INFO", log_format: str = None
) -> logging.Logger:
    """
    Return a logger configured with RichHandler and colorized level styles.
    Safe to call multiple times; handler added once.
    """
    if not _configured():
        handler = RichHandler(show_time=True, show_level=True, show_path=False)
        handler.addFilter(JSONPrettyFilter())

        # optional: keep a concise formatter for non-rich consumers
        if log_format:
            fmt = log_format
        else:
            fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        handler.setFormatter(logging.Formatter(fmt))

        logging.root.setLevel(log_level)
        logging.root.addHandler(handler)

    return logging.getLogger(name)
