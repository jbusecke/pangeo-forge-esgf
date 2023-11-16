from .recipe_inputs import get_urls_from_esgf
import logging
import backoff #noqa #https://github.com/litl/backoff/issues/71

logging.getLogger('backoff').setLevel(logging.FATAL) 
# not sure if this is needed, but I want to avoid the many backoff messages

def setup_logging(level: str = "INFO"):
    """A convenience function that sets up logging for developing and debugging recipes in Jupyter,
    iPython, or another interactive context.

    :param level: One of (in decreasing level of detail) ``"DEBUG"``, ``"INFO"``, or ``"WARNING"``.
      Defaults to ``"INFO"``.
    """
    import logging

    try:
        from rich.logging import RichHandler

        handler = RichHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
    except ImportError:
        import sys

        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))

    logger = logging.getLogger("pangeo_forge_esgf")
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(getattr(logging, level))
    logger.addHandler(handler)
