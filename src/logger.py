import structlog
import logging
from pythonjsonlogger import jsonlogger

def setup_loggin(log_level="INFO"):
    """Configure the structured record for the project"""
    logHandler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        '%(timestamp)s %(level)s %(name)s %(message)s'
    )
    logHandler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, log_level),
        handlers=[logHandler]
    )
    
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )