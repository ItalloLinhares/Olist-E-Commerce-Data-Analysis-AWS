import structlog
import logging
import sys
from structlog.processors import CallsiteParameterAdder, CallsiteParameter

def setup_logging(log_level="INFO"):
    """Configure the structured record for the project"""
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(message)s", # Structlog envia a mensagem já formatada
        stream=sys.stdout,
    )

    structlog.configure(
        processors=[
            # Filtra logs por nível antes de processar
            structlog.stdlib.filter_by_level,
            # Adiciona metadados básicos
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            # ESTE É O CARA: Extrai info de onde o log foi chamado
            structlog.processors.CallsiteParameterAdder(
                {
                    CallsiteParameter.FILENAME,
                    CallsiteParameter.FUNC_NAME,
                    CallsiteParameter.LINENO,
                    CallsiteParameter.MODULE,
                }
            ),
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True
    )
    
    return structlog.get_logger()