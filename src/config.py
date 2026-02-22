"""
Centralized configuration for ETL pipeline

This module contains all configuration parameters for the project,
including AWS settings, data validation rules, and processing options.

Usage:
    from src.config import Config, get_config
    
    config = Config()
    print(config.S3_INPUT_BUCKET)
    
    # Or for environment-specific config:
    config = get_config('production')
"""

import os
from typing import Dict, List, Optional
import validations.validate_customers, validations.validate_orders, validate_order_items

FUNCTION_REGISTRY = {
    "olist_customers_dataset.csv": validations.validate_customers
    "olist_orders_dataset.csv": validations.validate_orders.validate_customers_and_get_clean_data
    "olist_geolocation_dataset": null,
    "olist_order_items_dataset": validate_order_items.validate_order_items

}


class Config:
    """
    Base configuration class for ETL pipeline
    
    All configuration values can be overridden by environment variables.
    Priority: Environment Variables > Class Attributes > Defaults
    """
    
    # ============================================================
    # AWS CONFIGURATION
    # ============================================================
    
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    AWS_PROFILE = os.getenv('AWS_PROFILE', 'default')
    AWS_ACCOUNT_ID = os.getenv('AWS_ACCOUNT_ID', '')
    
    # ============================================================
    # S3 CONFIGURATION
    # ============================================================
    
    # S3 Buckets
    S3_INPUT_BUCKET = os.getenv('S3_INPUT_BUCKET', 'olist-etl-raw')
    S3_OUTPUT_BUCKET = os.getenv('S3_OUTPUT_BUCKET', 'olist-etl-processed')
    S3_REJECTED_BUCKET = os.getenv('S3_REJECTED_BUCKET', 'olist-etl-rejected')
    S3_SCRIPTS_BUCKET = os.getenv('S3_SCRIPTS_BUCKET', 'olist-etl-scripts')
    
    # S3 Prefixes (folders)
    S3_INPUT_PREFIX = os.getenv('S3_INPUT_PREFIX', 'raw/')
    S3_OUTPUT_PREFIX = os.getenv('S3_OUTPUT_PREFIX', 'processed/')
    S3_REJECTED_PREFIX = os.getenv('S3_REJECTED_PREFIX', 'rejected/')
    S3_TEMP_PREFIX = os.getenv('S3_TEMP_PREFIX', 'temp/')
    
    # ============================================================
    # FILE PROCESSING CONFIGURATION
    # ============================================================
    
    # Supported file extensions
    SUPPORTED_FILE_EXTENSIONS = ['.csv', '.parquet']
    
    # CSV Configuration
    CSV_ENCODING = 'utf-8'
    CSV_DELIMITER = ','
    CSV_QUOTE_CHAR = '"'
    CSV_ESCAPE_CHAR = '\\'
    
    # Parquet Configuration
    PARQUET_COMPRESSION = os.getenv('PARQUET_COMPRESSION', 'snappy')
    PARQUET_ENGINE = 'pyarrow'
    
    # Compression options: snappy, gzip, lzo, brotli, lz4, zstd
    # Recommendation: snappy (best balance of speed/compression)
    
    # ============================================================
    # SPARK CONFIGURATION
    # ============================================================
    
    SPARK_APP_NAME = 'OlistETL'
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    
    # Spark Shuffle Partitions
    # Default: 200 (too many for small datasets)
    # Recommendation: 2-4 per CPU core
    SPARK_SHUFFLE_PARTITIONS = int(os.getenv('SPARK_SHUFFLE_PARTITIONS', '8'))
    
    # Spark Memory Configuration
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
    SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '2g')
    SPARK_EXECUTOR_CORES = int(os.getenv('SPARK_EXECUTOR_CORES', '2'))
    
    # Spark Serialization
    SPARK_SERIALIZER = 'org.apache.spark.serializer.KryoSerializer'
    
    # Spark Dynamic Allocation (for AWS Glue)
    SPARK_DYNAMIC_ALLOCATION = os.getenv('SPARK_DYNAMIC_ALLOCATION', 'true')
    
    # ============================================================
    # GLUE CONFIGURATION
    # ============================================================
    
    GLUE_JOB_NAME = os.getenv('GLUE_JOB_NAME', 'olist-etl-job')
    GLUE_ROLE_NAME = os.getenv('GLUE_ROLE_NAME', 'GlueETLRole')
    GLUE_ROLE_ARN = os.getenv('GLUE_ROLE_ARN', '')
    
    # Glue Worker Configuration
    GLUE_WORKER_TYPE = os.getenv('GLUE_WORKER_TYPE', 'G.1X')  # G.1X, G.2X, G.4X
    GLUE_NUMBER_OF_WORKERS = int(os.getenv('GLUE_NUMBER_OF_WORKERS', '5'))
    GLUE_MAX_RETRIES = int(os.getenv('GLUE_MAX_RETRIES', '1'))
    GLUE_TIMEOUT_MINUTES = int(os.getenv('GLUE_TIMEOUT_MINUTES', '60'))
    
    # ============================================================
    # LOGGING CONFIGURATION
    # ============================================================
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = os.getenv('LOG_FORMAT', 'json')  # json or text
    
    # Log output destinations
    LOG_TO_CONSOLE = os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true'
    LOG_TO_FILE = os.getenv('LOG_TO_FILE', 'false').lower() == 'true'
    LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'logs/etl.log')
    
    # CloudWatch Logs
    CLOUDWATCH_LOG_GROUP = os.getenv('CLOUDWATCH_LOG_GROUP', '/aws/glue/jobs/olist-etl')
    CLOUDWATCH_LOG_STREAM = os.getenv('CLOUDWATCH_LOG_STREAM', 'etl-job-run')
    
    # ============================================================
    # DATA VALIDATION RULES
    # ============================================================
    
    # ID Validation
    ID_LENGTH = 32
    ID_PATTERN = r'^[a-zA-Z0-9]{32}$'
    
    # ZIP Code Validation (Brazilian format)
    ZIP_CODE_LENGTH = 5
    ZIP_CODE_PATTERN = r'^[0-9]{5}$'
    
    # Valid Brazilian States (UF codes)
    VALID_BRAZILIAN_STATES = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
        'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    # Valid Order Statuses
    VALID_ORDER_STATUSES = [
        'delivered', 'shipped', 'canceled', 'unavailable',
        'invoiced', 'processing', 'created', 'approved'
    ]
    
    # Valid Payment Types
    VALID_PAYMENT_TYPES = [
        'credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined'
    ]
    
    # Valid Product Categories
    VALID_PRODUCT_CATEGORIES = [
        'perfumaria', 'artes', 'esporte_lazer', 'bebes',
        'utilidades_domesticas', 'instrumentos_musicais', 'cool_stuff',
        'moveis_decoracao', 'eletrodomesticos', 'brinquedos',
        'cama_mesa_banho', 'construcao_ferramentas_seguranca',
        'informatica_acessorios', 'beleza_saude', 'malas_acessorios',
        'automotivo', 'papelaria', 'relogios_presentes', 'telefonia',
        'livros_tecnicos', 'livros_interesse_geral', 'alimentos_bebidas',
        'telefonia_fixa', 'industria_comercio_negocios', 'eletronicos',
        'consoles_games', 'audio', 'fashion_bolsas_e_acessorios',
        'pet_shop', 'casa_construcao', 'artigos_de_festas',
        'esporte_lazer', 'flores', 'artes_e_artesanato',
        'fraldas_higiene', 'market_place', 'fashion_calcados',
        'sinalizacao_e_seguranca', 'climatizacao', 'fashion_roupa_masculina',
        'portateis_cozinha_e_preparadores_de_alimentos',
        'casa_conforto', 'fashion_roupa_feminina', 'fashion_roupa_infanto_juvenil',
        'livros_importados', 'bebidas', 'cine_foto', 'la_cuisine',
        'pcs', 'tablets_impressao_imagem', 'artigos_de_natal',
        'fashion_underwear_e_moda_praia', 'seguros_e_servicos',
        'dvds_blu_ray', 'musica', 'fashion_esporte', 'agro_industria_e_comercio'
    ]
    
    # Date Ranges (Olist dataset specific)
    MIN_ORDER_DATE = '2016-09-01'
    MAX_ORDER_DATE = '2018-10-31'
    
    # Numeric Ranges
    MIN_PRICE = 0.0
    MAX_PRICE = 1000000.0  # R$ 1 million
    MIN_FREIGHT_VALUE = 0.0
    MAX_FREIGHT_VALUE = 10000.0  # R$ 10k
    
    # ============================================================
    # DATA QUALITY THRESHOLDS
    # ============================================================
    
    # Minimum percentage of valid records to consider processing successful
    MIN_VALID_PERCENTAGE = float(os.getenv('MIN_VALID_PERCENTAGE', '80.0'))
    
    # Maximum percentage of dropped records allowed
    MAX_DROP_RATE_PERCENTAGE = float(os.getenv('MAX_DROP_RATE_PERCENTAGE', '20.0'))
    
    # Null value thresholds
    MAX_NULL_PERCENTAGE_CRITICAL = 5.0  # Critical fields (e.g., IDs)
    MAX_NULL_PERCENTAGE_NON_CRITICAL = 30.0  # Non-critical fields
    
    # Duplicate detection
    CHECK_DUPLICATES = True
    MAX_DUPLICATE_PERCENTAGE = 1.0
    
    # ============================================================
    # FILE PROCESSOR MAPPING
    # ============================================================
    
    FILE_PROCESSOR_MAP: Dict[str, Dict] = {
        "olist_customers_dataset.csv": {
            "output_name": "olist_customers_dataset.parquet",
            "output_folder": "customers_dataset/",
            "quality_threshold": 95.0,
            "partition_by": None  # No partitioning for customers
        },
        "olist_orders_dataset.csv": {
            "output_name": "olist_orders_dataset.parquet",
            "output_folder": "orders_dataset/",
            "quality_threshold": 95.0,
            "partition_by": ["year", "month"]  # Partition by order date
        },
        "olist_products_dataset.csv": {
            "output_name": "olist_products_dataset.parquet",
            "output_folder": "products_dataset/",
            "quality_threshold": 80.0,  # Products can have more nulls
            "partition_by": ["product_category_name"]
        },
        "olist_order_items_dataset.csv": {
            "output_name": "olist_order_items_dataset.parquet",
            "output_folder": "order_items_dataset/",
            "quality_threshold": 95.0,
            "partition_by": None
        },
        "olist_sellers_dataset.csv": {
            "output_name": "olist_sellers_dataset.parquet",
            "output_folder": "sellers_dataset/",
            "quality_threshold": 95.0,
            "partition_by": ["seller_state"]
        },
        "olist_order_payments_dataset.csv": {
            "output_name": "olist_order_payments_dataset.parquet",
            "output_folder": "order_payments_dataset/",
            "quality_threshold": 95.0,
            "partition_by": ["payment_type"]
        },
        "olist_order_reviews_dataset.csv": {
            "output_name": "olist_order_reviews_dataset.parquet",
            "output_folder": "order_reviews_dataset/",
            "quality_threshold": 85.0,  # Reviews may have more nulls
            "partition_by": None
        },
        "olist_geolocation_dataset.csv": {
            "output_name": "olist_geolocation_dataset.parquet",
            "output_folder": "geolocation_dataset/",
            "quality_threshold": 90.0,
            "partition_by": ["geolocation_state"]
        }
    }
    
    # ============================================================
    # PERFORMANCE TUNING
    # ============================================================
    
    # Cache Configuration
    CACHE_DATAFRAMES = True
    CACHE_STORAGE_LEVEL = 'MEMORY_AND_DISK'  # MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY
    
    # Broadcast Join Threshold (in bytes)
    # DataFrames smaller than this will be broadcast in joins
    BROADCAST_THRESHOLD = 10 * 1024 * 1024  # 10 MB
    
    # Adaptive Query Execution (Spark 3.0+)
    ADAPTIVE_QUERY_EXECUTION = True
    
    # Coalesce partitions after filter
    AUTO_COALESCE = True
    COALESCE_THRESHOLD = 0.5  # Coalesce if > 50% of data filtered out
    
    # ============================================================
    # MONITORING & ALERTING
    # ============================================================
    
    # Enable metrics collection
    COLLECT_METRICS = os.getenv('COLLECT_METRICS', 'true').lower() == 'true'
    
    # Metrics to track
    TRACK_PROCESSING_TIME = True
    TRACK_DATA_QUALITY = True
    TRACK_COST_METRICS = True
    
    # Alert thresholds
    ALERT_ON_FAILURE = True
    ALERT_EMAIL = os.getenv('ALERT_EMAIL', '')
    ALERT_SLACK_WEBHOOK = os.getenv('ALERT_SLACK_WEBHOOK', '')
    
    # Performance alert thresholds
    MAX_PROCESSING_TIME_MINUTES = 30
    MIN_THROUGHPUT_MB_PER_SEC = 0.5
    
    # ============================================================
    # HELPER METHODS
    # ============================================================
    
    @classmethod
    def get_s3_input_path(cls) -> str:
        """
        Get full S3 input path
        
        Returns:
            Complete S3 URI for input data
            
        Example:
            >>> Config.get_s3_input_path()
            's3://olist-etl-raw/raw/'
        """
        return f"s3://{cls.S3_INPUT_BUCKET}/{cls.S3_INPUT_PREFIX}"
    
    @classmethod
    def get_s3_output_path(cls) -> str:
        """
        Get full S3 output path
        
        Returns:
            Complete S3 URI for processed data
        """
        return f"s3://{cls.S3_OUTPUT_BUCKET}/{cls.S3_OUTPUT_PREFIX}"
    
    @classmethod
    def get_s3_rejected_path(cls) -> str:
        """
        Get full S3 rejected records path
        
        Returns:
            Complete S3 URI for rejected records
        """
        return f"s3://{cls.S3_REJECTED_BUCKET}/{cls.S3_REJECTED_PREFIX}"
    
    @classmethod
    def get_s3_temp_path(cls) -> str:
        """
        Get full S3 temporary path for Glue
        
        Returns:
            Complete S3 URI for temporary data
        """
        return f"s3://{cls.S3_SCRIPTS_BUCKET}/{cls.S3_TEMP_PREFIX}"
    
    @classmethod
    def get_file_config(cls, file_name: str) -> Optional[Dict]:
        """
        Get configuration for a specific file
        
        Args:
            file_name: Name of the CSV file
            
        Returns:
            Configuration dict or None if not found
            
        Example:
            >>> config = Config.get_file_config('olist_customers_dataset.csv')
            >>> print(config['quality_threshold'])
            95.0
        """
        return cls.FILE_PROCESSOR_MAP.get(file_name)
    
    @classmethod
    def validate_config(cls) -> bool:
        """
        Validate that all required configuration is present
        
        Returns:
            True if valid, raises exception otherwise
            
        Raises:
            ValueError: If required configuration is missing
        """
        required_vars = [
            'S3_INPUT_BUCKET',
            'S3_OUTPUT_BUCKET'
        ]
        
        for var in required_vars:
            value = getattr(cls, var, None)
            if not value:
                raise ValueError(f"Missing required configuration: {var}")
        
        # Validate thresholds
        if cls.MIN_VALID_PERCENTAGE < 0 or cls.MIN_VALID_PERCENTAGE > 100:
            raise ValueError(f"MIN_VALID_PERCENTAGE must be between 0 and 100")
        
        return True
    
    @classmethod
    def to_dict(cls) -> Dict:
        """
        Export configuration as dictionary
        
        Returns:
            Dict with all configuration values
        """
        return {
            key: value
            for key, value in vars(cls).items()
            if not key.startswith('_') and not callable(value)
        }
    
    @classmethod
    def print_config(cls) -> None:
        """
        Print configuration for debugging
        """
        print("="*70)
        print("ETL CONFIGURATION")
        print("="*70)
        
        config_dict = cls.to_dict()
        
        for key, value in sorted(config_dict.items()):
            if isinstance(value, (list, dict)):
                print(f"{key}:")
                if isinstance(value, list) and len(value) > 5:
                    print(f"  [{value[0]}, {value[1]}, ... ({len(value)} items)]")
                else:
                    print(f"  {value}")
            else:
                print(f"{key}: {value}")
        
        print("="*70)


# ============================================================
# ENVIRONMENT-SPECIFIC CONFIGURATIONS
# ============================================================

class DevelopmentConfig(Config):
    """Development environment configuration"""
    
    LOG_LEVEL = 'DEBUG'
    SPARK_SHUFFLE_PARTITIONS = 4  # Fewer partitions for small local data
    GLUE_NUMBER_OF_WORKERS = 2
    COLLECT_METRICS = False
    
    # Override S3 buckets for development
    S3_INPUT_BUCKET = os.getenv('S3_INPUT_BUCKET', 'olist-etl-dev-raw')
    S3_OUTPUT_BUCKET = os.getenv('S3_OUTPUT_BUCKET', 'olist-etl-dev-processed')


class StagingConfig(Config):
    """Staging environment configuration"""
    
    LOG_LEVEL = 'INFO'
    SPARK_SHUFFLE_PARTITIONS = 8
    GLUE_NUMBER_OF_WORKERS = 3
    
    S3_INPUT_BUCKET = os.getenv('S3_INPUT_BUCKET', 'olist-etl-staging-raw')
    S3_OUTPUT_BUCKET = os.getenv('S3_OUTPUT_BUCKET', 'olist-etl-staging-processed')


class ProductionConfig(Config):
    """Production environment configuration"""
    
    LOG_LEVEL = 'INFO'
    SPARK_SHUFFLE_PARTITIONS = 16
    GLUE_NUMBER_OF_WORKERS = 10
    
    # Stricter quality thresholds for production
    MIN_VALID_PERCENTAGE = 95.0
    MAX_DROP_RATE_PERCENTAGE = 5.0
    
    # Enable all monitoring
    COLLECT_METRICS = True
    ALERT_ON_FAILURE = True
    
    S3_INPUT_BUCKET = os.getenv('S3_INPUT_BUCKET', 'olist-etl-prod-raw')
    S3_OUTPUT_BUCKET = os.getenv('S3_OUTPUT_BUCKET', 'olist-etl-prod-processed')


# ============================================================
# CONFIGURATION FACTORY
# ============================================================

def get_config(environment: Optional[str] = None) -> Config:
    """
    Get configuration based on environment
    
    Args:
        environment: Environment name ('development', 'staging', 'production')
                    If None, uses ENVIRONMENT env var or defaults to 'development'
        
    Returns:
        Configuration instance for the specified environment
        
    Example:
        >>> from src.config import get_config
        >>> config = get_config('production')
        >>> print(config.LOG_LEVEL)
        INFO
    """
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'development')
    
    configs = {
        'development': DevelopmentConfig,
        'dev': DevelopmentConfig,
        'staging': StagingConfig,
        'stage': StagingConfig,
        'production': ProductionConfig,
        'prod': ProductionConfig
    }
    
    config_class = configs.get(environment.lower(), DevelopmentConfig)
    
    # Validate configuration
    config_class.validate_config()
    
    return config_class


# ============================================================
# MODULE-LEVEL INSTANCE
# ============================================================

# Default config instance (can be imported directly)
config = get_config()


# ============================================================
# USAGE EXAMPLES
# ============================================================

if __name__ == "__main__":
    # Example 1: Use default config
    print("=== DEFAULT CONFIG ===")
    Config.print_config()
    
    # Example 2: Get environment-specific config
    print("\n=== PRODUCTION CONFIG ===")
    prod_config = get_config('production')
    print(f"Workers: {prod_config.GLUE_NUMBER_OF_WORKERS}")
    print(f"Quality threshold: {prod_config.MIN_VALID_PERCENTAGE}%")
    
    # Example 3: Get file-specific config
    print("\n=== FILE CONFIG ===")
    file_config = Config.get_file_config('olist_customers_dataset.csv')
    print(f"Output: {file_config['output_name']}")
    print(f"Threshold: {file_config['quality_threshold']}%")
    
    # Example 4: Get S3 paths
    print("\n=== S3 PATHS ===")
    print(f"Input: {Config.get_s3_input_path()}")
    print(f"Output: {Config.get_s3_output_path()}")
    print(f"Rejected: {Config.get_s3_rejected_path()}")