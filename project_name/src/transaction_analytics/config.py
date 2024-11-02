# src/transaction_analytics/config.py

from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class EnvironmentConfig:
    """Configuration for different environments"""
    spark_shuffle_partitions: int
    checkpoint_base_path: str
    output_table_name: str
    
class Config:
    """Global configuration settings"""
    
    DEV = EnvironmentConfig(
        spark_shuffle_partitions=10,
        checkpoint_base_path="/tmp/dev/checkpoints",
        output_table_name="dev_transaction_analytics"
    )
    
    PROD = EnvironmentConfig(
        spark_shuffle_partitions=200,
        checkpoint_base_path="/tmp/prod/checkpoints",
        output_table_name="prod_transaction_analytics"
    )
    
    @staticmethod
    def get_fraud_detection_params() -> Dict[str, Any]:
        return {
            'fraud_threshold_multiplier': 3,
            'max_transactions_per_hour': 10,
            'quality_threshold': 0.01
        }