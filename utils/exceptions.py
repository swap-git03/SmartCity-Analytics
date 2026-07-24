"""
Custom Exception Classes for Smart City Traffic & Environmental Analytics Platform.

Provides a structured exception hierarchy to ensure granular error handling
and clean stack trace debugging across APIs, Streaming, Spark ETL, AWS S3, and ML modules.
"""

class TrafficPlatformException(Exception):
    """Base exception class for all platform errors."""
    def __init__(self, message: str, original_exception: Exception = None):
        super().__init__(message)
        self.message = message
        self.original_exception = original_exception

    def __str__(self):
        if self.original_exception:
            return f"{self.message} | Cause: {type(self.original_exception).__name__}: {str(self.original_exception)}"
        return self.message


class APIException(TrafficPlatformException):
    """Raised when REST API calls (TomTom, OpenWeather) fail or return non-200 status codes."""
    pass


class KafkaException(TrafficPlatformException):
    """Raised when Kafka producer initialization, message publishing, or broker connection fails."""
    pass


class SparkETLException(TrafficPlatformException):
    """Raised when PySpark stream processing, schema enforcement, or Medallion layer writes fail."""
    pass


class AWSException(TrafficPlatformException):
    """Raised when AWS S3 Boto3 operations (upload, sync, bucket checks) encounter errors."""
    pass


class MLException(TrafficPlatformException):
    """Raised during Machine Learning model training, evaluation, or inference failures."""
    pass
