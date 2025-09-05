"""
CalFIRE Error Handling and Validation Framework
Comprehensive error handling, data quality assurance, and fault tolerance
"""

import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import pandas as pd
from functools import wraps
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class ErrorAction(Enum):
    """Actions to take when errors occur"""
    RETRY = "retry"
    QUARANTINE = "quarantine"
    SKIP = "skip"
    FAIL = "fail"
    ALERT = "alert"

@dataclass
class ValidationRule:
    """Data validation rule definition"""
    name: str
    description: str
    rule_expression: str
    severity: ErrorSeverity
    action: ErrorAction
    retry_count: int = 0
    quarantine_table: Optional[str] = None

@dataclass
class ErrorRecord:
    """Error record for tracking and analysis"""
    error_id: str
    timestamp: datetime
    source: str
    error_type: str
    error_message: str
    severity: ErrorSeverity
    affected_records: int
    stack_trace: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)

@dataclass
class RetryConfig:
    """Retry configuration for failed operations"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True

class DataQualityValidator:
    """Comprehensive data quality validation framework"""
    
    def __init__(self, rules: List[ValidationRule]):
        self.rules = {rule.name: rule for rule in rules}
        self.validation_results = []
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_dataframe(self, df: pd.DataFrame, source_name: str) -> Dict[str, Any]:
        """Validate a DataFrame against all rules"""
        validation_results = {
            'source': source_name,
            'timestamp': datetime.now(),
            'total_records': len(df),
            'passed_records': 0,
            'failed_records': 0,
            'rule_results': {},
            'quarantined_records': pd.DataFrame(),
            'quality_score': 0.0
        }
        
        passed_mask = pd.Series([True] * len(df), index=df.index)
        
        for rule_name, rule in self.rules.items():
            try:
                rule_result = self._apply_rule(df, rule, passed_mask)
                validation_results['rule_results'][rule_name] = rule_result
                
                # Update passed mask based on rule results
                if rule_result['action'] == ErrorAction.QUARANTINE:
                    passed_mask = passed_mask & rule_result['passed_mask']
                
            except Exception as e:
                self.logger.error(f"Error applying rule {rule_name}: {str(e)}")
                validation_results['rule_results'][rule_name] = {
                    'status': 'error',
                    'error': str(e),
                    'passed_records': 0,
                    'failed_records': len(df)
                }
        
        # Calculate final metrics
        validation_results['passed_records'] = passed_mask.sum()
        validation_results['failed_records'] = len(df) - passed_mask.sum()
        validation_results['quality_score'] = (
            validation_results['passed_records'] / validation_results['total_records'] * 100
            if validation_results['total_records'] > 0 else 0
        )
        
        # Get quarantined records
        if not passed_mask.all():
            validation_results['quarantined_records'] = df[~passed_mask].copy()
        
        self.validation_results.append(validation_results)
        return validation_results
    
    def _apply_rule(self, df: pd.DataFrame, rule: ValidationRule, current_mask: pd.Series) -> Dict[str, Any]:
        """Apply a single validation rule"""
        try:
            # Evaluate the rule expression
            passed_mask = df.eval(rule.rule_expression)
            
            # Apply current mask to maintain consistency across rules
            passed_mask = passed_mask & current_mask
            
            result = {
                'rule_name': rule.name,
                'description': rule.description,
                'severity': rule.severity.value,
                'action': rule.action.value,
                'passed_records': passed_mask.sum(),
                'failed_records': (~passed_mask).sum(),
                'passed_mask': passed_mask,
                'status': 'success'
            }
            
            # Log rule results
            if result['failed_records'] > 0:
                self.logger.warning(
                    f"Rule '{rule.name}' failed for {result['failed_records']} records "
                    f"(severity: {rule.severity.value})"
                )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error evaluating rule '{rule.name}': {str(e)}")
            return {
                'rule_name': rule.name,
                'status': 'error',
                'error': str(e),
                'passed_records': 0,
                'failed_records': len(df)
            }

class ErrorHandler:
    """Centralized error handling and management"""
    
    def __init__(self, quarantine_table: str = "calfire.quarantine.errors"):
        self.quarantine_table = quarantine_table
        self.error_log = []
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def handle_error(self, error: Exception, context: Dict[str, Any]) -> ErrorRecord:
        """Handle and log an error"""
        error_record = ErrorRecord(
            error_id=f"ERR_{int(time.time())}_{len(self.error_log)}",
            timestamp=datetime.now(),
            source=context.get('source', 'unknown'),
            error_type=type(error).__name__,
            error_message=str(error),
            severity=self._determine_severity(error, context),
            affected_records=context.get('affected_records', 0),
            stack_trace=traceback.format_exc(),
            context=context
        )
        
        self.error_log.append(error_record)
        self._log_error(error_record)
        
        return error_record
    
    def _determine_severity(self, error: Exception, context: Dict[str, Any]) -> ErrorSeverity:
        """Determine error severity based on error type and context"""
        if isinstance(error, (ValueError, TypeError)):
            return ErrorSeverity.MEDIUM
        elif isinstance(error, (ConnectionError, TimeoutError)):
            return ErrorSeverity.HIGH
        elif isinstance(error, (MemoryError, SystemError)):
            return ErrorSeverity.CRITICAL
        else:
            return ErrorSeverity.LOW
    
    def _log_error(self, error_record: ErrorRecord):
        """Log error to appropriate destinations"""
        log_message = (
            f"Error {error_record.error_id}: {error_record.error_type} - "
            f"{error_record.error_message} (Source: {error_record.source}, "
            f"Severity: {error_record.severity.value})"
        )
        
        if error_record.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(log_message)
        elif error_record.severity == ErrorSeverity.HIGH:
            self.logger.error(log_message)
        elif error_record.severity == ErrorSeverity.MEDIUM:
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)
    
    def get_error_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get error summary for the specified time period"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        recent_errors = [e for e in self.error_log if e.timestamp >= cutoff_time]
        
        summary = {
            'total_errors': len(recent_errors),
            'errors_by_severity': {},
            'errors_by_source': {},
            'errors_by_type': {},
            'time_range': f"Last {hours_back} hours"
        }
        
        for error in recent_errors:
            # Count by severity
            severity = error.severity.value
            summary['errors_by_severity'][severity] = summary['errors_by_severity'].get(severity, 0) + 1
            
            # Count by source
            source = error.source
            summary['errors_by_source'][source] = summary['errors_by_source'].get(source, 0) + 1
            
            # Count by type
            error_type = error.error_type
            summary['errors_by_type'][error_type] = summary['errors_by_type'].get(error_type, 0) + 1
        
        return summary

class RetryManager:
    """Manage retry logic for failed operations"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def retry_operation(self, func: Callable, *args, **kwargs) -> Any:
        """Retry a function with exponential backoff"""
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt == self.config.max_retries:
                    self.logger.error(f"Operation failed after {self.config.max_retries} retries: {str(e)}")
                    raise e
                
                delay = self._calculate_delay(attempt)
                self.logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay:.2f}s: {str(e)}")
                time.sleep(delay)
        
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt"""
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        if self.config.jitter:
            # Add random jitter to prevent thundering herd
            import random
            delay *= (0.5 + random.random() * 0.5)
        
        return delay

def retry_on_failure(config: RetryConfig = None):
    """Decorator for automatic retry on failure"""
    if config is None:
        config = RetryConfig()
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_manager = RetryManager(config)
            return retry_manager.retry_operation(func, *args, **kwargs)
        return wrapper
    return decorator

class FaultToleranceManager:
    """Manage fault tolerance across the pipeline"""
    
    def __init__(self):
        self.error_handler = ErrorHandler()
        self.retry_config = RetryConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute_with_fault_tolerance(self, func: Callable, context: Dict[str, Any], *args, **kwargs) -> Any:
        """Execute function with comprehensive fault tolerance"""
        try:
            # Try to execute the function
            return func(*args, **kwargs)
            
        except Exception as e:
            # Handle the error
            error_record = self.error_handler.handle_error(e, context)
            
            # Determine if we should retry
            if self._should_retry(e, context):
                self.logger.info(f"Retrying operation after error: {error_record.error_id}")
                retry_manager = RetryManager(self.retry_config)
                return retry_manager.retry_operation(func, *args, **kwargs)
            else:
                # Don't retry, handle gracefully
                self._handle_non_retryable_error(error_record, context)
                raise e
    
    def _should_retry(self, error: Exception, context: Dict[str, Any]) -> bool:
        """Determine if an error should trigger a retry"""
        # Don't retry for certain error types
        non_retryable_errors = (ValueError, TypeError, KeyError, AttributeError)
        if isinstance(error, non_retryable_errors):
            return False
        
        # Don't retry if we've already retried too many times
        retry_count = context.get('retry_count', 0)
        if retry_count >= self.retry_config.max_retries:
            return False
        
        return True
    
    def _handle_non_retryable_error(self, error_record: ErrorRecord, context: Dict[str, Any]):
        """Handle errors that shouldn't be retried"""
        # Log the error
        self.logger.error(f"Non-retryable error: {error_record.error_id}")
        
        # Quarantine data if applicable
        if 'data' in context:
            self._quarantine_data(context['data'], error_record)
        
        # Send alerts for critical errors
        if error_record.severity == ErrorSeverity.CRITICAL:
            self._send_critical_alert(error_record)
    
    def _quarantine_data(self, data: pd.DataFrame, error_record: ErrorRecord):
        """Quarantine problematic data"""
        try:
            # Add error metadata to the data
            data['error_id'] = error_record.error_id
            data['error_timestamp'] = error_record.timestamp
            data['error_type'] = error_record.error_type
            data['quarantine_reason'] = error_record.error_message
            
            # In a real implementation, you would write to the quarantine table
            self.logger.info(f"Quarantined {len(data)} records due to error {error_record.error_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to quarantine data: {str(e)}")
    
    def _send_critical_alert(self, error_record: ErrorRecord):
        """Send alert for critical errors"""
        alert_message = (
            f"CRITICAL ERROR ALERT\n"
            f"Error ID: {error_record.error_id}\n"
            f"Source: {error_record.source}\n"
            f"Type: {error_record.error_type}\n"
            f"Message: {error_record.error_message}\n"
            f"Time: {error_record.timestamp}\n"
            f"Affected Records: {error_record.affected_records}"
        )
        
        # In a real implementation, you would send this via email, Slack, etc.
        self.logger.critical(f"CRITICAL ALERT: {alert_message}")

# Predefined validation rules for CalFIRE data
def create_calfire_validation_rules() -> List[ValidationRule]:
    """Create predefined validation rules for CalFIRE data"""
    rules = [
        ValidationRule(
            name="valid_coordinates",
            description="Validate that coordinates are within California bounds",
            rule_expression="latitude.between(32.5, 42.0) & longitude.between(-124.5, -114.0)",
            severity=ErrorSeverity.HIGH,
            action=ErrorAction.QUARANTINE,
            quarantine_table="calfire.quarantine.invalid_coordinates"
        ),
        ValidationRule(
            name="valid_fire_year",
            description="Validate that fire year is reasonable",
            rule_expression="fire_year.between(1950, 2025)",
            severity=ErrorSeverity.MEDIUM,
            action=ErrorAction.QUARANTINE,
            quarantine_table="calfire.quarantine.invalid_years"
        ),
        ValidationRule(
            name="valid_acres",
            description="Validate that acres is non-negative",
            rule_expression="acres >= 0",
            severity=ErrorSeverity.MEDIUM,
            action=ErrorAction.QUARANTINE,
            quarantine_table="calfire.quarantine.invalid_acres"
        ),
        ValidationRule(
            name="required_fire_name",
            description="Validate that fire name is not null or empty",
            rule_expression="fire_name.notna() & (fire_name != '')",
            severity=ErrorSeverity.HIGH,
            action=ErrorAction.QUARANTINE,
            quarantine_table="calfire.quarantine.missing_names"
        ),
        ValidationRule(
            name="valid_damage_level",
            description="Validate damage level values",
            rule_expression="damage_level.isin(['MINOR', 'MODERATE', 'MAJOR', 'DESTROYED', 'UNKNOWN'])",
            severity=ErrorSeverity.LOW,
            action=ErrorAction.QUARANTINE,
            quarantine_table="calfire.quarantine.invalid_damage"
        )
    ]
    return rules

# Example usage
if __name__ == "__main__":
    # Create validation rules
    rules = create_calfire_validation_rules()
    validator = DataQualityValidator(rules)
    
    # Create fault tolerance manager
    fault_manager = FaultToleranceManager()
    
    # Example data
    sample_data = pd.DataFrame({
        'fire_name': ['Test Fire', 'Another Fire', ''],
        'fire_year': [2024, 2023, 2020],
        'acres': [100.5, -50.0, 200.0],
        'latitude': [37.7749, 45.0, 35.0],
        'longitude': [-122.4194, -120.0, -120.0],
        'damage_level': ['MINOR', 'INVALID', 'MAJOR']
    })
    
    # Validate data
    print("Validating sample data...")
    results = validator.validate_dataframe(sample_data, "test_source")
    
    print(f"Validation Results:")
    print(f"Total Records: {results['total_records']}")
    print(f"Passed Records: {results['passed_records']}")
    print(f"Failed Records: {results['failed_records']}")
    print(f"Quality Score: {results['quality_score']:.1f}%")
    
    # Show rule results
    for rule_name, rule_result in results['rule_results'].items():
        print(f"\nRule: {rule_name}")
        print(f"  Passed: {rule_result['passed_records']}")
        print(f"  Failed: {rule_result['failed_records']}")
        print(f"  Status: {rule_result['status']}")
    
    # Show quarantined records
    if not results['quarantined_records'].empty:
        print(f"\nQuarantined Records ({len(results['quarantined_records'])}):")
        print(results['quarantined_records'])
