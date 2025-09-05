"""
CalFIRE Production Error Handling and Validation Framework
Comprehensive error handling, data quality validation, and recovery mechanisms
"""

import logging
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import traceback
import requests
from functools import wraps
import yaml
from pathlib import Path

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
    """Error handling actions"""
    LOG = "log"
    QUARANTINE = "quarantine"
    RETRY = "retry"
    FAIL = "fail"
    ALERT = "alert"

class ValidationRule:
    """Data validation rule definition"""
    
    def __init__(self, name: str, description: str, rule_expression: str, 
                 severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                 action: ErrorAction = ErrorAction.QUARANTINE,
                 custom_validator: Optional[Callable] = None):
        self.name = name
        self.description = description
        self.rule_expression = rule_expression
        self.severity = severity
        self.action = action
        self.custom_validator = custom_validator
        self.created_at = datetime.now()
        self.last_used = None
        self.success_count = 0
        self.failure_count = 0

@dataclass
class ValidationResult:
    """Result of data validation"""
    rule_name: str
    passed: bool
    failed_records: int
    total_records: int
    error_message: Optional[str] = None
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    @property
    def pass_rate(self) -> float:
        """Calculate pass rate percentage"""
        if self.total_records == 0:
            return 100.0
        return (self.passed_records / self.total_records) * 100
    
    @property
    def passed_records(self) -> int:
        """Calculate number of passed records"""
        return self.total_records - self.failed_records

@dataclass
class ErrorRecord:
    """Error record for tracking and analysis"""
    error_id: str
    error_type: str
    error_message: str
    severity: ErrorSeverity
    source_table: str
    source_record_id: Optional[str]
    timestamp: datetime
    context: Dict[str, Any]
    resolved: bool = False
    resolution_notes: Optional[str] = None
    resolved_at: Optional[datetime] = None

class DataQualityValidator:
    """Comprehensive data quality validator"""
    
    def __init__(self, rules: List[ValidationRule]):
        self.rules = {rule.name: rule for rule in rules}
        self.validation_history = []
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_dataframe(self, df: pd.DataFrame, table_name: str) -> List[ValidationResult]:
        """Validate a DataFrame against all rules"""
        results = []
        
        for rule_name, rule in self.rules.items():
            try:
                result = self._validate_rule(df, rule, table_name)
                results.append(result)
                
                # Update rule statistics
                rule.last_used = datetime.now()
                if result.passed:
                    rule.success_count += 1
                else:
                    rule.failure_count += 1
                
            except Exception as e:
                self.logger.error(f"Error validating rule {rule_name}: {str(e)}")
                results.append(ValidationResult(
                    rule_name=rule_name,
                    passed=False,
                    failed_records=len(df),
                    total_records=len(df),
                    error_message=str(e),
                    severity=ErrorSeverity.HIGH
                ))
        
        # Store validation history
        self.validation_history.append({
            'table_name': table_name,
            'timestamp': datetime.now(),
            'results': [asdict(result) for result in results]
        })
        
        return results
    
    def _validate_rule(self, df: pd.DataFrame, rule: ValidationRule, table_name: str) -> ValidationResult:
        """Validate DataFrame against a specific rule"""
        try:
            if rule.custom_validator:
                # Use custom validator function
                passed, failed_records, error_message = rule.custom_validator(df)
            else:
                # Use SQL-like expression validation
                passed, failed_records, error_message = self._validate_expression(df, rule.rule_expression)
            
            return ValidationResult(
                rule_name=rule.name,
                passed=passed,
                failed_records=failed_records,
                total_records=len(df),
                error_message=error_message,
                severity=rule.severity
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=rule.name,
                passed=False,
                failed_records=len(df),
                total_records=len(df),
                error_message=f"Validation error: {str(e)}",
                severity=ErrorSeverity.HIGH
            )
    
    def _validate_expression(self, df: pd.DataFrame, expression: str) -> tuple:
        """Validate DataFrame using SQL-like expression"""
        try:
            # Convert expression to pandas query
            # This is a simplified implementation
            if "IS NOT NULL" in expression:
                column = expression.split("IS NOT NULL")[0].strip()
                failed_records = df[df[column].isna()].shape[0]
                passed = failed_records == 0
                error_message = f"{failed_records} records have null values in {column}" if not passed else None
            elif "BETWEEN" in expression:
                # Parse BETWEEN expression
                parts = expression.split("BETWEEN")
                column = parts[0].strip()
                range_parts = parts[1].strip().split("AND")
                min_val = float(range_parts[0].strip())
                max_val = float(range_parts[1].strip())
                failed_records = df[(df[column] < min_val) | (df[column] > max_val)].shape[0]
                passed = failed_records == 0
                error_message = f"{failed_records} records have values outside range [{min_val}, {max_val}] in {column}" if not passed else None
            elif ">=" in expression:
                parts = expression.split(">=")
                column = parts[0].strip()
                threshold = float(parts[1].strip())
                failed_records = df[df[column] < threshold].shape[0]
                passed = failed_records == 0
                error_message = f"{failed_records} records have values below {threshold} in {column}" if not passed else None
            else:
                # Default validation - assume all records pass
                passed = True
                failed_records = 0
                error_message = None
            
            return passed, failed_records, error_message
            
        except Exception as e:
            return False, len(df), f"Expression validation error: {str(e)}"

class ErrorHandler:
    """Comprehensive error handling and recovery system"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.error_records = []
        self.retry_attempts = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.alerting_enabled = config.get('alerting', {}).get('enabled', True)
        self.quarantine_enabled = config.get('quarantine', {}).get('enabled', True)
    
    def handle_error(self, exception: Exception, context: Dict[str, Any]) -> ErrorRecord:
        """Handle an error and determine appropriate action"""
        error_id = f"ERR_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.error_records)}"
        
        # Determine error severity
        severity = self._determine_severity(exception, context)
        
        # Create error record
        error_record = ErrorRecord(
            error_id=error_id,
            error_type=type(exception).__name__,
            error_message=str(exception),
            severity=severity,
            source_table=context.get('table_name', 'unknown'),
            source_record_id=context.get('record_id'),
            timestamp=datetime.now(),
            context=context
        )
        
        # Store error record
        self.error_records.append(error_record)
        
        # Determine action based on severity and retry count
        action = self._determine_action(error_record)
        
        # Execute action
        self._execute_action(error_record, action)
        
        return error_record
    
    def _determine_severity(self, exception: Exception, context: Dict[str, Any]) -> ErrorSeverity:
        """Determine error severity based on exception type and context"""
        error_type = type(exception).__name__
        
        # Critical errors
        if error_type in ['ConnectionError', 'AuthenticationError', 'PermissionError']:
            return ErrorSeverity.CRITICAL
        
        # High severity errors
        if error_type in ['DataValidationError', 'SchemaError', 'DataQualityError']:
            return ErrorSeverity.HIGH
        
        # Medium severity errors
        if error_type in ['TimeoutError', 'RateLimitError', 'TemporaryError']:
            return ErrorSeverity.MEDIUM
        
        # Low severity errors
        return ErrorSeverity.LOW
    
    def _determine_action(self, error_record: ErrorRecord) -> ErrorAction:
        """Determine action based on error severity and retry count"""
        retry_count = self.retry_attempts.get(error_record.error_id, 0)
        max_retries = self.config.get('retry_policy', {}).get('max_retries', 3)
        
        # Always log the error
        self.logger.error(f"Error {error_record.error_id}: {error_record.error_message}")
        
        # Determine action based on severity
        if error_record.severity == ErrorSeverity.CRITICAL:
            return ErrorAction.ALERT
        elif error_record.severity == ErrorSeverity.HIGH:
            return ErrorAction.QUARANTINE
        elif error_record.severity == ErrorSeverity.MEDIUM and retry_count < max_retries:
            return ErrorAction.RETRY
        else:
            return ErrorAction.QUARANTINE
    
    def _execute_action(self, error_record: ErrorRecord, action: ErrorAction):
        """Execute the determined action"""
        if action == ErrorAction.RETRY:
            self._handle_retry(error_record)
        elif action == ErrorAction.QUARANTINE:
            self._handle_quarantine(error_record)
        elif action == ErrorAction.ALERT:
            self._handle_alert(error_record)
        elif action == ErrorAction.FAIL:
            self._handle_fail(error_record)
    
    def _handle_retry(self, error_record: ErrorRecord):
        """Handle retry action"""
        retry_count = self.retry_attempts.get(error_record.error_id, 0)
        self.retry_attempts[error_record.error_id] = retry_count + 1
        
        # Calculate exponential backoff delay
        base_delay = self.config.get('retry_policy', {}).get('base_delay', 60)
        max_delay = self.config.get('retry_policy', {}).get('max_delay', 300)
        delay = min(base_delay * (2 ** retry_count), max_delay)
        
        self.logger.info(f"Retrying error {error_record.error_id} in {delay} seconds (attempt {retry_count + 1})")
        time.sleep(delay)
    
    def _handle_quarantine(self, error_record: ErrorRecord):
        """Handle quarantine action"""
        if self.quarantine_enabled:
            # Move problematic data to quarantine table
            quarantine_table = f"calfire.quarantine.{error_record.source_table}"
            self.logger.warning(f"Quarantining data for error {error_record.error_id} to {quarantine_table}")
            
            # In a real implementation, you would move the data here
            # For now, we'll just log the action
    
    def _handle_alert(self, error_record: ErrorRecord):
        """Handle alert action"""
        if self.alerting_enabled:
            self._send_alert(error_record)
    
    def _handle_fail(self, error_record: ErrorRecord):
        """Handle fail action"""
        self.logger.critical(f"Pipeline failed due to error {error_record.error_id}")
        raise Exception(f"Pipeline failure: {error_record.error_message}")
    
    def _send_alert(self, error_record: ErrorRecord):
        """Send alert notification"""
        try:
            alert_config = self.config.get('alerting', {})
            
            # Email alert
            if alert_config.get('email_enabled', False):
                self._send_email_alert(error_record)
            
            # Slack alert
            if alert_config.get('slack_enabled', False):
                self._send_slack_alert(error_record)
            
            # Teams alert
            if alert_config.get('teams_enabled', False):
                self._send_teams_alert(error_record)
                
        except Exception as e:
            self.logger.error(f"Failed to send alert: {str(e)}")
    
    def _send_email_alert(self, error_record: ErrorRecord):
        """Send email alert"""
        # In a real implementation, you would use an email service
        self.logger.info(f"Email alert sent for error {error_record.error_id}")
    
    def _send_slack_alert(self, error_record: ErrorRecord):
        """Send Slack alert"""
        try:
            webhook_url = self.config.get('alerting', {}).get('slack_webhook')
            if not webhook_url:
                return
            
            message = {
                "text": f"🚨 CalFIRE Pipeline Alert",
                "attachments": [
                    {
                        "color": "danger" if error_record.severity == ErrorSeverity.CRITICAL else "warning",
                        "fields": [
                            {"title": "Error ID", "value": error_record.error_id, "short": True},
                            {"title": "Severity", "value": error_record.severity.value, "short": True},
                            {"title": "Source", "value": error_record.source_table, "short": True},
                            {"title": "Message", "value": error_record.error_message, "short": False},
                            {"title": "Time", "value": error_record.timestamp.isoformat(), "short": True}
                        ]
                    }
                ]
            }
            
            response = requests.post(webhook_url, json=message, timeout=10)
            response.raise_for_status()
            
        except Exception as e:
            self.logger.error(f"Failed to send Slack alert: {str(e)}")
    
    def _send_teams_alert(self, error_record: ErrorRecord):
        """Send Teams alert"""
        # In a real implementation, you would use Teams webhook
        self.logger.info(f"Teams alert sent for error {error_record.error_id}")

class RetryDecorator:
    """Decorator for automatic retry with exponential backoff"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(self.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == self.max_retries:
                        logger.error(f"Function {func.__name__} failed after {self.max_retries} retries")
                        raise e
                    
                    # Calculate delay with exponential backoff
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}), retrying in {delay}s")
                    time.sleep(delay)
            
            raise last_exception
        
        return wrapper

class DataQualityMonitor:
    """Monitor data quality metrics over time"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.quality_history = []
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def record_quality_metrics(self, table_name: str, validation_results: List[ValidationResult]):
        """Record quality metrics for a table"""
        total_records = sum(result.total_records for result in validation_results)
        failed_records = sum(result.failed_records for result in validation_results)
        quality_score = ((total_records - failed_records) / total_records * 100) if total_records > 0 else 100
        
        quality_record = {
            'timestamp': datetime.now(),
            'table_name': table_name,
            'total_records': total_records,
            'failed_records': failed_records,
            'quality_score': quality_score,
            'validation_results': [asdict(result) for result in validation_results]
        }
        
        self.quality_history.append(quality_record)
        
        # Check if quality score is below threshold
        threshold = self.config.get('quality_threshold', 80.0)
        if quality_score < threshold:
            self.logger.warning(f"Data quality below threshold for {table_name}: {quality_score:.2f}% < {threshold}%")
    
    def get_quality_trend(self, table_name: str, hours_back: int = 24) -> pd.DataFrame:
        """Get quality trend for a specific table"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        table_history = [
            record for record in self.quality_history
            if record['table_name'] == table_name and record['timestamp'] >= cutoff_time
        ]
        
        if not table_history:
            return pd.DataFrame()
        
        return pd.DataFrame(table_history)

# Predefined validation rules for CalFIRE data
def create_calfire_validation_rules() -> List[ValidationRule]:
    """Create predefined validation rules for CalFIRE data"""
    rules = [
        # Fire Perimeters validation rules
        ValidationRule(
            name="valid_fire_year",
            description="Fire year must be between 1950 and 2025",
            rule_expression="FIRE_YEAR BETWEEN 1950 AND 2025",
            severity=ErrorSeverity.HIGH,
            action=ErrorAction.QUARANTINE
        ),
        ValidationRule(
            name="valid_acres",
            description="Acres must be non-negative",
            rule_expression="ACRES >= 0",
            severity=ErrorSeverity.MEDIUM,
            action=ErrorAction.QUARANTINE
        ),
        ValidationRule(
            name="valid_coordinates",
            description="Coordinates must be within California bounds",
            rule_expression="LATITUDE BETWEEN 32.5 AND 42.0 AND LONGITUDE BETWEEN -124.5 AND -114.0",
            severity=ErrorSeverity.HIGH,
            action=ErrorAction.QUARANTINE
        ),
        ValidationRule(
            name="required_fire_name",
            description="Fire name is required",
            rule_expression="FIRE_NAME IS NOT NULL",
            severity=ErrorSeverity.HIGH,
            action=ErrorAction.QUARANTINE
        ),
        
        # Damage Inspection validation rules
        ValidationRule(
            name="valid_inspection_date",
            description="Inspection date is required",
            rule_expression="INSPECTION_DATE IS NOT NULL",
            severity=ErrorSeverity.HIGH,
            action=ErrorAction.QUARANTINE
        ),
        ValidationRule(
            name="valid_damage_level",
            description="Damage level is required",
            rule_expression="DAMAGE_LEVEL IS NOT NULL",
            severity=ErrorSeverity.MEDIUM,
            action=ErrorAction.QUARANTINE
        ),
        
        # Custom validation rules
        ValidationRule(
            name="fire_duration_validation",
            description="Fire duration must be reasonable",
            rule_expression="",
            severity=ErrorSeverity.MEDIUM,
            action=ErrorAction.QUARANTINE,
            custom_validator=lambda df: _validate_fire_duration(df)
        )
    ]
    
    return rules

def _validate_fire_duration(df: pd.DataFrame) -> tuple:
    """Custom validator for fire duration"""
    try:
        if 'CONT_DATE' in df.columns and 'ALARM_DATE' in df.columns:
            # Calculate duration in days
            duration = (pd.to_datetime(df['CONT_DATE']) - pd.to_datetime(df['ALARM_DATE'])).dt.days
            
            # Check for unreasonable durations (more than 365 days or negative)
            invalid_duration = (duration > 365) | (duration < 0)
            failed_records = invalid_duration.sum()
            
            passed = failed_records == 0
            error_message = f"{failed_records} records have invalid fire duration" if not passed else None
            
            return passed, failed_records, error_message
        else:
            return True, 0, None
    except Exception as e:
        return False, len(df), f"Duration validation error: {str(e)}"

# Example usage and testing
def main():
    """Example usage of the error handling framework"""
    
    # Load configuration
    config_path = Path(__file__).parent.parent.parent / "config" / "pipeline_config.yaml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create error handler
    error_handler = ErrorHandler(config)
    
    # Create data quality validator
    validation_rules = create_calfire_validation_rules()
    validator = DataQualityValidator(validation_rules)
    
    # Create quality monitor
    quality_monitor = DataQualityMonitor(config)
    
    # Example DataFrame for testing
    test_data = pd.DataFrame({
        'FIRE_NAME': ['Test Fire 1', 'Test Fire 2', None, 'Test Fire 4'],
        'FIRE_YEAR': [2023, 2024, 2025, 1950],
        'ACRES': [100.5, -10.0, 250.0, 500.0],
        'LATITUDE': [37.7749, 34.0522, 45.0, 32.5],
        'LONGITUDE': [-122.4194, -118.2437, -120.0, -124.5],
        'CONT_DATE': ['2023-08-15', '2024-07-20', '2025-06-10', '1950-09-05'],
        'ALARM_DATE': ['2023-08-10', '2024-07-15', '2025-06-05', '1950-09-01']
    })
    
    # Validate data
    print("Validating test data...")
    validation_results = validator.validate_dataframe(test_data, "test_fire_perimeters")
    
    # Record quality metrics
    quality_monitor.record_quality_metrics("test_fire_perimeters", validation_results)
    
    # Print results
    for result in validation_results:
        print(f"Rule: {result.rule_name}")
        print(f"  Passed: {result.passed}")
        print(f"  Pass Rate: {result.pass_rate:.2f}%")
        print(f"  Failed Records: {result.failed_records}")
        if result.error_message:
            print(f"  Error: {result.error_message}")
        print()

if __name__ == "__main__":
    main()
