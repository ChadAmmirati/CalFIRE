"""
CalFIRE Data Pipeline Monitoring Dashboard
Comprehensive monitoring solution using Databricks SQL and Unity Catalog
"""

from databricks import sql
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
import json

class CalFIREMonitoringDashboard:
    """
    Comprehensive monitoring dashboard for CalFIRE data ingestion pipeline
    Provides real-time visibility into pipeline health, data quality, and performance metrics
    """
    
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self.connection = None
        
    def connect(self):
        """Establish connection to Databricks SQL"""
        try:
            self.connection = sql.connect(
                server_hostname=self.connection_params['server_hostname'],
                http_path=self.connection_params['http_path'],
                access_token=self.connection_params['access_token']
            )
            return True
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            return False
    
    def get_pipeline_metrics(self, hours_back=24):
        """Retrieve pipeline performance metrics"""
        query = f"""
        SELECT 
            pipeline_name,
            run_id,
            start_time,
            end_time,
            status,
            records_processed,
            processing_latency_seconds,
            data_quality_score,
            error_count,
            error_rate
        FROM calfire.monitoring.pipeline_metrics
        WHERE start_time >= current_timestamp() - INTERVAL {hours_back} HOURS
        ORDER BY start_time DESC
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
    
    def get_data_quality_metrics(self, hours_back=24):
        """Retrieve data quality metrics"""
        query = f"""
        SELECT 
            table_name,
            validation_rule,
            passed_records,
            failed_records,
            total_records,
            quality_score,
            validation_timestamp
        FROM calfire.monitoring.data_quality_metrics
        WHERE validation_timestamp >= current_timestamp() - INTERVAL {hours_back} HOURS
        ORDER BY validation_timestamp DESC
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
    
    def get_ingestion_metrics(self, hours_back=24):
        """Retrieve data ingestion metrics by source"""
        query = f"""
        SELECT 
            source_type,
            source_name,
            ingestion_timestamp,
            records_ingested,
            bytes_processed,
            ingestion_latency_seconds,
            success_rate
        FROM calfire.monitoring.ingestion_metrics
        WHERE ingestion_timestamp >= current_timestamp() - INTERVAL {hours_back} HOURS
        ORDER BY ingestion_timestamp DESC
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
    
    def get_error_analysis(self, hours_back=24):
        """Retrieve error analysis and trends"""
        query = f"""
        SELECT 
            error_type,
            error_message,
            error_count,
            first_occurrence,
            last_occurrence,
            affected_tables
        FROM calfire.monitoring.error_analysis
        WHERE last_occurrence >= current_timestamp() - INTERVAL {hours_back} HOURS
        ORDER BY error_count DESC
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)

def create_dashboard():
    """Create the Streamlit monitoring dashboard"""
    st.set_page_config(
        page_title="CalFIRE Data Pipeline Monitoring",
        page_icon="🔥",
        layout="wide"
    )
    
    st.title("🔥 CalFIRE Data Pipeline Monitoring Dashboard")
    st.markdown("Real-time monitoring of wildfire data ingestion pipeline")
    
    # Connection parameters (in production, these would be stored securely)
    connection_params = {
        'server_hostname': st.secrets["databricks"]["server_hostname"],
        'http_path': st.secrets["databricks"]["http_path"],
        'access_token': st.secrets["databricks"]["access_token"]
    }
    
    dashboard = CalFIREMonitoringDashboard(connection_params)
    
    if not dashboard.connect():
        st.stop()
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    hours_back = st.sidebar.slider("Hours to look back", 1, 168, 24)
    refresh_interval = st.sidebar.selectbox("Refresh interval", [30, 60, 300], index=1)
    
    # Auto-refresh
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Main dashboard content
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "⚡ Performance", 
        "🔍 Data Quality", 
        "📥 Ingestion", 
        "🚨 Alerts"
    ])
    
    with tab1:
        st.header("Pipeline Overview")
        
        # Key metrics cards
        col1, col2, col3, col4 = st.columns(4)
        
        try:
            pipeline_metrics = dashboard.get_pipeline_metrics(hours_back)
            
            if not pipeline_metrics.empty:
                with col1:
                    total_runs = len(pipeline_metrics)
                    st.metric("Total Pipeline Runs", total_runs)
                
                with col2:
                    success_rate = (pipeline_metrics['status'] == 'SUCCESS').mean() * 100
                    st.metric("Success Rate", f"{success_rate:.1f}%")
                
                with col3:
                    avg_latency = pipeline_metrics['processing_latency_seconds'].mean()
                    st.metric("Avg Processing Latency", f"{avg_latency:.1f}s")
                
                with col4:
                    avg_quality = pipeline_metrics['data_quality_score'].mean()
                    st.metric("Avg Data Quality", f"{avg_quality:.1f}%")
                
                # Pipeline status over time
                st.subheader("Pipeline Status Over Time")
                fig = px.timeline(
                    pipeline_metrics, 
                    x_start='start_time', 
                    x_end='end_time',
                    y='pipeline_name',
                    color='status',
                    title="Pipeline Execution Timeline"
                )
                st.plotly_chart(fig, use_container_width=True)
                
            else:
                st.warning("No pipeline metrics available for the selected time range")
                
        except Exception as e:
            st.error(f"Error retrieving pipeline metrics: {str(e)}")
    
    with tab2:
        st.header("Performance Metrics")
        
        try:
            pipeline_metrics = dashboard.get_pipeline_metrics(hours_back)
            
            if not pipeline_metrics.empty:
                # Performance trends
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Processing Latency Trend")
                    fig = px.line(
                        pipeline_metrics, 
                        x='start_time', 
                        y='processing_latency_seconds',
                        title="Processing Latency Over Time"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.subheader("Records Processed")
                    fig = px.bar(
                        pipeline_metrics, 
                        x='start_time', 
                        y='records_processed',
                        title="Records Processed Per Run"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Performance distribution
                st.subheader("Performance Distribution")
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.histogram(
                        pipeline_metrics, 
                        x='processing_latency_seconds',
                        title="Latency Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    fig = px.histogram(
                        pipeline_metrics, 
                        x='data_quality_score',
                        title="Data Quality Score Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
        except Exception as e:
            st.error(f"Error retrieving performance metrics: {str(e)}")
    
    with tab3:
        st.header("Data Quality Metrics")
        
        try:
            quality_metrics = dashboard.get_data_quality_metrics(hours_back)
            
            if not quality_metrics.empty:
                # Quality score by table
                st.subheader("Data Quality by Table")
                fig = px.bar(
                    quality_metrics, 
                    x='table_name', 
                    y='quality_score',
                    title="Data Quality Score by Table"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Validation rule performance
                st.subheader("Validation Rule Performance")
                rule_performance = quality_metrics.groupby('validation_rule').agg({
                    'passed_records': 'sum',
                    'failed_records': 'sum',
                    'total_records': 'sum'
                }).reset_index()
                
                rule_performance['pass_rate'] = (
                    rule_performance['passed_records'] / rule_performance['total_records'] * 100
                )
                
                fig = px.bar(
                    rule_performance, 
                    x='validation_rule', 
                    y='pass_rate',
                    title="Validation Rule Pass Rate"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Quality trends over time
                st.subheader("Quality Trends Over Time")
                fig = px.line(
                    quality_metrics, 
                    x='validation_timestamp', 
                    y='quality_score',
                    color='table_name',
                    title="Data Quality Score Trends"
                )
                st.plotly_chart(fig, use_container_width=True)
                
            else:
                st.warning("No data quality metrics available for the selected time range")
                
        except Exception as e:
            st.error(f"Error retrieving data quality metrics: {str(e)}")
    
    with tab4:
        st.header("Data Ingestion Metrics")
        
        try:
            ingestion_metrics = dashboard.get_ingestion_metrics(hours_back)
            
            if not ingestion_metrics.empty:
                # Ingestion by source type
                st.subheader("Ingestion Volume by Source")
                fig = px.pie(
                    ingestion_metrics, 
                    values='records_ingested', 
                    names='source_type',
                    title="Records Ingested by Source Type"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Ingestion latency by source
                st.subheader("Ingestion Latency by Source")
                fig = px.box(
                    ingestion_metrics, 
                    x='source_type', 
                    y='ingestion_latency_seconds',
                    title="Ingestion Latency Distribution by Source"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Success rate trends
                st.subheader("Success Rate Trends")
                fig = px.line(
                    ingestion_metrics, 
                    x='ingestion_timestamp', 
                    y='success_rate',
                    color='source_type',
                    title="Success Rate Over Time by Source"
                )
                st.plotly_chart(fig, use_container_width=True)
                
            else:
                st.warning("No ingestion metrics available for the selected time range")
                
        except Exception as e:
            st.error(f"Error retrieving ingestion metrics: {str(e)}")
    
    with tab5:
        st.header("Alerts and Error Analysis")
        
        try:
            error_analysis = dashboard.get_error_analysis(hours_back)
            
            if not error_analysis.empty:
                # Top errors
                st.subheader("Top Errors by Count")
                fig = px.bar(
                    error_analysis.head(10), 
                    x='error_count', 
                    y='error_type',
                    orientation='h',
                    title="Top 10 Errors by Count"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Error trends
                st.subheader("Error Trends Over Time")
                error_analysis['date'] = pd.to_datetime(error_analysis['last_occurrence']).dt.date
                daily_errors = error_analysis.groupby(['date', 'error_type'])['error_count'].sum().reset_index()
                
                fig = px.line(
                    daily_errors, 
                    x='date', 
                    y='error_count',
                    color='error_type',
                    title="Daily Error Count by Type"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Error details table
                st.subheader("Error Details")
                st.dataframe(error_analysis, use_container_width=True)
                
            else:
                st.success("No errors detected in the selected time range! 🎉")
                
        except Exception as e:
            st.error(f"Error retrieving error analysis: {str(e)}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Data range: Last {hours_back} hours"
    )

# SQL queries for creating monitoring tables
MONITORING_TABLE_QUERIES = {
    "pipeline_metrics": """
    CREATE TABLE IF NOT EXISTS calfire.monitoring.pipeline_metrics (
        pipeline_name STRING,
        run_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        status STRING,
        records_processed BIGINT,
        processing_latency_seconds DOUBLE,
        data_quality_score DOUBLE,
        error_count BIGINT,
        error_rate DOUBLE
    ) USING DELTA
    PARTITIONED BY (DATE(start_time))
    """,
    
    "data_quality_metrics": """
    CREATE TABLE IF NOT EXISTS calfire.monitoring.data_quality_metrics (
        table_name STRING,
        validation_rule STRING,
        passed_records BIGINT,
        failed_records BIGINT,
        total_records BIGINT,
        quality_score DOUBLE,
        validation_timestamp TIMESTAMP
    ) USING DELTA
    PARTITIONED BY (DATE(validation_timestamp))
    """,
    
    "ingestion_metrics": """
    CREATE TABLE IF NOT EXISTS calfire.monitoring.ingestion_metrics (
        source_type STRING,
        source_name STRING,
        ingestion_timestamp TIMESTAMP,
        records_ingested BIGINT,
        bytes_processed BIGINT,
        ingestion_latency_seconds DOUBLE,
        success_rate DOUBLE
    ) USING DELTA
    PARTITIONED BY (DATE(ingestion_timestamp))
    """,
    
    "error_analysis": """
    CREATE TABLE IF NOT EXISTS calfire.monitoring.error_analysis (
        error_type STRING,
        error_message STRING,
        error_count BIGINT,
        first_occurrence TIMESTAMP,
        last_occurrence TIMESTAMP,
        affected_tables ARRAY<STRING>
    ) USING DELTA
    PARTITIONED BY (DATE(last_occurrence))
    """
}

if __name__ == "__main__":
    create_dashboard()
