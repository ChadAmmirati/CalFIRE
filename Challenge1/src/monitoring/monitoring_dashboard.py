"""
CalFIRE Production Monitoring Dashboard
Comprehensive real-time monitoring with latency and fidelity metrics
Built with Streamlit for production deployment
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, List, Optional, Any
import yaml
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CalFIREProductionDashboard:
    """Production monitoring dashboard for CalFIRE data pipeline"""
    
    def __init__(self):
        self.config = self._load_config()
        self.databricks_config = self.config.get('databricks', {})
        self.pipeline_config = self.config.get('pipeline', {})
        self.monitoring_config = self.config.get('monitoring', {})
        
        # Initialize session state
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        if 'auto_refresh' not in st.session_state:
            st.session_state.auto_refresh = True
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML files"""
        try:
            config_path = Path(__file__).parent.parent.parent / "config"
            
            # Load databricks config
            with open(config_path / "databricks_config.yaml", 'r') as f:
                databricks_config = yaml.safe_load(f)
            
            # Load pipeline config
            with open(config_path / "pipeline_config.yaml", 'r') as f:
                pipeline_config = yaml.safe_load(f)
            
            return {
                'databricks': databricks_config,
                'pipeline': pipeline_config,
                'monitoring': pipeline_config.get('monitoring', {})
            }
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            return {}
    
    def render_dashboard(self):
        """Render the complete monitoring dashboard"""
        st.set_page_config(
            page_title="CalFIRE Production Dashboard",
            page_icon="🔥",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Header
        self._render_header()
        
        # Sidebar
        self._render_sidebar()
        
        # Main content
        self._render_main_content()
        
        # Auto-refresh
        if st.session_state.auto_refresh:
            time.sleep(30)  # Refresh every 30 seconds
            st.rerun()
    
    def _render_header(self):
        """Render dashboard header"""
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.title("🔥 CalFIRE Production Dashboard")
            st.markdown("**Real-time monitoring of wildfire data ingestion pipeline**")
        
        with col2:
            st.metric(
                label="Pipeline Status",
                value="🟢 RUNNING",
                delta="Active"
            )
        
        with col3:
            last_refresh = st.session_state.last_refresh.strftime("%H:%M:%S")
            st.metric(
                label="Last Refresh",
                value=last_refresh,
                delta="30s ago"
            )
    
    def _render_sidebar(self):
        """Render sidebar with controls"""
        st.sidebar.title("🎛️ Dashboard Controls")
        
        # Auto-refresh toggle
        st.session_state.auto_refresh = st.sidebar.checkbox(
            "Auto-refresh (30s)",
            value=st.session_state.auto_refresh
        )
        
        # Manual refresh button
        if st.sidebar.button("🔄 Refresh Now"):
            st.session_state.last_refresh = datetime.now()
            st.rerun()
        
        # Time range selector
        st.sidebar.subheader("📅 Time Range")
        time_range = st.sidebar.selectbox(
            "Select time range",
            ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"],
            index=2
        )
        
        # Data source selector
        st.sidebar.subheader("📊 Data Sources")
        selected_sources = st.sidebar.multiselect(
            "Select data sources to monitor",
            ["Fire Perimeters", "Damage Inspection", "Fire Incidents", "Weather Data"],
            default=["Fire Perimeters", "Damage Inspection", "Fire Incidents"]
        )
        
        # Alert settings
        st.sidebar.subheader("🚨 Alert Settings")
        alert_threshold = st.sidebar.slider(
            "Error Rate Threshold (%)",
            min_value=1,
            max_value=20,
            value=5
        )
        
        latency_threshold = st.sidebar.slider(
            "Latency Threshold (seconds)",
            min_value=60,
            max_value=600,
            value=300
        )
        
        # Store selections in session state
        st.session_state.time_range = time_range
        st.session_state.selected_sources = selected_sources
        st.session_state.alert_threshold = alert_threshold
        st.session_state.latency_threshold = latency_threshold
    
    def _render_main_content(self):
        """Render main dashboard content"""
        # Key Metrics Row
        self._render_key_metrics()
        
        # Charts Row 1: Latency and Throughput
        col1, col2 = st.columns(2)
        with col1:
            self._render_latency_chart()
        with col2:
            self._render_throughput_chart()
        
        # Charts Row 2: Data Quality and Error Analysis
        col1, col2 = st.columns(2)
        with col1:
            self._render_data_quality_chart()
        with col2:
            self._render_error_analysis_chart()
        
        # Data Sources Status
        self._render_data_sources_status()
        
        # Recent Alerts
        self._render_recent_alerts()
        
        # Pipeline Performance
        self._render_pipeline_performance()
    
    def _render_key_metrics(self):
        """Render key performance metrics"""
        st.subheader("📊 Key Performance Metrics")
        
        # Get metrics data (simulated for demo)
        metrics = self._get_key_metrics()
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                label="Records Processed (24h)",
                value=f"{metrics['records_processed']:,}",
                delta=f"+{metrics['records_delta']:,}"
            )
        
        with col2:
            st.metric(
                label="Avg Processing Latency",
                value=f"{metrics['avg_latency']:.1f}s",
                delta=f"{metrics['latency_delta']:.1f}s"
            )
        
        with col3:
            st.metric(
                label="Data Quality Score",
                value=f"{metrics['quality_score']:.1f}%",
                delta=f"{metrics['quality_delta']:.1f}%"
            )
        
        with col4:
            st.metric(
                label="Error Rate",
                value=f"{metrics['error_rate']:.2f}%",
                delta=f"{metrics['error_delta']:.2f}%"
            )
        
        with col5:
            st.metric(
                label="Active Data Sources",
                value=metrics['active_sources'],
                delta=f"+{metrics['sources_delta']}"
            )
    
    def _render_latency_chart(self):
        """Render latency monitoring chart"""
        st.subheader("⏱️ Processing Latency")
        
        # Get latency data (simulated)
        latency_data = self._get_latency_data()
        
        fig = go.Figure()
        
        # Add latency lines for different data sources
        for source, data in latency_data.items():
            fig.add_trace(go.Scatter(
                x=data['timestamp'],
                y=data['latency'],
                mode='lines+markers',
                name=source,
                line=dict(width=2)
            ))
        
        # Add threshold line
        threshold = st.session_state.get('latency_threshold', 300)
        fig.add_hline(
            y=threshold,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Threshold: {threshold}s"
        )
        
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Latency (seconds)",
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_throughput_chart(self):
        """Render throughput monitoring chart"""
        st.subheader("📈 Data Throughput")
        
        # Get throughput data (simulated)
        throughput_data = self._get_throughput_data()
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=throughput_data['timestamp'],
            y=throughput_data['records_per_minute'],
            name='Records/Minute',
            marker_color='lightblue'
        ))
        
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Records per Minute",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_data_quality_chart(self):
        """Render data quality monitoring chart"""
        st.subheader("✅ Data Quality Score")
        
        # Get quality data (simulated)
        quality_data = self._get_quality_data()
        
        fig = go.Figure()
        
        # Add quality score line
        fig.add_trace(go.Scatter(
            x=quality_data['timestamp'],
            y=quality_data['quality_score'],
            mode='lines+markers',
            name='Overall Quality',
            line=dict(color='green', width=3)
        ))
        
        # Add threshold line
        threshold = 80.0
        fig.add_hline(
            y=threshold,
            line_dash="dash",
            line_color="orange",
            annotation_text=f"Threshold: {threshold}%"
        )
        
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Quality Score (%)",
            height=400,
            yaxis=dict(range=[0, 100])
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_error_analysis_chart(self):
        """Render error analysis chart"""
        st.subheader("🚨 Error Analysis")
        
        # Get error data (simulated)
        error_data = self._get_error_data()
        
        fig = go.Figure()
        
        # Error types pie chart
        fig.add_trace(go.Pie(
            labels=error_data['error_types'],
            values=error_data['error_counts'],
            hole=0.3
        ))
        
        fig.update_layout(
            height=400,
            showlegend=True
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_data_sources_status(self):
        """Render data sources status"""
        st.subheader("📡 Data Sources Status")
        
        # Get data sources status (simulated)
        sources_status = self._get_sources_status()
        
        for source, status in sources_status.items():
            col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
            
            with col1:
                st.write(f"**{source}**")
            
            with col2:
                status_icon = "🟢" if status['status'] == 'healthy' else "🔴"
                st.write(f"{status_icon} {status['status'].title()}")
            
            with col3:
                st.write(f"📊 {status['records_processed']:,}")
            
            with col4:
                st.write(f"⏱️ {status['last_update']}")
    
    def _render_recent_alerts(self):
        """Render recent alerts"""
        st.subheader("🚨 Recent Alerts")
        
        # Get recent alerts (simulated)
        alerts = self._get_recent_alerts()
        
        if alerts:
            for alert in alerts:
                severity_color = {
                    'critical': '🔴',
                    'warning': '🟡',
                    'info': '🔵'
                }
                
                with st.expander(f"{severity_color.get(alert['severity'], '⚪')} {alert['title']}"):
                    st.write(f"**Time:** {alert['timestamp']}")
                    st.write(f"**Source:** {alert['source']}")
                    st.write(f"**Message:** {alert['message']}")
                    if alert.get('action_required'):
                        st.warning(f"**Action Required:** {alert['action_required']}")
        else:
            st.success("✅ No recent alerts")
    
    def _render_pipeline_performance(self):
        """Render pipeline performance metrics"""
        st.subheader("⚡ Pipeline Performance")
        
        # Get performance data (simulated)
        performance_data = self._get_performance_data()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # CPU and Memory usage
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('CPU Usage', 'Memory Usage'),
                vertical_spacing=0.1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=performance_data['timestamp'],
                    y=performance_data['cpu_usage'],
                    mode='lines',
                    name='CPU %'
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=performance_data['timestamp'],
                    y=performance_data['memory_usage'],
                    mode='lines',
                    name='Memory %'
                ),
                row=2, col=1
            )
            
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Storage usage
            storage_data = performance_data['storage_usage']
            
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=storage_data['current'],
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Storage Usage (%)"},
                delta={'reference': storage_data['previous']},
                gauge={
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 80], 'color': "yellow"},
                        {'range': [80, 100], 'color': "red"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Data simulation methods (replace with real data in production)
    def _get_key_metrics(self) -> Dict[str, Any]:
        """Get key performance metrics (simulated)"""
        return {
            'records_processed': 1250000,
            'records_delta': 15000,
            'avg_latency': 45.2,
            'latency_delta': -5.3,
            'quality_score': 94.7,
            'quality_delta': 2.1,
            'error_rate': 0.8,
            'error_delta': -0.2,
            'active_sources': 4,
            'sources_delta': 0
        }
    
    def _get_latency_data(self) -> Dict[str, Dict]:
        """Get latency data (simulated)"""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=24),
            end=datetime.now(),
            freq='1H'
        )
        
        return {
            'Fire Perimeters': {
                'timestamp': timestamps,
                'latency': [30 + i * 0.5 + (i % 3) * 10 for i in range(len(timestamps))]
            },
            'Damage Inspection': {
                'timestamp': timestamps,
                'latency': [25 + i * 0.3 + (i % 2) * 8 for i in range(len(timestamps))]
            },
            'Fire Incidents': {
                'timestamp': timestamps,
                'latency': [20 + i * 0.2 + (i % 4) * 5 for i in range(len(timestamps))]
            }
        }
    
    def _get_throughput_data(self) -> Dict[str, List]:
        """Get throughput data (simulated)"""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=24),
            end=datetime.now(),
            freq='1H'
        )
        
        return {
            'timestamp': timestamps,
            'records_per_minute': [1000 + i * 50 + (i % 5) * 200 for i in range(len(timestamps))]
        }
    
    def _get_quality_data(self) -> Dict[str, List]:
        """Get quality data (simulated)"""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=24),
            end=datetime.now(),
            freq='1H'
        )
        
        return {
            'timestamp': timestamps,
            'quality_score': [90 + i * 0.1 + (i % 3) * 2 for i in range(len(timestamps))]
        }
    
    def _get_error_data(self) -> Dict[str, List]:
        """Get error data (simulated)"""
        return {
            'error_types': ['Schema Validation', 'Data Type Mismatch', 'Missing Values', 'Coordinate Out of Bounds'],
            'error_counts': [15, 8, 12, 5]
        }
    
    def _get_sources_status(self) -> Dict[str, Dict]:
        """Get data sources status (simulated)"""
        return {
            'Fire Perimeters': {
                'status': 'healthy',
                'records_processed': 450000,
                'last_update': '2 min ago'
            },
            'Damage Inspection': {
                'status': 'healthy',
                'records_processed': 12500,
                'last_update': '5 min ago'
            },
            'Fire Incidents': {
                'status': 'healthy',
                'records_processed': 8500,
                'last_update': '1 min ago'
            },
            'Weather Data': {
                'status': 'warning',
                'records_processed': 2500,
                'last_update': '15 min ago'
            }
        }
    
    def _get_recent_alerts(self) -> List[Dict]:
        """Get recent alerts (simulated)"""
        return [
            {
                'title': 'High Latency Detected',
                'severity': 'warning',
                'timestamp': '2024-01-15 14:30:00',
                'source': 'Fire Perimeters',
                'message': 'Processing latency exceeded 300 seconds',
                'action_required': 'Check compute resources'
            },
            {
                'title': 'Data Quality Below Threshold',
                'severity': 'critical',
                'timestamp': '2024-01-15 13:45:00',
                'source': 'Damage Inspection',
                'message': 'Data quality score dropped to 75%',
                'action_required': 'Review data validation rules'
            }
        ]
    
    def _get_performance_data(self) -> Dict[str, Any]:
        """Get performance data (simulated)"""
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=24),
            end=datetime.now(),
            freq='1H'
        )
        
        return {
            'timestamp': timestamps,
            'cpu_usage': [60 + i * 0.5 + (i % 4) * 10 for i in range(len(timestamps))],
            'memory_usage': [45 + i * 0.3 + (i % 3) * 8 for i in range(len(timestamps))],
            'storage_usage': {
                'current': 75.5,
                'previous': 72.3
            }
        }

def main():
    """Main function to run the dashboard"""
    dashboard = CalFIREProductionDashboard()
    dashboard.render_dashboard()

if __name__ == "__main__":
    main()
