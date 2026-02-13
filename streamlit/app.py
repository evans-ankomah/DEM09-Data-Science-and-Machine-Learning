"""
Flight Fare Prediction - Streamlit App
=======================================
Live prediction interface using the trained ML model.
"""

import os
import sys
import streamlit as st
import pandas as pd
import numpy as np
import joblib

# Add project root to path
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# ============================================
# Configuration
# ============================================
MODEL_PATH = os.path.join(_PROJECT_ROOT, 'models', 'best_model.pkl')
METRICS_PATH = os.path.join(_PROJECT_ROOT, 'output', 'metrics', 'metrics.csv')
FIGURES_DIR = os.path.join(_PROJECT_ROOT, 'output', 'figures')


@st.cache_resource
def load_model():
    """Load the trained model artifact."""
    if not os.path.exists(MODEL_PATH):
        return None
    return joblib.load(MODEL_PATH)


@st.cache_data
def load_metrics():
    """Load model metrics."""
    if not os.path.exists(METRICS_PATH):
        return None
    return pd.read_csv(METRICS_PATH)


# ============================================
# Page Configuration
# ============================================
st.set_page_config(
    page_title="Flight Fare Prediction",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# Header
# ============================================
st.markdown('<p class="main-header">✈️ Flight Fare Prediction</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Bangladesh Domestic Flight Price Estimator</p>', unsafe_allow_html=True)

# Load model
artifact = load_model()
metrics_df = load_metrics()

if artifact is None:
    st.error(
        "⚠️ No trained model found. Please run the ML training pipeline first.\n\n"
        "Run: `docker compose -f docker/docker-compose.yml up --build` "
        "and trigger the Airflow DAG."
    )
    st.stop()

model = artifact['model']
model_name = artifact['model_name']
scaler = artifact['scaler']
feature_names = artifact['feature_names']
model_metrics = artifact['metrics']

# ============================================
# Sidebar - Model Info
# ============================================
with st.sidebar:
    st.header("📊 Model Info")
    st.success(f"**Active Model:** {model_name}")

    st.metric("R² Score", f"{model_metrics.get('r2', 0):.4f}")
    st.metric("MAE", f"{model_metrics.get('mae', 0):,.2f} BDT")
    st.metric("RMSE", f"{model_metrics.get('rmse', 0):,.2f} BDT")

    st.divider()
    st.header("ℹ️ About")
    st.info(
        "This app predicts flight fares for Bangladesh domestic flights "
        "using machine learning. The model is trained on historical "
        "flight price data through an automated ELT pipeline."
    )

# ============================================
# Main Content - Prediction Form
# ============================================

tab1, tab2, tab3 = st.tabs(["🔮 Predict Fare", "📈 Model Performance", "📊 Analysis"])

with tab1:
    st.header("Enter Flight Details")

    col1, col2, col3 = st.columns(3)

    with col1:
        airline = st.selectbox("Airline", [
            "Biman Bangladesh Airlines", "US-Bangla Airlines",
            "Novo Air", "Air Astra", "Regent Airways"
        ])
        source = st.selectbox("Source", [
            "DAC", "CGP", "ZYL", "RJH", "SPD", "JSR", "BZL"
        ])
        destination = st.selectbox("Destination", [
            "CGP", "DAC", "ZYL", "RJH", "SPD", "JSR", "BZL", "CXB"
        ])

    with col2:
        flight_class = st.selectbox("Class", ["Economy", "Business", "First Class"])
        stopovers = st.selectbox("Stopovers", ["Direct", "1 Stop", "2 Stops"])
        booking_source = st.selectbox("Booking Source", [
            "Online", "Travel Agent", "Airline Website", "Mobile App"
        ])

    with col3:
        seasonality = st.selectbox("Season", [
            "Regular", "Eid", "Hajj", "Winter Holidays"
        ])
        duration_hrs = st.slider("Duration (hours)", 0.5, 12.0, 1.5, 0.5)
        days_before = st.slider("Days Before Departure", 1, 90, 14)

    col4, col5 = st.columns(2)
    with col4:
        dep_month = st.selectbox("Departure Month", list(range(1, 13)),
                                  format_func=lambda x: [
                                      "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
                                  ][x-1])
    with col5:
        dep_hour = st.slider("Departure Hour", 0, 23, 10)

    dep_weekday = st.selectbox("Departure Day", list(range(7)),
                                format_func=lambda x: [
                                    "Monday", "Tuesday", "Wednesday",
                                    "Thursday", "Friday", "Saturday", "Sunday"
                                ][x])

    # Build input DataFrame
    if st.button("🔮 Predict Fare", use_container_width=True, type="primary"):
        # Create duration bucket
        if duration_hrs <= 3:
            dur_bucket = 'Short (0-3h)'
        elif duration_hrs <= 6:
            dur_bucket = 'Medium (3-6h)'
        else:
            dur_bucket = 'Long (6+h)'

        # Create booking lead bucket
        if days_before <= 3:
            lead_bucket = 'Last Minute (0-3 days)'
        elif days_before <= 14:
            lead_bucket = 'Short Notice (4-14 days)'
        elif days_before <= 30:
            lead_bucket = 'Standard (15-30 days)'
        else:
            lead_bucket = 'Early Bird (30+ days)'

        input_data = {
            'airline': airline,
            'source': source,
            'destination': destination,
            'class': flight_class,
            'stopovers': stopovers,
            'booking_source': booking_source,
            'seasonality': seasonality,
            'duration_bucket': dur_bucket,
            'booking_lead_bucket': lead_bucket,
            'aircraft_type': 'Boeing 737',
            'duration_hrs': duration_hrs,
            'days_before_departure': days_before,
            'departure_month': dep_month,
            'departure_weekday': dep_weekday,
            'departure_hour': dep_hour,
        }
        input_df = pd.DataFrame([input_data])

        # One-hot encode
        categorical = [
            'airline', 'source', 'destination', 'class', 'stopovers',
            'booking_source', 'seasonality', 'duration_bucket',
            'booking_lead_bucket', 'aircraft_type'
        ]
        input_encoded = pd.get_dummies(input_df, columns=categorical, dtype=int)

        # Scale numerical features
        numerical = [
            'duration_hrs', 'days_before_departure',
            'departure_month', 'departure_weekday', 'departure_hour'
        ]
        if scaler is not None:
            num_cols = [c for c in numerical if c in input_encoded.columns]
            if num_cols:
                input_encoded[num_cols] = scaler.transform(input_encoded[num_cols])

        # Align with training features
        for col in feature_names:
            if col not in input_encoded.columns:
                input_encoded[col] = 0
        input_encoded = input_encoded[feature_names]

        # Predict
        prediction = model.predict(input_encoded)[0]

        st.divider()
        st.success(f"## Predicted Fare: **{prediction:,.2f} BDT**")
        st.caption(f"Approximately **{prediction/120:,.2f} USD** (at 1 USD = 120 BDT)")

        # Show confidence info
        st.info(
            f"📊 Model: **{model_name}** | "
            f"R²: **{model_metrics.get('r2', 0):.4f}** | "
            f"Typical error: ±**{model_metrics.get('mae', 0):,.0f} BDT**"
        )

with tab2:
    st.header("Model Performance Comparison")

    if metrics_df is not None:
        # Display metrics table
        st.dataframe(
            metrics_df.style.format({
                'r2': '{:.4f}',
                'mae': '{:,.2f}',
                'rmse': '{:,.2f}',
            }).highlight_max(subset=['r2'], color='#90EE90')
             .highlight_min(subset=['mae', 'rmse'], color='#90EE90'),
            use_container_width=True
        )

        # Bar charts
        col1, col2 = st.columns(2)
        with col1:
            st.bar_chart(metrics_df.set_index('model')['r2'])
            st.caption("R² Score (higher is better)")
        with col2:
            st.bar_chart(metrics_df.set_index('model')['mae'])
            st.caption("MAE (lower is better)")
    else:
        st.warning("No metrics file found. Run the training pipeline first.")

    # Show plots if available
    comparison_plot = os.path.join(FIGURES_DIR, 'model_comparison.png')
    if os.path.exists(comparison_plot):
        st.image(comparison_plot, caption="Model Comparison")

with tab3:
    st.header("Data Analysis Visualizations")

    # Show available plots
    if os.path.exists(FIGURES_DIR):
        plots = [f for f in os.listdir(FIGURES_DIR) if f.endswith('.png')]
        if plots:
            cols = st.columns(2)
            for i, plot_file in enumerate(sorted(plots)):
                with cols[i % 2]:
                    st.image(
                        os.path.join(FIGURES_DIR, plot_file),
                        caption=plot_file.replace('_', ' ').replace('.png', '').title()
                    )
        else:
            st.warning("No plots found. Run the training pipeline first.")
    else:
        st.warning("Output directory not found.")
