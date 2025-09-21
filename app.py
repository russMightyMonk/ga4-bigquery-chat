# app.py
# pylint: disable=broad-exception-caught

import os
import json
import hashlib
from datetime import datetime, timedelta, timezone

import streamlit as st
from google import genai
from google.cloud import bigquery
from google.genai.types import FunctionDeclaration, GenerateContentConfig, Part, Tool

from query_template_library import QUERY_TEMPLATE_LIBRARY

st.set_page_config(page_title="Speak with GA4 v1", layout="wide")

# ------------------------------------------------------------------------------
# Config (env-driven; safe defaults)
# ------------------------------------------------------------------------------
MODEL_ID = os.getenv("MODEL_ID", "gemini-2.5-pro")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")
GA4_DATASET = os.getenv("GA4_BIGQUERY_DATASET", "")

# Simple Auth (optional)
SIMPLE_AUTH_USERNAME = os.getenv("SIMPLE_AUTH_USERNAME")
SIMPLE_AUTH_PASSWORD_HASH = os.getenv("SIMPLE_AUTH_PASSWORD_HASH")

# Fail fast on missing critical configuration
if not GA4_DATASET:
    st.error("Missing env var GA4_BIGQUERY_DATASET (e.g., analytics_123456789). Set it in Cloud Run.")
    st.stop()


# ------------------------------------------------------------------------------
# Simple Authentication
# ------------------------------------------------------------------------------
def check_password():
    """Returns `True` if the user has entered the correct password."""
    # If auth is not configured, just proceed.
    if not SIMPLE_AUTH_USERNAME or not SIMPLE_AUTH_PASSWORD_HASH:
        return True

    # Check if we're already logged in
    if st.session_state.get("password_correct", False):
        return True

    # Show a login form
    with st.form("login"):
        st.header("Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            # Hash the entered password
            password_hash = hashlib.sha256(password.encode()).hexdigest()
            
            if username == SIMPLE_AUTH_USERNAME and password_hash == SIMPLE_AUTH_PASSWORD_HASH:
                st.session_state["password_correct"] = True
                st.rerun()  # Rerun the app to show the main content
            else:
                st.error("The username or password you have entered is invalid.")
    return False


# ------------------------------------------------------------------------------
# Tools (Function Calling) — GA4-aware params
# ------------------------------------------------------------------------------
execute_template_query_func = FunctionDeclaration(
    name="execute_template_query",
    description=(
        "Executes a GA4 BigQuery template. Use this to answer user questions about GA4 data."
    ),
    parameters={
        "type": "object",
        "properties": {
            "template_name": {
                "type": "string",
                "description": "One of the available GA4 query template names.",
            },
            "parameters": {
                "type": "object",
                "description": (
                    "Template parameters. Common: start_date/end_date (YYYYMMDD), top_n, "
                    "and other specific filters like event_name, property_key, or country_name."
                ),
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "YYYYMMDD. Defaults to 7 days ago.",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "YYYYMMDD. Defaults to yesterday.",
                    },
                    "property_key": {
                        "type": "string",
                        "description": "The key of the user property to analyze (e.g., 'user_tier'). Used by templates like 'extract_specific_user_property'.",
                    },
                    "event_name": {
                        "type": "string",
                        "description": "The name of the event to analyze (e.g., 'purchase', 'page_view'). Used by templates like 'calculate_events_per_user' or 'analyze_specific_event_details'.",
                    },
                    "country_name": {
                        "type": "string",
                        "description": "The full name of a country for analysis (e.g., 'United States'). Used by 'analyze_specific_country'.",
                    },
                    "campaign_name": {
                        "type": "string",
                        "description": "The name of a marketing campaign for analysis. Used by 'analyze_specific_campaign'.",
                    },
                },
            },
        },
        "required": ["template_name", "parameters"],
    },
)

query_tool = Tool(function_declarations=[execute_template_query_func])


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def get_template_descriptions() -> str:
    lines = []
    for name, details in QUERY_TEMPLATE_LIBRARY.items():
        desc = details.get("description", "").strip()
        lines.append(f"- `{name}`: {desc}")
    return "\n".join(lines)


def execute_bq_query(sql: str, bq_client: bigquery.Client):
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=10_000_000_000) # 10 GB
    query_job = bq_client.query(sql, job_config=job_config)
    rows = query_job.result()
    return [dict(row.items()) for row in rows]


def default_dates():
    today = datetime.now(timezone.utc).date()
    end_date = today - timedelta(days=1)
    start_date = today - timedelta(days=8)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def build_system_prompt(project_id: str, ga4_dataset: str) -> str:
    today_iso = datetime.now(timezone.utc).date().isoformat()
    return f"""
You are a Google Analytics 4 BigQuery expert assistant. Your goal is to answer user questions by selecting the correct GA4 query template and parameters.

Plan:
1) Analyze the user's question (intent, metrics, dimensions, filters, time range).
2) Choose the best template from the list.
3) Extract parameters:
   - Dates in YYYYMMDD (handle phrases like "yesterday", "last 7 days", "this month").
   - If missing, default to the last 8 days (inclusive).
4) Call execute_template_query with template_name and parameters.
5) After receiving results, produce a concise answer grounded ONLY in the returned data.

Available templates:
{get_template_descriptions()}

Rules:
- Today's date (UTC): {today_iso}
- Project: {project_id}
- GA4 dataset: {ga4_dataset}
- Use only provided data when summarizing (no fabrication).
""".strip()

# ------------------------------------------------------------------------------
# Main App Logic
# ------------------------------------------------------------------------------
# Call the authentication check and store the result.
is_authenticated = check_password()

# Initialize clients and run the main app only if authenticated.
if is_authenticated:
    try:
        bq_client = bigquery.Client()
        PROJECT_ID = bq_client.project
        genai_client = genai.Client(vertexai=True, location=VERTEX_LOCATION, project=PROJECT_ID)
    except Exception as e:
        st.error(f"Failed to initialize Google Cloud clients: {e}")
        st.stop()

    st.title("Speak with GA4 v1")

    with st.expander("About", expanded=False):
        st.write("""
        - Ask natural language questions about your GA4 BigQuery export data.
        - Gemini routes your question to a specialized SQL template and extracts parameters.
        - The app securely runs the query in your BigQuery project and returns the data.
        - Gemini summarizes the results into a concise, easy-to-read answer for you.
        """)
    
    with st.sidebar:
        st.header("⚙️ Configuration")
        st.markdown("""
        This app is connected to the following BigQuery resources. 
        These values are set via environment variables during deployment.
        """)
        st.success(f"**Project ID:** `{PROJECT_ID}`")
        st.success(f"**GA4 Dataset:** `{GA4_DATASET}`")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
            if "details" in m and m["details"]:
                with st.expander("Execution Details"):
                    st.json(m["details"])

    if user_prompt := st.chat_input("Ask about your GA4 data..."):
        st.session_state.messages.append({"role": "user", "content": user_prompt})
        with st.chat_message("user"):
            st.markdown(user_prompt)

        with st.chat_message("assistant"):
            try:
                with st.spinner("Thinking..."):
                    system_prompt = build_system_prompt(PROJECT_ID, GA4_DATASET)
                    full_prompt = f"{system_prompt}\nUser question: {user_prompt}"

                    chat = genai_client.chats.create(
                        model=MODEL_ID,
                        config=GenerateContentConfig(temperature=0, tools=[query_tool]),
                    )

                    response = chat.send_message(full_prompt)
                    part = response.candidates[0].content.parts[0]

                    backend_details = {}
                    final_answer = ""

                    if part.function_call and part.function_call.name == "execute_template_query":
                        fc_args = dict(part.function_call.args.items())
                        template_name = fc_args.get("template_name")
                        params = fc_args.get("parameters", {}) or {}

                        if not template_name or template_name not in QUERY_TEMPLATE_LIBRARY:
                            raise ValueError(f"Invalid template selected by model: {template_name}")

                        sql_template = QUERY_TEMPLATE_LIBRARY[template_name]["template"]
                        start_def, end_def = default_dates()
                        final_params = {
                            "project_id": PROJECT_ID,
                            "dataset_id": GA4_DATASET,
                            "start_date": (params.get("start_date") or start_def),
                            "end_date": (params.get("end_date") or end_def),
                        }
                        
                        # Add any other params from the template library
                        template_specific = ["event_name", "country_name", "property_key", "campaign_name"]
                        for param in template_specific:
                            if param in params:
                                final_params[param] = params[param]

                        try:
                            final_sql = sql_template.format(**final_params)
                        except KeyError as ke:
                            raise ValueError(f"Template missing parameter: {ke}") from ke

                        backend_details = {
                            "chosen_template": template_name,
                            "extracted_parameters": params,
                            "final_parameters": final_params,
                            "generated_sql": final_sql,
                        }

                        with st.spinner(f"Querying BigQuery with '{template_name}'..."):
                            rows = execute_bq_query(final_sql, bq_client)
                            backend_details["query_results_preview"] = rows[:5]
                            api_response_json = json.dumps(rows, ensure_ascii=False, default=str)

                        response2 = chat.send_message(
                            Part.from_function_response(
                                name="execute_template_query",
                                response={"content": api_response_json},
                            )
                        )
                        final_answer = response2.candidates[0].content.parts[0].text
                    else:
                        final_answer = getattr(part, "text", "I couldn't map this to a template. Try rephrasing with a time range.")

                st.markdown(final_answer)
                with st.expander("Execution Details"):
                    st.json(backend_details)

                st.session_state.messages.append(
                    {"role": "assistant", "content": final_answer, "details": backend_details}
                )

            except Exception as e:
                msg = f"An error occurred: {e}"
                st.error(msg)
                st.session_state.messages.append({"role": "assistant", "content": msg})
