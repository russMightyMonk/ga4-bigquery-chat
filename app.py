# app.py
# pylint: disable=broad-exception-caught

import os
import json
from datetime import datetime, timedelta

import streamlit as st
from google import genai
from google.cloud import bigquery
from google.genai.types import FunctionDeclaration, GenerateContentConfig, Part, Tool

from query_template_library import QUERY_TEMPLATE_LIBRARY


# ------------------------------------------------------------------------------
# Config (env-driven; safe defaults)
# ------------------------------------------------------------------------------
MODEL_ID = os.getenv("MODEL_ID", "gemini-2.5-pro")             # same family as original
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")  # regional Vertex endpoint
GA4_DATASET = os.getenv("GA4_BIGQUERY_DATASET", "")            # e.g. analytics_XXXXXXXX

# Fail fast on missing critical configuration
if not GA4_DATASET:
    st.error("Missing env var GA4_BIGQUERY_DATASET (e.g., analytics_123456789). Set it in Cloud Run.")
    st.stop()


# ------------------------------------------------------------------------------
# Clients
# ------------------------------------------------------------------------------
# BigQuery client (auto picks up credentials from env)
bq_client = bigquery.Client()
PROJECT_ID = bq_client.project
# GenAI client
genai_client = genai.Client(vertexai=True, location=VERTEX_LOCATION, project=PROJECT_ID)


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


def execute_bq_query(sql: str):
    job_config = bigquery.QueryJobConfig(maximum_bytes_billed=30_000_000_000)  # 30 GB cap - Change here if you need more data to be processed
    query_job = bq_client.query(sql, job_config=job_config)
    rows = query_job.result()
    return [dict(row.items()) for row in rows]


def default_dates():
    """
    Returns default date range for GA4 queries.
    GA4 BigQuery exports have ~1 day lag, so 'yesterday' is the latest available data.
    Returns 7 days of data ending yesterday.
    """
    from datetime import timezone
    
    today = datetime.now(timezone.utc).date()
    end_date = today - timedelta(days=1)      # Yesterday (latest GA4 data)
    start_date = today - timedelta(days=8)    # 8 days ago (gives us 7 days inclusive)
    
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def build_system_prompt() -> str:
    today_iso = datetime.now(datetime.timezone.utc).date().isoformat()
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
- Project: {PROJECT_ID}
- GA4 dataset: {GA4_DATASET}
- Use only provided data when summarizing (no fabrication).
""".strip()

# ------------------------------------------------------------------------------
# UI
# ------------------------------------------------------------------------------
st.set_page_config(page_title="Speak with GA4 (Chat Router)", layout="wide")
st.title("Speak with GA4 (Chat Router)")

with st.expander("About", expanded=True):
    st.write("""
    - Ask natural language questions about your GA4 BigQuery export data.
    - Gemini routes your question to a specialized SQL template and extracts parameters.
    - The app securely runs the query in your BigQuery project and returns the data.
    - Gemini summarizes the results into a concise, easy-to-read answer for you.
    """)
    st.markdown("---")
    st.markdown("**Example Questions to Get Real Value:**")
    st.info(
        """
        - **Acquisition:** "What were my top 5 acquisition channels for new users last month?"
        - **E-commerce:** "Show me the top 10 best-selling products by revenue this quarter."
        - **Engagement:** "Compare user engagement rates on mobile vs. desktop for the last 30 days."
        - **Campaigns:** "Which marketing campaigns drove the most revenue in May?"
        - **User Behavior:** "What are the most common pages users exit from?"
        - **Geography:** "Analyze user performance and engagement in the United States vs. Canada."
        """
    )

with st.sidebar:
    st.header("Connection")
    st.text_input("Project ID", value=PROJECT_ID, disabled=True)
    st.text_input("GA4 Dataset", value=GA4_DATASET, disabled=True)

# Chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
        if "details" in m:
            with st.expander("Execution Details"):
                st.json(m["details"])

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
placeholder = "Ask e.g., 'Active vs inactive users last 7 days', 'Top 10 traffic sources this month', 'LTV by currency last quarter'."
if user_prompt := st.chat_input(placeholder):
    st.session_state.messages.append({"role": "user", "content": user_prompt})
    with st.chat_message("user"):
        st.markdown(user_prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("Thinking..."):
                system_prompt = build_system_prompt()
                full_prompt = f"{system_prompt}\n\nUser question: {user_prompt}"

                chat = genai_client.chats.create(
                    model=MODEL_ID,
                    config=GenerateContentConfig(temperature=0, tools=[query_tool]),
                )

                response = chat.send_message(full_prompt)
                part = response.candidates[0].content.parts[0]

                backend_details = {}
                final_answer = ""

                if part.function_call and part.function_call.name == "execute_template_query":
                    # args is KV pairs in this SDK; normalize to dict
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
                        "top_n": params.get("top_n", 10),
                    }

                    # Render SQL
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

                    # Execute
                    with st.spinner(f"Querying BigQuery with '{template_name}'..."):
                        rows = execute_bq_query(final_sql)
                        backend_details["query_results_preview"] = rows[:5]
                        api_response_json = json.dumps(rows, ensure_ascii=False, default=str)

                    # Summarize
                    response2 = chat.send_message(
                        Part.from_function_response(
                            name="execute_template_query",
                            response={"content": api_response_json},
                        )
                    )
                    final_answer = response2.candidates[0].content.parts[0].text
                else:
                    # No function call: show model text
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