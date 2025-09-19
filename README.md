# ga4-bigquery-chat

# Speak with GA4: A Self-Deploying Streamlit App for BigQuery

This repository contains a Streamlit application that allows you to chat with your Google Analytics 4 (GA4) BigQuery export data using natural language. It's powered by Google's Gemini API, which translates your questions into SQL queries, executes them, and provides answers in plain English.

The key feature is a setup script that automates the entire deployment process, including the creation of a secure, production-ready CI/CD pipeline on Google Cloud.

## Architecture Overview

This project is designed for rapid, repeatable deployment. The workflow is as follows:

1.  A developer pushes a code change to the `main` branch of their forked GitHub repository.
2.  A Cloud Build trigger automatically starts a build process.
3.  Cloud Build builds a Docker image of the Streamlit application.
4.  The image is pushed to a private repository in Google Artifact Registry.
5.  Cloud Build deploys the new image as a revision to a Cloud Run service.
6.  The Cloud Run service is protected by Identity-Aware Proxy (IAP), ensuring only authorized users can access the application.

---

## Deployment Guide

Follow these steps to deploy the application and its CI/CD pipeline to your own Google Cloud project.

### Prerequisites

Before you begin, ensure you have the following:

1.  **GitHub Account**: To fork this repository.
2.  **GitHub Personal Access Token (PAT)**: This is required for Cloud Build to securely access your forked repository.
    *   Navigate to `https://github.com/settings/tokens`.
    *   Click **"Generate new token"** and select **"Generate new token (classic)"**.
    *   Give it a name (e.g., `gcp-cloud-build-access`).
    *   Set an expiration date.
    *   Under **"Select scopes"**, check the **`repo`** scope.
    *   Click **"Generate token"** and **copy the token immediately**. You will need it for the setup script.
3.  **Google Cloud Project**:
    *   A GCP project with **Billing enabled**.
    *   Your user account must have the `Owner` or `Editor` IAM role.

### Step 1: Fork and Clone the Repository

1.  **Fork** this repository to your own GitHub account.
2.  **(Recommended)** For security, make your forked repository private. In your forked repo, go to `Settings` -> `Danger Zone` -> `Change repository visibility` -> `Make private`.
3.  **Clone your forked repository** to your local machine:
    ```bash
    git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
    cd YOUR_REPO_NAME
    ```

### Step 2: Configure GCP OAuth Consent Screen

This is a **one-time manual step** per project. It configures the login screen that IAP presents to users.

1.  Go to the [OAuth Consent Screen page](https://console.cloud.google.com/apis/credentials/consent) in the GCP Console.
2.  Choose **Internal** (if all users are in your Google organization) or **External** (for any Google account).
3.  Fill in the required fields (App name, User support email). Click **SAVE AND CONTINUE**.
4.  You can skip adding scopes and test users.
5.  On the summary page, click **BACK TO DASHBOARD**. If you chose "External", you may need to click **PUBLISH APP**.

### Step 3: Run the Automated Setup from Cloud Shell

This is the main setup step. You will use the Google Cloud Shell to clone your repository and run a script that provisions all the necessary cloud infrastructure and sets up the CI/CD pipeline.

1. **Activate Cloud Shell**
   In the Google Cloud Console, click the **Activate Cloud Shell** icon (`>_`) in the top-right corner. This will open a terminal pre-authenticated to your GCP account.

2. **Clone Your Repository into Cloud Shell**
   Run the following command in the Cloud Shell terminal, replacing `YOUR_USERNAME` and `YOUR_REPO_NAME` with your details.

   ```bash
   git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
   ```

   > **Important**: When prompted for a password, **paste your GitHub Personal Access Token (PAT)**. Do not use your regular GitHub password.

3. **Navigate into the Project Directory**

   ```bash
   # Replace YOUR_REPO_NAME with the actual repository name
   cd YOUR_REPO_NAME
   ```

4. **Execute the Setup Script**
   The `setup.sh` script will now configure your GCP project.

   ```bash
   # First, make the script executable
   chmod +x setup.sh
   
   # Now, run the script
   ./setup.sh
   ```

5. **Follow the Prompts**
   The script will ask for the following information to configure your resources:

   *   Your GCP **Project ID**.
   *   A **Region** for your resources (e.g., `us-central1`).
   *   The **email address** you want to grant access to (your own Google account).
   *   Your **GitHub Username**.
   *   The **name of your forked repository**.
   *   Your **GitHub Personal Access Token (PAT)**
   *   Your **GA4 BigQuery Dataset ID** (e.g., `analytics_123456789`).

The script will take several minutes to complete as it enables APIs, creates service accounts, builds the application, deploys it to Cloud Run, and configures the Cloud Build trigger.

Once the script completes successfully, your CI/CD pipeline is live. **You can now close Cloud Shell.** All future development can be done from your local machine; simply push code changes to your GitHub repository's `main` branch, and Cloud Build will automatically deploy them.

---

## How the Application Works

The application provides a conversational interface for querying your GA4 data.

1.  **Chat Interface**: The user asks a question in a chat window (e.g., "how many active users were there yesterday?").
2.  **Intent Routing**: The question is sent to the Gemini API. Using a library of predefined SQL templates in `query_template_library.py`, Gemini decides which template is most appropriate.
3.  **Parameter Extraction**: Gemini also extracts necessary parameters from the question (e.g., `start_date='20240101'`, `end_date='20240101'`).
4.  **Query Execution**: The application populates the chosen SQL template with the extracted parameters and executes the query against your GA4 BigQuery export table.
5.  **Summarization**: The query results are sent back to Gemini, which generates a user-friendly, natural language summary.
6.  **Display**: The final answer is displayed in the chat. For transparency, an expander shows the chosen template, parameters, and the exact SQL query that was run.

## Development and Customization
### Customizing Example Prompts
To update the placeholder examples users see in the chat input, edit the `placeholder` variable in `app.py`:

```python
placeholder = "Try: 'Top traffic sources last month', 'Mobile vs desktop users', 'Best campaigns by revenue'"

### BigQuery Query Limits
The app has a 30GB per-query limit for cost protection. To modify this:

1. Edit `app.py`, find the `execute_bq_query()` function
2. Change `maximum_bytes_billed=30_000_000_000` to your desired bytes
3. Push changes to trigger redeployment

**Examples:**
- 10GB: `10_000_000_000`
- 50GB: `50_000_000_000`
- 100GB: `100_000_000_000`

### Adding New Queries

The core logic of the app resides in **`query_template_library.py`**. To teach the app how to answer new types of questions, add a new entry to the `QUERY_TEMPLATE_LIBRARY` dictionary.

Each entry requires:
*   **A unique key** (e.g., `traffic_by_source_medium`).
*   **`description`**: A clear, natural language description of what the query does. Gemini uses this to match the user's question to the right template. Be descriptive!
*   **`template`**: A parameterized SQL query string. Use placeholders like `{start_date}` and `{end_date}` that the application and Gemini can fill in.

**Example:**
```python
"traffic_by_source_medium": {
    "description": "Analyzes website traffic sources and mediums. Good for questions like 'where did my traffic come from?', 'top traffic sources', or 'breakdown by source and medium'.",
    "template": """
-- Reports on user acquisition by traffic source and medium
-- LLM: Replace {start_date} and {end_date}
SELECT
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    COUNT(DISTINCT user_pseudo_id) AS user_count,
    COUNT(event_name) AS event_count
FROM
    `{project_id}.{dataset_id}.events_*`
WHERE
    _table_suffix BETWEEN '{start_date}' AND '{end_date}'
GROUP BY
    traffic_source,
    traffic_medium
ORDER BY
    user_count DESC
"""
}, ...
```

After adding your new template, commit and push the change to `main`. Cloud Build will automatically deploy the updated application.