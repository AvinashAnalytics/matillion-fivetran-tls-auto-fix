# Daily Fivetran TLS Fix Pipeline — Complete Build Document

**Author:** Maia (AI Pipeline Architect)  
**Prepared for:** Avinash, Data Engineer 
**Date:** 2026-03-27  
**Pipeline Name:** `Daily_Fivetran_TLS_Fix`  
**Warehouse:** Snowflake (TRIAL_18E299FB043A4435A6C9BC2959CE62E4)  
**Schedule:** Daily at 3:00 AM IST (9:30 PM UTC previous day)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Custom Connectors (API Configuration)](#3-custom-connectors-api-configuration)
4. [Orchestration Pipeline: Daily_Fivetran_TLS_Fix](#4-orchestration-pipeline-daily_fivetran_tls_fix)
5. [Transformation Pipeline: Flatten_and_Filter_Broken_TLS](#5-transformation-pipeline-flatten_and_filter_broken_tls)
6. [Sub-Orchestration Pipeline: Validate_TLS_Connection](#6-sub-orchestration-pipeline-validate_tls_connection)
7. [Watermark Logic — Detailed Explanation](#7-watermark-logic--detailed-explanation)
8. [Filter Logic — Detailed Explanation](#8-filter-logic--detailed-explanation)
9. [Loop Logic — Detailed Explanation](#9-loop-logic--detailed-explanation)
10. [Complete Component Inventory & Connection Map](#10-complete-component-inventory--connection-map)
11. [Variables Reference](#11-variables-reference)
12. [Log Table Schema](#12-log-table-schema)
13. [Snowflake Prerequisites](#13-snowflake-prerequisites)
14. [Error Handling Strategy](#14-error-handling-strategy)
15. [Scheduling & Notification](#15-scheduling--notification)

---

## 1. Executive Summary

This document describes the complete, production-ready architecture for a daily automated pipeline that:

1. **Fetches** all Fivetran connections via the Fivetran REST API (with pagination).
2. **Loads** the raw JSON response into a Snowflake staging table.
3. **Flattens** the nested JSON structure into relational columns.
4. **Filters** for connections broken specifically due to TLS, SSL, or certificate issues.
5. **Appends** those broken connections into a historical log table with a `CHECK_DATE` (today) and `WATERMARK_DATE` (exact timestamp), enabling daily partitioning and deduplication.
6. **Loops** through only today's unvalidated broken IDs and calls the Fivetran test endpoint with `trust_certificates = true`.
7. **Logs** each validation result back into the log table.
8. **Generates** a summary report with counts of broken, fixed, and failed connections for the day.

The pipeline is built across **3 files**:

| File | Type | Purpose |
|---|---|---|
| `Daily_Fivetran_TLS_Fix.orch.yaml` | Orchestration | Main pipeline — coordinates all steps |
| `Flatten_and_Filter_Broken_TLS.tran.yaml` | Transformation | Flattens JSON, filters TLS errors, adds watermark |
| `Validate_TLS_Connection.orch.yaml` | Orchestration | Sub-pipeline called per broken ID by the iterator |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MAIN ORCHESTRATION PIPELINE                              │
│                    Daily_Fivetran_TLS_Fix.orch.yaml                        │
│                                                                             │
│  [Start]                                                                    │
│    │                                                                        │
│    ▼                                                                        │
│  [Create Log Table]  ──── CREATE TABLE IF NOT EXISTS                       │
│    │                       FIVETRAN_TLS_BROKEN_LOG                          │
│    ▼                                                                        │
│  [Fetch Connections via API]  ──── Python Pushdown                         │
│    │                               GET /v1/connections (paginated)          │
│    │                               → RAW_FIVETRAN_CONNECTIONS               │
│    ▼                                                                        │
│  [Run Flatten and Filter]  ──── Calls transformation pipeline              │
│    │                             Flatten_and_Filter_Broken_TLS.tran.yaml   │
│    │                             → FIVETRAN_TLS_BROKEN_STAGING              │
│    ▼                                                                        │
│  [Insert Staging into Log]  ──── SQL Script                                │
│    │                              INSERT INTO FIVETRAN_TLS_BROKEN_LOG      │
│    │                              (deduplicates against today's entries)    │
│    ▼                                                                        │
│  [Loop Today's Broken IDs]  ──── Table Iterator (Advanced mode)            │
│    │                              SELECT ... WHERE CHECK_DATE = TODAY       │
│    │                              AND VALIDATED = FALSE                     │
│    │                                                                        │
│    │  ┌─── Iterator Target ───────────────────────────────────┐            │
│    │  │  [Validate Each Connection]                            │            │
│    │  │   Run Orchestration →                                  │            │
│    │  │   Validate_TLS_Connection.orch.yaml                    │            │
│    │  │   (passes broken_id, connector_name, service,          │            │
│    │  │    fivetran_api_key, fivetran_api_secret)               │            │
│    │  └────────────────────────────────────────────────────────┘            │
│    │                                                                        │
│    ▼                                                                        │
│  [Send Summary Notification]  ──── SQL Script                              │
│                                    Creates FIVETRAN_TLS_DAILY_SUMMARY      │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION PIPELINE                                  │
│                    Flatten_and_Filter_Broken_TLS.tran.yaml                 │
│                                                                             │
│  [Load Raw Connections]                                                     │
│    │  Table Input ← RAW_FIVETRAN_CONNECTIONS (DATA column)                 │
│    ▼                                                                        │
│  [Flatten Connection Items]                                                 │
│    │  Flatten Variant → explode data.items[] array                         │
│    │  Extract: id, name, service, setup_state, status, setup_tests         │
│    ▼                                                                        │
│  [Filter TLS Broken Only]                                                   │
│    │  Advanced Filter → setup_state='broken' AND error contains            │
│    │  'tls' OR 'certificate' OR 'ssl'                                      │
│    ▼                                                                        │
│  [Add Watermark Columns]                                                    │
│    │  Calculator → adds CHECK_DATE, WATERMARK_DATE, VALIDATED=FALSE        │
│    ▼                                                                        │
│  [Append to Log Table]                                                      │
│     Rewrite Table → FIVETRAN_TLS_BROKEN_STAGING                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    SUB-ORCHESTRATION PIPELINE                               │
│                    Validate_TLS_Connection.orch.yaml                       │
│                    (Called once per broken connection ID)                    │
│                                                                             │
│  [Start]                                                                    │
│    │                                                                        │
│    ▼                                                                        │
│  [Call Fivetran Test API]  ──── Python Pushdown                            │
│    │                            POST /v1/connections/{broken_id}/test       │
│    │                            Body: {"trust_certificates": true}          │
│    │                            Auth: Basic (API key + secret)              │
│    │                                                                        │
│    ├── success ──▶ [Update Log with Result]                                │
│    │                SQL Script → SET VALIDATED=TRUE,                        │
│    │                VALIDATION_RESULT = <API response JSON>                 │
│    │                                                                        │
│    └── failure ──▶ [Log Failure]                                           │
│                     SQL Script → SET VALIDATED=TRUE,                        │
│                     VALIDATION_RESULT = {"error": "API call failed..."}     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Custom Connectors (API Configuration)

### Why Python Pushdown Instead of Custom Connectors

After thorough analysis, we use **Python Pushdown** components instead of Matillion Custom Connectors for the Fivetran API calls. Here is why:

| Concern | Custom Connector | Python Pushdown (Chosen) |
|---|---|---|
| **Pagination** | No built-in cursor-based pagination for Fivetran's API | Full control — loop until `next_cursor` is null |
| **Dynamic URI parameters** | Known limitation: Configurable URI params may fail validation at runtime | Full control — `f-string` interpolation in Python |
| **POST with body** | Limited body customization | Full control — `requests.post(json=payload)` |
| **Error handling** | Limited — binary success/fail | Try/except with detailed error capture |
| **Secrets** | Auth entered at connector level | Variables passed from parent pipeline |

### However — If You Still Want Custom Connectors

For reference, here are the exact configurations if you prefer to create them in the Custom Connector UI:

#### Connector 1: `Fivetran_List_Connections`

| Setting | Value |
|---|---|
| **Connector Name** | `Fivetran_List_Connections` |
| **Method** | GET |
| **URL** | `https://api.fivetran.com/v1/connections` |
| **Authentication** | Basic Auth |
| **Username** | Fivetran API Key (from Matillion Secret Definition) |
| **Password** | Fivetran API Secret (from Matillion Secret Definition) |
| **Query Parameters** | `limit` = `1000` (Constant) |
| **Response Format** | JSON |
| **Pagination** | Not natively supported — requires manual cursor handling |

> **Limitation:** This connector fetches only the first page (up to 1000 connections). If you have more than 1000 connections, you would need to handle `cursor`-based pagination externally — which is why Python Pushdown is the superior choice.

#### Connector 2: `Fivetran_Fix_TLS`

| Setting | Value |
|---|---|
| **Connector Name** | `Fivetran_Fix_TLS` |
| **Method** | POST |
| **URL** | `https://api.fivetran.com/v1/connections/{connectionId}/test` |
| **Authentication** | Basic Auth |
| **Username** | Fivetran API Key (from Matillion Secret Definition) |
| **Password** | Fivetran API Secret (from Matillion Secret Definition) |
| **URI Parameter** | `connectionId` → Configurable (set at pipeline runtime) |
| **Request Body** | `{ "trust_certificates": true }` |
| **Response Format** | JSON |

> **⚠️ Known Limitation:** Configurable URI parameters in Custom Connectors may fail validation during pipeline execution. This is a documented Matillion platform limitation. The Python Pushdown approach avoids this entirely.

---

## 4. Orchestration Pipeline: Daily_Fivetran_TLS_Fix

**File:** `Daily_Fivetran_TLS_Fix.orch.yaml`  
**Type:** Orchestration  
**Purpose:** Main daily coordinator pipeline

### Step-by-Step Component Breakdown

---

### Step 1: Start

| Property | Value |
|---|---|
| **Component** | [Start](https://docs.matillion.com/data-productivity-cloud/designer/docs/start) |
| **Type** | `start` |
| **Transition** | Unconditional → `Create Log Table` |
| **Purpose** | Entry point for the pipeline |

---

### Step 2: Create Log Table

| Property | Value |
|---|---|
| **Component** | [Create Table](https://docs.matillion.com/data-productivity-cloud/designer/docs/create-table-v2) |
| **Type** | `create-table-v2` |
| **Create Method** | `Create If Not Exists` |
| **Database** | `[Environment Default]` |
| **Schema** | `[Environment Default]` |
| **Table Name** | `FIVETRAN_TLS_BROKEN_LOG` |
| **Table Type** | Permanent |
| **Transition** | Success → `Fetch Connections via API` |

**Why this step exists:**  
On the very first run, the log table won't exist yet. Using `Create If Not Exists` ensures the table is created on first run and safely skipped on all subsequent runs. This makes the pipeline idempotent.

**Columns:** See [Section 12: Log Table Schema](#12-log-table-schema) for full column definitions.

**Primary Key:** (`CHECK_DATE`, `BROKEN_ID`) — ensures one entry per connection per day.

---

### Step 3: Fetch Connections via API

| Property | Value |
|---|---|
| **Component** | [Python Pushdown](https://docs.matillion.com/data-productivity-cloud/designer/docs/python-pushdown) |
| **Type** | `python-pushdown` |
| **External Access Integration** | `FIVETRAN_API_ACCESS` (Snowflake-level config) |
| **Python Version** | 3.11 |
| **Packages** | `requests` |
| **Variable Resolution** | Enabled (`Yes`) |
| **Script Timeout** | 360 seconds |
| **Transition** | Success → `Run Flatten and Filter` |

**What the script does:**

1. Reads `${fivetran_api_key}` and `${fivetran_api_secret}` from pipeline variables.
2. Calls `GET https://api.fivetran.com/v1/connections?limit=1000`.
3. **Paginates** — if the response contains a `next_cursor`, it fetches the next page and appends results.
4. Continues until all connections are collected.
5. Creates/replaces `RAW_FIVETRAN_CONNECTIONS` table with a single VARIANT column (`DATA`).
6. Inserts the complete JSON payload as `PARSE_JSON('{...}')`.
7. Prints the count of loaded connections for task logging.

**Pseudocode:**
```
all_items = []
cursor = None
LOOP:
    GET /v1/connections?limit=1000&cursor={cursor}
    all_items += response.data.items
    cursor = response.data.next_cursor
    IF cursor IS NULL → BREAK
END LOOP

CREATE OR REPLACE TABLE RAW_FIVETRAN_CONNECTIONS (DATA VARIANT)
INSERT INTO RAW_FIVETRAN_CONNECTIONS SELECT PARSE_JSON('{items: all_items}')
```

**Output table:** `RAW_FIVETRAN_CONNECTIONS`

| Column | Type | Content |
|---|---|---|
| `DATA` | VARIANT | `{"items": [{connection1}, {connection2}, ...]}` |

---

### Step 4: Run Flatten and Filter

| Property | Value |
|---|---|
| **Component** | [Run Transformation](https://docs.matillion.com/data-productivity-cloud/designer/docs/run-transformation) |
| **Type** | `run-transformation` |
| **Target Pipeline** | `Flatten_and_Filter_Broken_TLS.tran.yaml` |
| **Transition** | Success → `Insert Staging into Log` |

**What happens:**  
This triggers the transformation pipeline (detailed in Section 5) which:
- Reads `RAW_FIVETRAN_CONNECTIONS`
- Flattens the JSON array
- Filters for TLS/SSL/certificate errors only
- Adds `CHECK_DATE` and `WATERMARK_DATE`
- Writes results to `FIVETRAN_TLS_BROKEN_STAGING`

---

### Step 5: Insert Staging into Log

| Property | Value |
|---|---|
| **Component** | [SQL Script](https://docs.matillion.com/data-productivity-cloud/designer/docs/sql-executor) |
| **Type** | `sql-executor` |
| **Transition** | Success → `Loop Today's Broken IDs` |

**SQL executed:**
```sql
INSERT INTO "FIVETRAN_TLS_BROKEN_LOG" (
  "CHECK_DATE", "WATERMARK_DATE", "BROKEN_ID", "CONNECTOR_NAME",
  "SERVICE", "ERROR_REASON", "VALIDATED", "VALIDATION_RESULT"
)
SELECT
  "CHECK_DATE", "WATERMARK_DATE", "BROKEN_ID", "CONNECTOR_NAME",
  "SERVICE", "ERROR_REASON", "VALIDATED", NULL
FROM "FIVETRAN_TLS_BROKEN_STAGING"
WHERE "BROKEN_ID" NOT IN (
  SELECT "BROKEN_ID" FROM "FIVETRAN_TLS_BROKEN_LOG"
  WHERE "CHECK_DATE" = CURRENT_DATE()
);
```

**Why this step exists (instead of writing directly to the log table from the transformation):**

1. **Deduplication** — If the pipeline is re-run on the same day (e.g., after a failure), this `NOT IN` subquery prevents duplicate entries for the same `BROKEN_ID` on the same `CHECK_DATE`.
2. **Separation of concerns** — The transformation writes to a staging table; the orchestration controls when and how data enters the permanent log.
3. **Idempotency** — Re-running the pipeline on the same day won't create duplicate log entries.

---

### Step 6: Loop Today's Broken IDs

| Property | Value |
|---|---|
| **Component** | [Table Iterator](https://docs.matillion.com/data-productivity-cloud/designer/docs/table-iterator) |
| **Type** | `table-iterator` |
| **Mode** | Advanced |
| **Concurrency** | Sequential |
| **Break on Failure** | No |
| **Iteration Target** | `Validate Each Connection` |
| **Transition** | Success → `Send Summary Notification` |

**Iterator query:**
```sql
SELECT "BROKEN_ID", "CONNECTOR_NAME", "SERVICE"
FROM "FIVETRAN_TLS_BROKEN_LOG"
WHERE "CHECK_DATE" = CURRENT_DATE()
  AND "VALIDATED" = FALSE
ORDER BY "BROKEN_ID"
```

**Column mapping (per iteration):**

| Table Column | → Pipeline Variable |
|---|---|
| `BROKEN_ID` | `broken_id` |
| `CONNECTOR_NAME` | `connector_name` |
| `SERVICE` | `service` |

**Key design decisions:**
- **Sequential** execution prevents Fivetran API rate-limiting.
- **Break on Failure = No** ensures all broken IDs are attempted even if some fail.
- **Advanced mode** with a custom SQL query allows the precise `WHERE` clause filtering.

See [Section 9: Loop Logic](#9-loop-logic--detailed-explanation) for full details.

---

### Step 7: Validate Each Connection (Iterator Target)

| Property | Value |
|---|---|
| **Component** | [Run Orchestration](https://docs.matillion.com/data-productivity-cloud/designer/docs/run-orchestration) |
| **Type** | `run-orchestration` |
| **Target Pipeline** | `Validate_TLS_Connection.orch.yaml` |
| **Position** | Same coordinates as the iterator (stacked) |

**Variable pass-through:**

| Child Variable | Value from Parent |
|---|---|
| `broken_id` | `${broken_id}` |
| `connector_name` | `${connector_name}` |
| `service` | `${service}` |
| `fivetran_api_key` | `${fivetran_api_key}` |
| `fivetran_api_secret` | `${fivetran_api_secret}` |

**Why a sub-pipeline?**  
The Table Iterator can only target **one** component. Since we need to both call the API AND update the log, we wrap both steps in a sub-orchestration pipeline.

---

### Step 8: Send Summary Notification

| Property | Value |
|---|---|
| **Component** | [SQL Script](https://docs.matillion.com/data-productivity-cloud/designer/docs/sql-executor) |
| **Type** | `sql-executor` |

**SQL executed:**
```sql
CREATE OR REPLACE TEMPORARY TABLE FIVETRAN_TLS_DAILY_SUMMARY AS
SELECT
  CURRENT_DATE() AS REPORT_DATE,
  COUNT(*) AS TOTAL_BROKEN_TODAY,
  SUM(CASE WHEN "VALIDATED" = TRUE THEN 1 ELSE 0 END) AS VALIDATED_TODAY,
  SUM(CASE WHEN "VALIDATED" = FALSE THEN 1 ELSE 0 END) AS NOT_VALIDATED
FROM "FIVETRAN_TLS_BROKEN_LOG"
WHERE "CHECK_DATE" = CURRENT_DATE();
```

**Transition:** Success → `Send Slack Notification`

---

### Step 9: Send Slack Notification

| Property | Value |
|---|---|
| **Component** | [Python Pushdown](https://docs.matillion.com/data-productivity-cloud/designer/docs/python-pushdown) |
| **Type** | `python-pushdown` |
| **External Access Integration** | `FIVETRAN_API_ACCESS` |
| **Packages** | `requests` |
| **Variable Resolution** | Enabled (`Yes`) |
| **Script Timeout** | 120 seconds |
| **Transition** | Success → `Send Email Notification` |

**What the script does:**

1. Reads `${slack_webhook_url}` from pipeline variable.
2. Queries `FIVETRAN_TLS_DAILY_SUMMARY` for today's counts.
3. If no webhook URL is configured, gracefully skips (prints "Skipping").
4. Builds a rich Slack Block Kit message with:
   - **Header**: "🔒 Fivetran TLS Fix Report - {date}"
   - **Fields**: Total Broken, Validated/Fixed, Not Validated, Success Rate %
   - **Footer**: "Automated by Matillion"
5. POSTs the JSON payload to the Slack incoming webhook URL.
6. Logs the result (success or failure with HTTP status).

**Slack message preview:**
```
┌─────────────────────────────────────────┐
│ 🔒 Fivetran TLS Fix Report - 2026-03-27│
│                                         │
│ Total TLS Broken:    12                 │
│ Validated/Fixed:     10                 │
│ Not Validated:        2                 │
│ Success Rate:       83.3%               │
│                                         │
│ Automated by Matillion                  │
└─────────────────────────────────────────┘
```

**Graceful degradation:** If `slack_webhook_url` is empty or not set, the component prints a skip message and succeeds — it does NOT fail the pipeline.

---

### Step 10: Send Email Notification

| Property | Value |
|---|---|
| **Component** | [Python Pushdown](https://docs.matillion.com/data-productivity-cloud/designer/docs/python-pushdown) |
| **Type** | `python-pushdown` |
| **Variable Resolution** | Enabled (`Yes`) |
| **Script Timeout** | 120 seconds |
| **Transition** | _(end of pipeline)_ |

**What the script does:**

1. Reads `${notification_email}` from pipeline variable.
2. Queries `FIVETRAN_TLS_DAILY_SUMMARY` for today's counts.
3. If no email is configured, gracefully skips.
4. Builds a plain-text email with subject and body:
   - **Subject**: `Fivetran TLS Fix Report - 2026-03-27: 12 broken, 10 fixed`
   - **Body**: Formatted report with all counts and success rate
5. Calls Snowflake's `SYSTEM$SEND_EMAIL()` stored procedure using the `tls_fix_email_integration` integration.
6. Catches exceptions gracefully — if the email integration isn't configured, it logs the error but still prints the summary.

**Email body preview:**
```
Fivetran TLS Fix Daily Report
================================
Date:             2026-03-27
Total TLS Broken: 12
Validated/Fixed:  10
Not Validated:    2
Success Rate:     83.3%
================================
Automated by Matillion - Daily_Fivetran_TLS_Fix pipeline
Check FIVETRAN_TLS_BROKEN_LOG for full details.
```

**Graceful degradation:** If `notification_email` is empty or the Snowflake email integration isn't configured, the component catches the error, prints the summary to the task log, and succeeds.

---

## 5. Transformation Pipeline: Flatten_and_Filter_Broken_TLS

**File:** `Flatten_and_Filter_Broken_TLS.tran.yaml`  
**Type:** Transformation  
**Purpose:** Flatten raw JSON, extract connection fields, filter for TLS errors, add watermark columns

### Step-by-Step Component Breakdown

---

### Step T1: Load Raw Connections

| Property | Value |
|---|---|
| **Component** | [Table Input](https://docs.matillion.com/data-productivity-cloud/designer/docs/table-input) |
| **Type** | `table-input` |
| **Table** | `RAW_FIVETRAN_CONNECTIONS` |
| **Columns** | `DATA` |
| **Output** | → `Flatten Connection Items` |

**What it reads:**  
The single-row, single-column VARIANT table containing the complete Fivetran API response.

**Sample input:**
```json
{
  "items": [
    {
      "id": "abc_123",
      "name": "My Postgres Connector",
      "service": "postgres",
      "setup_state": "broken",
      "status": {
        "setup_state": "broken",
        "error": "TLS certificate validation failed..."
      },
      "setup_tests": [...]
    },
    ...
  ]
}
```

---

### Step T2: Flatten Connection Items

| Property | Value |
|---|---|
| **Component** | [Flatten Variant](https://docs.matillion.com/data-productivity-cloud/designer/docs/flatten-variant) |
| **Type** | `flatten-variant` |
| **Include Input Columns** | No |
| **Input** | ← `Load Raw Connections` |
| **Output** | → `Filter TLS Broken Only` |

**Column Flattens (array explosion):**

| Column | Property | Alias | Outer | Recursive | Mode |
|---|---|---|---|---|---|
| `DATA` | `items` | `items_flat` | Yes | No | Array |

This explodes the `items` array so each connection becomes its own row.

**Column Mapping (field extraction):**

| Source Column | Property | Type | Output Alias |
|---|---|---|---|
| `items_flat` | `id` | VARCHAR | `BROKEN_ID` |
| `items_flat` | `name` | VARCHAR | `CONNECTOR_NAME` |
| `items_flat` | `service` | VARCHAR | `SERVICE` |
| `items_flat` | `setup_state` | VARCHAR | `SETUP_STATE` |
| `items_flat` | `status.setup_state` | VARCHAR | `STATUS_SETUP_STATE` |
| `items_flat` | `setup_tests` | VARIANT | `SETUP_TESTS` |
| `items_flat` | `status` | VARIANT | `ERROR_REASON` |

**Output:** One row per Fivetran connection with extracted fields.

---

### Step T3: Filter TLS Broken Only

| Property | Value |
|---|---|
| **Component** | [Filter](https://docs.matillion.com/data-productivity-cloud/designer/docs/filter) |
| **Type** | `filter` |
| **Mode** | Advanced |
| **Input** | ← `Flatten Connection Items` |
| **Output** | → `Add Watermark Columns` |

**Filter clause (Advanced SQL WHERE):**
```sql
("SETUP_STATE" = 'broken' OR "STATUS_SETUP_STATE" = 'broken')
AND (
  LOWER("ERROR_REASON"::VARCHAR) LIKE '%tls%'
  OR LOWER("ERROR_REASON"::VARCHAR) LIKE '%certificate%'
  OR LOWER("ERROR_REASON"::VARCHAR) LIKE '%ssl%'
  OR LOWER("SETUP_TESTS"::VARCHAR) LIKE '%tls%'
  OR LOWER("SETUP_TESTS"::VARCHAR) LIKE '%certificate%'
)
```

See [Section 8: Filter Logic](#8-filter-logic--detailed-explanation) for detailed explanation.

---

### Step T4: Add Watermark Columns

| Property | Value |
|---|---|
| **Component** | [Calculator](https://docs.matillion.com/data-productivity-cloud/designer/docs/calculator) |
| **Type** | `calculator` |
| **Include Input Columns** | Yes |
| **Input** | ← `Filter TLS Broken Only` |
| **Output** | → `Append to Log Table` |

**Calculations:**

| Expression | Output Column | Purpose |
|---|---|---|
| `CURRENT_DATE()` | `CHECK_DATE` | Today's date — used as daily partition key |
| `CURRENT_TIMESTAMP()` | `WATERMARK_DATE` | Exact timestamp — distinguishes runs within a day |
| `FALSE` | `VALIDATED` | Default — marks as not yet validated |

See [Section 7: Watermark Logic](#7-watermark-logic--detailed-explanation) for detailed explanation.

---

### Step T5: Append to Log Table (Staging)

| Property | Value |
|---|---|
| **Component** | [Rewrite Table](https://docs.matillion.com/data-productivity-cloud/designer/docs/rewrite-table) |
| **Type** | `rewrite-table` |
| **Target Table** | `FIVETRAN_TLS_BROKEN_STAGING` |
| **Input** | ← `Add Watermark Columns` |

**Why Rewrite Table (not Table Output)?**  
We write to a **staging table** (not the final log) because:
1. The staging table is recreated fresh each run.
2. The orchestration pipeline then handles the dedup INSERT into the permanent log.
3. This avoids the need for the log table to already exist when the transformation validates.

---

## 6. Sub-Orchestration Pipeline: Validate_TLS_Connection

**File:** `Validate_TLS_Connection.orch.yaml`  
**Type:** Orchestration  
**Purpose:** Called once per broken connection — tests it and logs the result

### Step S1: Start

Standard entry point. Transitions unconditionally to the API call.

### Step S2: Call Fivetran Test API

| Property | Value |
|---|---|
| **Component** | [Python Pushdown](https://docs.matillion.com/data-productivity-cloud/designer/docs/python-pushdown) |
| **Type** | `python-pushdown` |
| **External Access Integration** | `FIVETRAN_API_ACCESS` |
| **Packages** | `requests` |
| **Script Timeout** | 120 seconds |

**What the script does:**

1. Reads `${broken_id}`, `${fivetran_api_key}`, `${fivetran_api_secret}` from variables.
2. Calls `POST https://api.fivetran.com/v1/connections/{broken_id}/test` with body `{"trust_certificates": true}`.
3. Uses Basic Auth with the API key and secret.
4. On success: stores the JSON response in `${validation_result}` variable.
5. On exception: stores the error message as JSON in `${validation_result}`.
6. Updates `${api_status_code}` with the HTTP status code.

**Transitions:**
- Success → `Update Log with Result`
- Failure → `Log Failure`

### Step S3a: Update Log with Result (Success Path)

| Property | Value |
|---|---|
| **Component** | [SQL Script](https://docs.matillion.com/data-productivity-cloud/designer/docs/sql-executor) |

**SQL:**
```sql
UPDATE "FIVETRAN_TLS_BROKEN_LOG"
SET "VALIDATED" = TRUE,
    "VALIDATION_RESULT" = PARSE_JSON('${validation_result}')
WHERE "BROKEN_ID" = '${broken_id}'
  AND "CHECK_DATE" = CURRENT_DATE();
```

### Step S3b: Log Failure (Failure Path)

| Property | Value |
|---|---|
| **Component** | [SQL Script](https://docs.matillion.com/data-productivity-cloud/designer/docs/sql-executor) |

**SQL:**
```sql
UPDATE "FIVETRAN_TLS_BROKEN_LOG"
SET "VALIDATED" = TRUE,
    "VALIDATION_RESULT" = PARSE_JSON('{"error": "API call failed for connection ${broken_id}"}')
WHERE "BROKEN_ID" = '${broken_id}'
  AND "CHECK_DATE" = CURRENT_DATE();
```

**Why mark VALIDATED=TRUE even on failure?**  
To prevent infinite retry loops. If the API call fails, we still mark it as validated (attempted) and store the error. The `VALIDATION_RESULT` JSON will contain the error details for investigation.

---

## 7. Watermark Logic — Detailed Explanation

### The Problem

We need to:
1. Keep a **full history** of all broken connections across all days.
2. Each day, only validate **that day's newly discovered broken IDs**.
3. If a connection is broken today AND was also broken yesterday, it should still appear as a new entry for today (it may have been re-broken after being fixed).

### The Solution: Dual-Column Watermark

| Column | Type | Purpose | Example |
|---|---|---|---|
| `CHECK_DATE` | DATE | **Daily partition key** — groups entries by calendar date | `2026-03-27` |
| `WATERMARK_DATE` | TIMESTAMP_NTZ | **Exact detection time** — unique per pipeline run | `2026-03-27 09:30:15.123` |

### How It Works

```
Day 1 (March 27):
  Pipeline runs at 3:00 AM IST
  → CHECK_DATE = 2026-03-27
  → WATERMARK_DATE = 2026-03-27 03:00:05.XXX
  → Finds connections A, B, C broken
  → Inserts 3 rows with VALIDATED=FALSE
  → Iterator selects WHERE CHECK_DATE='2026-03-27' AND VALIDATED=FALSE
  → Validates A, B, C → sets VALIDATED=TRUE for each

Day 2 (March 28):
  Pipeline runs at 3:00 AM IST
  → CHECK_DATE = 2026-03-28
  → WATERMARK_DATE = 2026-03-28 03:00:05.XXX
  → Finds connections A, D broken (B and C were fixed)
  → Inserts 2 rows (A is a NEW entry for today, not a duplicate of yesterday)
  → Iterator selects WHERE CHECK_DATE='2026-03-28' AND VALIDATED=FALSE
  → Validates A, D → only today's entries
  → Yesterday's entries (A, B, C from March 27) are untouched
```

### Why Both Columns?

- **CHECK_DATE alone** would suffice for daily partitioning, but wouldn't distinguish between multiple runs on the same day.
- **WATERMARK_DATE alone** would make it hard to query "all broken IDs from today" without timestamp range logic.
- **Together**, they provide both **easy daily querying** (`WHERE CHECK_DATE = CURRENT_DATE()`) and **exact lineage** (which specific run detected this issue).

### Deduplication Guard

The `Insert Staging into Log` SQL Script prevents duplicates within the same day:

```sql
WHERE "BROKEN_ID" NOT IN (
  SELECT "BROKEN_ID" FROM "FIVETRAN_TLS_BROKEN_LOG"
  WHERE "CHECK_DATE" = CURRENT_DATE()
)
```

This means:
- If the pipeline is re-run on the same day (after a failure), already-inserted IDs are skipped.
- Each `BROKEN_ID` can appear at most **once per day** in the log.

---

## 8. Filter Logic — Detailed Explanation

### What We're Filtering For

Fivetran connections where:
1. The connection's setup state is **broken** (either in the top-level `setup_state` field OR the nested `status.setup_state` field — Fivetran's API returns both).
2. The error message or setup test results contain keywords indicating a **TLS/SSL/certificate** issue.

### The Filter Expression (Advanced SQL WHERE)

```sql
("SETUP_STATE" = 'broken' OR "STATUS_SETUP_STATE" = 'broken')
AND (
  LOWER("ERROR_REASON"::VARCHAR) LIKE '%tls%'
  OR LOWER("ERROR_REASON"::VARCHAR) LIKE '%certificate%'
  OR LOWER("ERROR_REASON"::VARCHAR) LIKE '%ssl%'
  OR LOWER("SETUP_TESTS"::VARCHAR) LIKE '%tls%'
  OR LOWER("SETUP_TESTS"::VARCHAR) LIKE '%certificate%'
)
```

### Breakdown

| Clause | Purpose |
|---|---|
| `"SETUP_STATE" = 'broken'` | Catches connections where top-level setup_state is broken |
| `"STATUS_SETUP_STATE" = 'broken'` | Catches connections where nested status.setup_state is broken |
| `LOWER("ERROR_REASON"::VARCHAR) LIKE '%tls%'` | Matches TLS errors in the error/status JSON |
| `LOWER("ERROR_REASON"::VARCHAR) LIKE '%certificate%'` | Matches certificate errors |
| `LOWER("ERROR_REASON"::VARCHAR) LIKE '%ssl%'` | Matches SSL errors |
| `LOWER("SETUP_TESTS"::VARCHAR) LIKE '%tls%'` | Matches TLS issues in setup test results |
| `LOWER("SETUP_TESTS"::VARCHAR) LIKE '%certificate%'` | Matches certificate issues in setup tests |

### Why VARIANT::VARCHAR Cast?

The `ERROR_REASON` and `SETUP_TESTS` columns are VARIANT type (raw JSON). Casting to VARCHAR with `::VARCHAR` converts the entire JSON structure to a searchable string. Using `LOWER()` ensures case-insensitive matching.

### Why Check Both `ERROR_REASON` and `SETUP_TESTS`?

Fivetran reports errors in different places depending on the connection type:
- Some connectors report in `status.error` (captured in our `ERROR_REASON` column)
- Some connectors report in `setup_tests` array (captured in our `SETUP_TESTS` column)

Checking both ensures we catch TLS issues regardless of where Fivetran reports them.

---

## 9. Loop Logic — Detailed Explanation

### The Iterator Pattern

```
┌──────────────────────────┐
│   Table Iterator          │
│   (Loop Today's Broken)   │
│                            │
│   Query:                   │
│   SELECT BROKEN_ID,        │
│     CONNECTOR_NAME, SERVICE │
│   FROM FIVETRAN_TLS_..LOG  │
│   WHERE CHECK_DATE = TODAY  │
│     AND VALIDATED = FALSE   │
│                            │
│   For each row:            │
│     broken_id = row[0]     │
│     connector_name = row[1]│
│     service = row[2]       │
│                            │
│   ┌────────────────────┐   │
│   │  Run Orchestration │   │ ← Iterator Target (stacked)
│   │  Validate_TLS_     │   │
│   │  Connection.orch   │   │
│   └────────────────────┘   │
│                            │
│   After all iterations:    │
│   Success → Summary        │
└──────────────────────────┘
```

### Why Table Iterator (Advanced Mode)?

- **Advanced mode** allows a custom SQL query with `WHERE` clause, which is essential for filtering only today's unvalidated rows.
- **Basic mode** would iterate ALL rows in the table (including historical), which is incorrect.

### Why Run Orchestration as Target (Not Direct API Call)?

The iterator can only target **ONE component**. Our validation logic requires:
1. Call the Fivetran test API
2. Update the log table with the result

Two steps → must be wrapped in a sub-pipeline.

### Concurrency: Sequential

We iterate sequentially (not concurrently) because:
1. **API rate limiting** — Fivetran may rate-limit concurrent requests.
2. **Variable safety** — COPIED scope variables ensure each iteration gets its own values, but sequential is safer for database updates.
3. **Debugging** — Sequential execution makes it easy to trace which connection caused issues.

### Break on Failure: No

Even if one connection fails to validate (API timeout, auth error, etc.), the iterator continues to the next one. This ensures maximum coverage per run.

### After All Iterations

Once the iterator has processed all rows (or the query returns 0 rows), it follows its `success` transition to the summary notification step.

---

## 10. Complete Component Inventory & Connection Map

### Pipeline 1: Daily_Fivetran_TLS_Fix.orch.yaml

| # | Component Name | Type | Matillion Component | Connects To | Connection Type |
|---|---|---|---|---|---|
| 1 | Start | Entry point | `start` | Create Log Table | Unconditional |
| 2 | Create Log Table | DDL | `create-table-v2` | Fetch Connections via API | Success |
| 3 | Fetch Connections via API | API call | `python-pushdown` | Run Flatten and Filter | Success |
| 4 | Run Flatten and Filter | Child pipeline | `run-transformation` | Insert Staging into Log | Success |
| 5 | Insert Staging into Log | SQL | `sql-executor` | Loop Todays Broken IDs | Success |
| 6 | Loop Todays Broken IDs | Iterator | `table-iterator` | Send Summary Notification | Success |
| 7 | Validate Each Connection | Iterator target | `run-orchestration` | _(controlled by iterator)_ | Iteration |
| 8 | Send Summary Notification | SQL | `sql-executor` | Send Slack Notification | Success |
| 9 | Send Slack Notification | Slack alert | `python-pushdown` | Send Email Notification | Success |
| 10 | Send Email Notification | Email alert | `python-pushdown` | _(end of pipeline)_ | — |

### Pipeline 2: Flatten_and_Filter_Broken_TLS.tran.yaml

| # | Component Name | Type | Matillion Component | Sources |
|---|---|---|---|---|
| 1 | Load Raw Connections | Data source | `table-input` | — |
| 2 | Flatten Connection Items | JSON flatten | `flatten-variant` | Load Raw Connections |
| 3 | Filter TLS Broken Only | Row filter | `filter` | Flatten Connection Items |
| 4 | Add Watermark Columns | Calculated fields | `calculator` | Filter TLS Broken Only |
| 5 | Append to Log Table | Write output | `rewrite-table` | Add Watermark Columns |

### Pipeline 3: Validate_TLS_Connection.orch.yaml

| # | Component Name | Type | Matillion Component | Connects To | Connection Type |
|---|---|---|---|---|---|
| 1 | Start | Entry point | `start` | Call Fivetran Test API | Unconditional |
| 2 | Call Fivetran Test API | API call | `python-pushdown` | Update Log with Result | Success |
| 2b | Call Fivetran Test API | API call | `python-pushdown` | Log Failure | Failure |
| 3a | Update Log with Result | SQL | `sql-executor` | _(end)_ | — |
| 3b | Log Failure | SQL | `sql-executor` | _(end)_ | — |

### Total Component Count: 18

| Component Type | Count | Used In |
|---|---|---|
| `start` | 2 | Main orch, Sub orch |
| `create-table-v2` | 1 | Main orch |
| `python-pushdown` | 4 | Main orch (fetch, Slack, email), Sub orch (test) |
| `run-transformation` | 1 | Main orch |
| `sql-executor` | 4 | Main orch (insert, summary), Sub orch (update, log fail) |
| `table-iterator` | 1 | Main orch |
| `run-orchestration` | 1 | Main orch (iterator target) |
| `table-input` | 1 | Transformation |
| `flatten-variant` | 1 | Transformation |
| `filter` | 1 | Transformation |
| `calculator` | 1 | Transformation |
| `rewrite-table` | 1 | Transformation |

---

## 11. Variables Reference

### Daily_Fivetran_TLS_Fix.orch.yaml (Main Pipeline)

| Variable | Type | Scope | Visibility | Default | Purpose |
|---|---|---|---|---|---|
| `broken_id` | TEXT | COPIED | PRIVATE | `""` | Set by iterator — current connection ID |
| `connector_name` | TEXT | COPIED | PRIVATE | `""` | Set by iterator — current connector name |
| `service` | TEXT | COPIED | PRIVATE | `""` | Set by iterator — current service type |
| `fivetran_api_key` | TEXT | SHARED | PRIVATE | `""` | Fivetran API key — set at runtime |
| `fivetran_api_secret` | TEXT | SHARED | PRIVATE | `""` | Fivetran API secret — set at runtime |
| `slack_webhook_url` | TEXT | SHARED | PRIVATE | `""` | Slack incoming webhook URL for notifications |
| `notification_email` | TEXT | SHARED | PRIVATE | `""` | Email address for daily report (requires Snowflake email integration) |

### Validate_TLS_Connection.orch.yaml (Sub-Pipeline)

| Variable | Type | Scope | Visibility | Default | Purpose |
|---|---|---|---|---|---|
| `broken_id` | TEXT | COPIED | PUBLIC | `""` | Received from parent — connection to test |
| `connector_name` | TEXT | COPIED | PUBLIC | `""` | Received from parent — for logging |
| `service` | TEXT | COPIED | PUBLIC | `""` | Received from parent — for logging |
| `fivetran_api_key` | TEXT | SHARED | PUBLIC | `""` | Received from parent — API auth |
| `fivetran_api_secret` | TEXT | SHARED | PUBLIC | `""` | Received from parent — API auth |
| `validation_result` | TEXT | COPIED | PRIVATE | `""` | Set by Python — API response JSON |
| `api_status_code` | TEXT | COPIED | PRIVATE | `""` | Set by Python — HTTP status code |

### Why COPIED Scope for Iterator Variables?

Variables used in iterations MUST be `COPIED` scope. This ensures each iteration gets its own independent copy of the variable, preventing race conditions if concurrency is ever enabled.

---

## 12. Log Table Schema

### Table: `FIVETRAN_TLS_BROKEN_LOG`

```sql
CREATE TABLE IF NOT EXISTS FIVETRAN_TLS_BROKEN_LOG (
  CHECK_DATE          DATE            COMMENT 'Date when broken connection was detected',
  WATERMARK_DATE      TIMESTAMP_NTZ   COMMENT 'Exact timestamp of detection',
  BROKEN_ID           VARCHAR(255)    COMMENT 'Fivetran connection ID',
  CONNECTOR_NAME      VARCHAR(500)    COMMENT 'Human-readable connector name',
  SERVICE             VARCHAR(255)    COMMENT 'Fivetran service type',
  ERROR_REASON        VARIANT         COMMENT 'Full error/status JSON',
  VALIDATED           BOOLEAN         DEFAULT FALSE COMMENT 'Whether re-tested today',
  VALIDATION_RESULT   VARIANT         COMMENT 'JSON result from test API',
  PRIMARY KEY (CHECK_DATE, BROKEN_ID)
);
```

### Staging Table: `FIVETRAN_TLS_BROKEN_STAGING`

Created automatically by the Rewrite Table component in the transformation pipeline. Contains the same columns as the log table minus `VALIDATION_RESULT` (which is NULL at this stage).

### Temporary Table: `FIVETRAN_TLS_DAILY_SUMMARY`

Created at the end of each run with today's summary:

| Column | Type | Description |
|---|---|---|
| `REPORT_DATE` | DATE | Today's date |
| `TOTAL_BROKEN_TODAY` | NUMBER | Total TLS-broken connections found |
| `VALIDATED_TODAY` | NUMBER | Connections where validation was attempted |
| `NOT_VALIDATED` | NUMBER | Connections not yet validated |

---

## 13. Snowflake Prerequisites

Before running the pipeline, the following must be configured in your Snowflake account:

### 1. External Access Integration

```sql
-- Allow outbound HTTPS to Fivetran API
CREATE OR REPLACE NETWORK RULE fivetran_api_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.fivetran.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION FIVETRAN_API_ACCESS
  ALLOWED_NETWORK_RULES = (fivetran_api_rule)
  ENABLED = TRUE;

-- Grant usage to the role used by Matillion
GRANT USAGE ON INTEGRATION FIVETRAN_API_ACCESS TO ROLE <your_matillion_role>;
```

### 2. Snowpark/Python Configuration

Ensure your Snowflake account has:
- Anaconda terms accepted (for Python packages)
- Python UDF execution enabled
- The warehouse used by Matillion has sufficient resources for Snowpark

### 3. Role Permissions

The Matillion execution role needs:
- `CREATE TABLE` on the target schema
- `INSERT`, `UPDATE`, `SELECT` on `FIVETRAN_TLS_BROKEN_LOG`
- `CREATE OR REPLACE TABLE` for staging tables
- `USAGE` on the `FIVETRAN_API_ACCESS` integration

### 4. Slack Webhook (for Slack notifications)

1. Go to [Slack API: Incoming Webhooks](https://api.slack.com/messaging/webhooks)
2. Create a new Slack App or use an existing one.
3. Enable **Incoming Webhooks** and create a webhook for your target channel.
4. Copy the webhook URL (format: `https://hooks.slack.com/services/T.../B.../xxx`).
5. Set the `slack_webhook_url` pipeline variable to this URL at runtime.

> **Note:** The `FIVETRAN_API_ACCESS` external access integration's network rule must also allow `hooks.slack.com:443`. Update the rule:
> ```sql
> CREATE OR REPLACE NETWORK RULE fivetran_api_rule
>   MODE = EGRESS
>   TYPE = HOST_PORT
>   VALUE_LIST = ('api.fivetran.com:443', 'hooks.slack.com:443');
> ```

### 5. Snowflake Email Integration (for email notifications)

```sql
-- Create an email integration
CREATE OR REPLACE NOTIFICATION INTEGRATION tls_fix_email_integration
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('your-team@company.com');

-- Grant usage to Matillion's role
GRANT USAGE ON INTEGRATION tls_fix_email_integration TO ROLE <your_matillion_role>;
```

Set the `notification_email` pipeline variable to the recipient address at runtime.

---

## 14. Error Handling Strategy

### Pipeline-Level

| Scenario | Behavior |
|---|---|
| API fetch fails (network/auth) | Python Pushdown exits with failure → pipeline stops |
| Transformation fails (malformed JSON) | Run Transformation exits with failure → pipeline stops |
| Insert SQL fails | Pipeline stops before iteration |
| Single iteration fails | Iterator continues (break_on_failure=No), failure logged |
| All iterations complete with some failures | Iterator follows success path to summary |

### Sub-Pipeline Level

| Scenario | Behavior |
|---|---|
| API call succeeds | Updates log with response JSON, VALIDATED=TRUE |
| API call throws exception | Python catches error, stores in variable, still updates log |
| Python Pushdown itself fails | Failure path → Log Failure component updates log with error |

### Key Design Principle: No Silent Failures

Every failure is captured in the `VALIDATION_RESULT` column as JSON. Even if the API call completely fails, the log table will contain `{"error": "..."}` so you can investigate.

---

## 15. Scheduling & Notification

### Schedule Configuration

| Setting | Value |
|---|---|
| **Pipeline** | `Daily_Fivetran_TLS_Fix.orch.yaml` |
| **Frequency** | Daily |
| **Time** | 3:00 AM IST = 9:30 PM UTC (previous day) |
| **Variable Overrides** | `fivetran_api_key`, `fivetran_api_secret` |

### Notification Implementation

The pipeline now includes **both Slack and Email notifications** as the final two steps:

| Step | Component | Method | Prerequisite |
|---|---|---|---|
| 9 | Send Slack Notification | `python-pushdown` → Slack Incoming Webhook | Slack App with webhook URL + network rule for `hooks.slack.com` |
| 10 | Send Email Notification | `python-pushdown` → `SYSTEM$SEND_EMAIL()` | Snowflake email notification integration |

**Both are gracefully optional:**
- If `slack_webhook_url` is empty → Slack step prints "Skipping" and succeeds.
- If `notification_email` is empty → Email step prints "Skipping" and succeeds.
- If the Snowflake email integration isn't configured → Email step catches the error, prints the summary to the task log, and succeeds.
- The pipeline **never fails** due to missing notification configuration.

**Variable Overrides at schedule time:**

| Setting | Value |
|---|---|
| **Pipeline** | `Daily_Fivetran_TLS_Fix.orch.yaml` |
| **Frequency** | Daily |
| **Time** | 3:00 AM IST = 9:30 PM UTC (previous day) |
| **Variable Overrides** | `fivetran_api_key`, `fivetran_api_secret`, `slack_webhook_url`, `notification_email` |

**Option C: Matillion Built-in Notifications**  
In addition to the in-pipeline notifications, you can configure Matillion's built-in pipeline completion notifications at the environment level for an extra layer of alerting.

---
