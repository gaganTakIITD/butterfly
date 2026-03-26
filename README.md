# 🔄 Workflow Orchestrator — Autonomous Data Pipeline Agent

An AI agent that manages **end-to-end execution of multi-step data pipeline workflows** from a single natural language instruction. Built on the [A2A (Agent-to-Agent) protocol](https://github.com/google/A2A) and powered by any OpenAI-compatible LLM (default: **Groq Llama 3.3 70B**).

> *Give it a plain-English instruction and it will fetch, validate, clean, transform, filter, visualise, report, and deliver — autonomously handling failures with intelligent retry logic and fallback chains.*

---

## 🎯 What It Does

Give it an instruction like:

> *"Fetch last week's sales data, summarise it by region, generate a bar chart, and email the report to the sales team."*

The agent will:

1. **Decompose** the goal into ordered steps mapped to specific tools
2. **Execute** each step, chaining outputs from one tool to the next
3. **Detect failures** and **replan** with different parameters or fallback tools (max 2 retries)
4. **Escalate** gracefully if a step cannot be resolved
5. **Generate a premium HTML report** with gradient KPI cards, CSS bar charts, and styled data tables
6. **Email the report** — embedded natively as the HTML body with a downloadable file attachment
7. **Produce a structured execution log** showing every step, its outcome, retries, and final status

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Natural Language Input                   │
│  "Fetch sales data, group by region, chart it, email it"   │
└──────────────────────────┬─────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────┐
│        LLM Orchestration Brain (OpenAI-compatible)           │
│     Default: Groq Llama 3.3 70B  •  Swappable via env       │
│                                                              │
│  1. Decompose → 2. Execute → 3. Retry/Fallback → 4. Log     │
└──────────────────────────┬───────────────────────────────────┘
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                   14 Domain Tools (5 Layers)                 │
│                                                              │
│  Layer 1 — INGESTION                                         │
│    fetch_data ──FAIL──► fetch_data_from_cache                 │
│                                                              │
│  Layer 2 — PROCESSING                                        │
│    validate_data → clean_data → transform_data               │
│                                                              │
│  Layer 3 — FILTERING                                         │
│    filter_data                                               │
│                                                              │
│  Layer 4 — VISUALISATION                                     │
│    generate_chart ──FAIL──► generate_text_table               │
│                                                              │
│  Layer 5 — DELIVERY                                          │
│    compose_report → export_report                            │
│    dispatch_email ──FAIL──► dispatch_to_file                  │
│                                                              │
│  ORCHESTRATION                                               │
│    check_health │ log_step                                   │
└──────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| **Premium HTML Reports** | Auto-generated reports with gradient KPI cards, pure-CSS horizontal bar charts, styled data tables, Inter font typography, and a dark gradient header |
| **Smart Email Delivery** | HTML reports rendered natively in the email body AND attached as a downloadable `.html` file |
| **Failure-Resilient Pipelines** | Every critical path has a guaranteed-success fallback — the pipeline never fully fails |
| **Structured Execution Logging** | Every tool call is logged with timestamps, retry counts, and status to a persistent JSON audit trail |
| **Provider-Agnostic** | Works with any OpenAI-compatible API (Groq, OpenAI, Together AI, Ollama, etc.) via `OPENAI_BASE_URL` |

---

## 🧰 14 Tools Reference

### Layer 1 — Data Ingestion

| Tool | Description | Fallback |
|------|-------------|----------|
| `fetch_data(source, time_range, filters)` | Fetches data from mock source (sales/inventory/users). Generates realistic datasets. | ➡️ `fetch_data_from_cache` |
| `fetch_data_from_cache(source, time_range)` | Reads cached data from disk. If no cache exists, generates minimal fallback data. **Never fails.** | — |

### Layer 2 — Data Processing

| Tool | Description |
|------|-------------|
| `validate_data(data_json)` | Checks schema, nulls, duplicates, negative values. Returns validation report. |
| `clean_data(data_json, strategy)` | Cleans data. Strategies: `drop_nulls`, `fill_zero`, `fill_mean`. |
| `transform_data(data_json, group_by, aggregation)` | Groups and aggregates. Aggregations: `sum`, `avg`, `count`, `min`, `max`. |

### Layer 3 — Filtering

| Tool | Description |
|------|-------------|
| `filter_data(data_json, conditions)` | Filters rows by conditions like `amount > 1000, region == North`. |

### Layer 4 — Visualisation

| Tool | Description | Fallback |
|------|-------------|----------|
| `generate_chart(data_json, chart_type, title, x_axis, y_axis)` | Generates data for CSS bar/line charts. | ➡️ `generate_text_table` |
| `generate_text_table(data_json, title, columns)` | Formatted ASCII table. **Never fails.** | — |

### Layer 5 — Reporting & Delivery

| Tool | Description | Fallback |
|------|-------------|----------|
| `compose_report(...)` | Generates a premium HTML report with KPI cards, CSS charts, styled tables, gradient header, and branded footer. | — |
| `export_report(report_content, filename, format)` | Exports report to disk as `.html`, `.md`, `.txt`, or `.json`. Intelligently extracts clean HTML from JSON payloads. | — |
| `dispatch_email(recipient, subject, body, attachment_path)` | Sends via SMTP — embeds HTML body + attaches file. Falls back to file log if SMTP is not configured. | ➡️ `dispatch_to_file` |
| `dispatch_to_file(content, recipient, output_dir)` | Writes report to `./reports/`. **Never fails.** | — |

### Orchestration Support

| Tool | Description |
|------|-------------|
| `check_health(service_name)` | Checks tool readiness (disk, env vars, SMTP). |
| `log_step(step_number, step_name, tool_used, status, details, retry_count)` | Logs each step to persistent audit trail. |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- A Groq API key (free at [console.groq.com](https://console.groq.com)) or any OpenAI-compatible API key

### Run with Docker

```bash
# 1. Create the Docker network (one-time)
docker network create agents-net

# 2. Set your API key in docker-compose.yml (line 8)

# 3. Build and start the agent
docker compose up -d --build

# 4. Find the mapped port
docker port workflow_orchestrator 5000
```

### Send a Test Request

```bash
curl -X POST http://localhost:<PORT>/ \
  -H "Content-Type: application/json" \
  -d @payload.json
```

Or compose your own:

```bash
curl -X POST http://localhost:<PORT>/ \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "message/send",
    "id": "test-1",
    "params": {
      "message": {
        "messageId": "msg-1",
        "role": "user",
        "parts": [{
          "kind": "text",
          "text": "Fetch last week sales data, summarise by region, generate a bar chart, compose a HTML report, export it, and email it to sales@company.com"
        }]
      }
    }
  }'
```

---

## 🧪 Sample Test Scenarios

| # | Instruction | Tests |
|---|-------------|-------|
| 1 | *"Fetch last week's sales data, summarise it by region, generate a bar chart, and email the report to sales@company.com"* | Full pipeline, all 5 layers |
| 2 | *"Pull inventory data, clean it, filter items below reorder level, and export a report as JSON"* | Filtering + export |
| 3 | *"Get user sign-up data for last month, group by plan type, generate a chart, compose a report, and dispatch to stakeholders"* | Multi-group aggregation |

To test failure handling, set `SIMULATE_FAILURES=true` in `docker-compose.yml`.

---

## 📊 Generated Report Preview

The agent autonomously generates premium HTML reports with:

- 🎨 **Dark gradient header** with "Workflow Orchestrator" branding
- 📊 **5 KPI cards** — Total Revenue, Orders, Quantity, Regions, Avg Revenue
- 📈 **Pure-CSS horizontal bar charts** with unique gradient colors per category
- 📋 **Styled data tables** with gradient headers and zebra-striped rows
- ✨ **Inter font** from Google Fonts for premium typography
- 🔻 **Branded footer** with gradient text

---

## 🗂️ Project Structure

```
workflow-orchestrator/
├── src/
│   ├── __init__.py                  # Package marker
│   ├── __main__.py                  # Entry point — AgentCard, skills, A2A server
│   ├── openai_agent.py              # Agent config — toolset + system prompt
│   ├── openai_agent_executor.py     # A2A executor (SDK integration)
│   └── workflow_toolset.py          # ★ 14 domain tools + HTML report engine
├── .workflow_cache/                 # Test data (JSON files)
├── reports/                         # Generated reports & email logs (auto-created)
├── docker-compose.yml               # Container config + env vars
├── Dockerfile                       # Python 3.11-slim image
├── payload.json                     # Sample test payload
├── test_prompt.txt                  # Human-readable test prompt
├── pyproject.toml                   # Python project metadata
└── README.md                        # This file
```

---

## ⚙️ Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENAI_API_KEY` | ✅ Yes | — | API key (Groq, OpenAI, or any compatible provider) |
| `OPENAI_BASE_URL` | No | OpenAI default | Base URL for provider (e.g. `https://api.groq.com/openai/v1`) |
| `MODEL_NAME` | No | `gpt-4o` | Model name (e.g. `llama-3.3-70b-versatile`) |
| `SIMULATE_FAILURES` | No | `false` | Set `true` to randomly trigger tool failures for testing |
| `SMTP_HOST` | No | — | SMTP server for real email dispatch |
| `SMTP_PORT` | No | `587` | SMTP port |
| `SMTP_USER` | No | — | SMTP username (e.g. Gmail address) |
| `SMTP_PASS` | No | — | SMTP app password |

> **Note:** If SMTP is not configured, `dispatch_email` automatically falls back to writing email logs to `./reports/`. No configuration needed for basic operation.

---

## 🛡️ Fallback Chains

Every critical path has a guaranteed-success fallback:

```
fetch_data       ──FAIL──►  fetch_data_from_cache   (reads local cache / generates minimal data)
generate_chart   ──FAIL──►  generate_text_table     (ASCII table, always works)
dispatch_email   ──FAIL──►  dispatch_to_file        (writes to ./reports/, always works)
```

The pipeline **never fully fails** — it degrades gracefully.

---

## 📝 License

Built for AgenticAI Hackathon 2026 — [Nasiko Platform](https://nasiko.io).