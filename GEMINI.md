# eCourts & Tribunal Scraper Backend

## Project Overview

This project is a Python-based backend service designed to provide a unified API for accessing case status and details from various Indian judicial systems. It interfaces with the eCourts mobile API (for District and High Courts) and directly scrapes websites for other tribunals and the Supreme Court.

It features a FastAPI-based REST API and a background worker for periodic updates of tracked cases stored in Supabase.

### Key Features
-   **Unified Search:** Search cases across multiple court types (DC, HC, SCI, NCLT, NCLAT, etc.).
-   **eCourts Integration:** Reverse-engineered client for the official eCourts mobile app API (`app.ecourts.gov.in`), handling AES encryption/decryption.
-   **Tribunal Scrapers:** specialized scrapers for:
    -   Supreme Court of India (SCI) - *Uses `ddddocr` for CAPTCHA*
    -   NCLT & NCLAT
    -   CESTAT, DRT, ITAT, NCDRC, APTEL
-   **Background Sync:** A cron task (`cron_task.py`) that monitors cases stored in Supabase and refreshes their status if a hearing is upcoming.

## Architecture

-   **`router.py`**: The FastAPI entry point defining API routes.
-   **`ecourts.py`**: Core service for handling eCourts mobile API logic (encryption/decryption).
-   **`hc_services.py`**: Specialized logic for High Court services.
-   **`scrapers/`**: Directory containing individual scraper modules for various tribunals.
-   **`cron_task.py`**: Script meant to be run periodically (e.g., via cron) to update case statuses in the Supabase database.

## Technologies

-   **Language:** Python
-   **Framework:** FastAPI
-   **Database:** Supabase (PostgreSQL)
-   **Scraping:** `requests`, `BeautifulSoup`
-   **CAPTCHA Solving:** `ddddocr`
-   **Crypto:** `pycryptodome` (AES encryption for eCourts API)
-   **Resilience:** `tenacity` (for retries)

## Setup and Usage

### Prerequisites
-   Python 3.8+
-   Supabase project (for background tasks)

### Environment Variables
Create a `.env` file (loaded by `dotenv`) with:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

### Installation
(Inferred dependencies)
```bash
pip install fastapi uvicorn requests beautifulsoup4 pycryptodome ddddocr tenacity supabase python-dotenv
```

### Running the API
```bash
python router.py
# OR
uvicorn ecourts.router:router --host 0.0.0.0 --port 8000
```
*Note: The `router.py` includes a `if __name__ == "__main__":` block to run via `uvicorn` directly.*

### Running Background Tasks
```bash
python cron_task.py
```

## Development Conventions

-   **Scrapers:** Each scraper is a standalone module in `scrapers/` but should expose a consistent interface (often returning a list of dictionaries).
-   **Normalization:** `cron_task.py` handles normalizing data from different sources into a common schema for Supabase.
-   **Encryption:** All communication with `app.ecourts.gov.in` must be encrypted using the logic in `ecourts.py`.
-   **Logging:** Basic logging is configured in `cron_task.py` and `ecourts.py`.
