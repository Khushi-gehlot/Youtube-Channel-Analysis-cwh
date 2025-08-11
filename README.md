# 📊 CWH YouTube Channel Analytics (ETL + SQLite + Power BI)

This project is an **end-to-end YouTube analytics pipeline** that extracts, transforms, and loads (ETL) data from cwh's YouTube channel using the **YouTube Data API v3**, stores it in **SQLite**, and visualizes the insights in **Power BI**.

---

## 🚀 Project Workflow

### 1. **Extract**
- Fetches **video metadata** (title, publish date, views, likes, comments) from **CWH's YouTube channel** using the YouTube Data API.
- API results are stored in a **pandas DataFrame**.

### 2. **Transform**
- Cleans and formats data:
  - Converts publish dates to datetime
  - Calculates **days since publish**
  - Calculates **engagement rate**:  
    \[
    \text{Engagement Rate} = \frac{\text{Likes} + \text{Comments}}{\text{Views}}
    \]
  - Calculates **views per day**
- Saves the transformed dataset as a CSV file for backup.

### 3. **Load**
- Loads the transformed data into a **SQLite database** (`youtube_data.db`) for persistent storage.
- Table name: `cwh_videos`.

### 4. **Visualize**
- Connects SQLite database to **Power BI** to create interactive dashboards:
  - **KPIs**: Total views, likes, comments, engagement rate
  - **Top videos by views**
  - **Views & engagement trends over time**
  - **Scatter plot**: Views vs Engagement rate

---

## 🛠 Tech Stack

| Component  | Technology |
|------------|------------|
| Language   | Python 3   |
| Data Source| YouTube Data API v3 |
| Database   | SQLite3    |
| Dashboard  | Power BI   |
| Platform   | Databricks |

---


