from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import io
import json
import os

app = FastAPI(title="MAXIMUS - Excel Visualizer")

# Allow frontend to talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend HTML
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Accepts .xlsx, .xls, .csv files
    Returns: sheets, headers, rows, stats, chart data
    """
    filename = file.filename.lower()
    contents = await file.read()

    try:
        # ── Read file based on extension ──────────────────────────────────
        if filename.endswith(".csv"):
            df_dict = {"Sheet1": pd.read_csv(io.BytesIO(contents))}


        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            xls = None
            for engine in ["openpyxl", "xlrd"]:
                try:
                    xls = pd.ExcelFile(io.BytesIO(contents), engine=engine)
                    break
                except Exception:
                    continue
            if xls is None:
                raise HTTPException(status_code=400, detail="Could not read Excel file.")
            df_dict = {
                sheet: xls.parse(sheet)
                for sheet in xls.sheet_names
            }
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format. Use .xlsx, .xls or .csv")

        # ── Process each sheet ────────────────────────────────────────────
        result = {
            "filename": file.filename,
            "sheets": []
        }

        for sheet_name, df in df_dict.items():
            # Clean up
            df = df.dropna(how="all").reset_index(drop=True)
            df.columns = [str(c).strip() for c in df.columns]

            # Replace NaN with None for JSON serialization
            df = df.where(pd.notnull(df), None)

            headers = list(df.columns)
            rows    = df.head(500).values.tolist()  # max 500 rows for table

            # ── Stats ─────────────────────────────────────────────────────
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            stats = {
                "total_rows":    len(df),
                "total_cols":    len(headers),
                "numeric_cols":  len(numeric_cols),
                "missing_vals":  int(df.isnull().sum().sum()),
            }

            # ── Numeric column stats (for summary cards) ──────────────────
            col_stats = {}
            for col in numeric_cols:
                s = df[col].dropna()
                if len(s) == 0:
                    continue
                col_stats[col] = {
                    "mean":   round(float(s.mean()), 2),
                    "min":    round(float(s.min()),  2),
                    "max":    round(float(s.max()),  2),
                    "sum":    round(float(s.sum()),  2),
                    "median": round(float(s.median()), 2),
                    "std":    round(float(s.std()),  2),
                }

            # ── Chart data ────────────────────────────────────────────────
            chart_data = {}

            # Bar / Line chart — first text col as labels, all numeric as datasets
            # Bar / Line chart — smart label detection (skip date columns)
            def is_date_col(series):
                try:
                    converted = pd.to_datetime(series.dropna().head(10), infer_datetime_format=True)
                    return True
                except:
                    return False

            label_col = None
            # First preference: non-numeric, non-date column (like CHANNEL, REGION etc.)
            for col in headers:
                if col not in numeric_cols:
                    if not is_date_col(df[col]):
                        label_col = col
                        break
            # Second preference: if no text col found, use date col
            if label_col is None:
                for col in headers:
                    if col not in numeric_cols:
                        label_col = col
                        break

            if label_col:
                labels = df[label_col].astype(str).head(50).tolist()
            else:
                labels = [str(i+1) for i in range(min(50, len(df)))]

            datasets = []
            colors   = ["#6c63ff","#ff6584","#43e97b","#f7971e",
                        "#4facfe","#f093fb","#fa709a","#fee140","#30cfd0"]
            for i, col in enumerate(numeric_cols[:6]):
                datasets.append({
                    "label": col,
                    "data":  df[col].head(50).fillna(0).round(2).tolist(),
                    "color": colors[i % len(colors)]
                })

            chart_data["main"] = {"labels": labels, "datasets": datasets}

            # Pie / Doughnut — distribution of first text column categories
            # Pie / Doughnut — skip date columns, pick meaningful category col
            cat_cols = [c for c in headers if c not in numeric_cols]
            cat_col = None
            for col in cat_cols:
                if not is_date_col(df[col]):
                    unique_count = df[col].nunique()
                    if 2 <= unique_count <= 30:  # meaningful category (not too many unique values)
                        cat_col = col
                        break
            if cat_col is None and cat_cols:
                cat_col = cat_cols[0]  # fallback to first col

            if cat_col:
                # Remove total/summary rows
                filtered = df[cat_col].astype(str)
                filtered = filtered[~filtered.str.lower().isin(['total', 'grand total', 'subtotal', 'sum', 'overall'])]
                vc = filtered.value_counts().head(10)
                chart_data["pie"] = {
                    "labels": vc.index.astype(str).tolist(),
                    "data":   vc.values.tolist(),
                    "colors": colors[:len(vc)]
                }

            # Distribution histogram — first numeric column
            if numeric_cols:
                col  = numeric_cols[0]
                vals = df[col].dropna()
                counts, bin_edges = np.histogram(vals, bins=10)
                chart_data["histogram"] = {
                    "labels": [f"{round(b, 1)}" for b in bin_edges[:-1]],
                    "data":   counts.tolist(),
                    "col":    col
                }

            # Column averages
            if numeric_cols:
                chart_data["averages"] = {
                    "labels": numeric_cols[:8],
                    "data":   [round(float(df[c].mean()), 2) for c in numeric_cols[:8]]
                }

            result["sheets"].append({
                "name":      sheet_name,
                "headers":   headers,
                "rows":      rows,
                "stats":     stats,
                "col_stats": col_stats,
                "chart_data": chart_data,
                "numeric_cols": numeric_cols,
                "label_col":    label_col,
            })

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.get("/health")
async def health():
    return {"status": "ok", "message": "MAXIMUS API is running"}


