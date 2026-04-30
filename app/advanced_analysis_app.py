import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from flask import Flask, render_template, jsonify, request, Response
import pandas as pd
import plotly
import plotly.graph_objs as go
import json
import numpy as np
import uuid
import tempfile
import io

from src.detection.magnet_detector import MagnetQuenchDetector

app = Flask(__name__)
app.secret_key = os.urandom(24)

UPLOAD_FOLDER = os.path.join(tempfile.gettempdir(), "lhc_uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500 MB

COLUMN_ALIASES = {
    "magnet_id":    ["magnet_id", "magnet", "id", "device", "element", "name"],
    "timestamp":    ["timestamp", "time", "datetime", "date", "t"],
    "voltage_diff": ["voltage_diff", "voltage", "v", "volt", "dv", "delta_v"],
    "resistance":   ["resistance", "r", "res", "ohm"],
    "temperature":  ["temperature", "temp", "t_kelvin", "kelvin", "t_k"],
    "quench_flag":  ["quench_flag", "quench", "flag", "label", "anomaly", "fault"],
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "csv"


def resolve_columns(df_cols):
    cols_lower = {c.lower(): c for c in df_cols}
    mapping = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in cols_lower:
                mapping[canonical] = cols_lower[alias]
                break
    return mapping


def load_csv_chunked(filepath, chunksize=50_000):
    chunks = []
    for chunk in pd.read_csv(filepath, chunksize=chunksize, low_memory=False):
        chunks.append(chunk)
    return pd.concat(chunks, ignore_index=True)


def prepare_dataframe(df, col_map):
    rename = {v: k for k, v in col_map.items()}
    df = df.rename(columns=rename)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)
    for col in ["voltage_diff", "resistance", "temperature"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "quench_flag" not in df.columns:
        df["quench_flag"] = 0
    if "magnet_id" not in df.columns:
        df["magnet_id"] = "MAG-001"
    return df


def run_detection(df):
    normal_df = df[df["quench_flag"] == 0].copy() if "quench_flag" in df.columns else df.copy()
    detector = MagnetQuenchDetector(use_persistence=False)
    detector.fit(normal_df)
    return detector.detect_batch(df)


def processed_path(safe_id):
    """Path where we cache the processed (post-detection) parquet file."""
    return os.path.join(UPLOAD_FOLDER, f"{safe_id}_processed.parquet")


def get_processed_df(safe_id):
    """
    Return the processed DataFrame.
    Uses cached parquet if available, otherwise re-runs the full pipeline.
    Raises FileNotFoundError if the original CSV is also gone.
    """
    ppath = processed_path(safe_id)
    if os.path.exists(ppath):
        df = pd.read_parquet(ppath)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        return df

    # Fall back to re-processing from raw CSV
    raw_path = os.path.join(UPLOAD_FOLDER, f"{safe_id}.csv")
    if not os.path.exists(raw_path):
        raise FileNotFoundError("Uploaded file not found. Please re-upload.")

    df_raw = load_csv_chunked(raw_path)
    col_map = resolve_columns(df_raw.columns.tolist())
    df = prepare_dataframe(df_raw, col_map)
    df = run_detection(df)

    # Cache it
    df_to_save = df.copy()
    if "timestamp" in df_to_save.columns:
        df_to_save["timestamp"] = df_to_save["timestamp"].astype(str)
    df_to_save.to_parquet(ppath, index=False)

    return df


# ── Plot helpers ─────────────────────────────────────────────────────────────

def fig_to_json(fig):
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


DARK = dict(template="plotly_dark", paper_bgcolor="#0d1117",
            plot_bgcolor="#0d1117", font=dict(color="#c9d1d9"))


def plot_multi_param(df, param):
    traces = [go.Scatter(x=df[df["magnet_id"] == mid]["timestamp"],
                         y=df[df["magnet_id"] == mid][param],
                         mode="lines", name=mid, line=dict(width=1))
              for mid in sorted(df["magnet_id"].unique())]
    fig = go.Figure(data=traces)
    fig.update_layout(title=f"Multi-Magnet: {param.replace('_',' ').title()}",
                      xaxis_title="Time", yaxis_title=param,
                      hovermode="closest", **DARK)
    return fig_to_json(fig)


def plot_correlation_heatmap(df):
    try:
        pivot = df.pivot_table(index="timestamp", columns="magnet_id",
                               values="temperature", aggfunc="mean")
        corr = pivot.corr()
        fig = go.Figure(data=go.Heatmap(z=corr.values,
                                        x=corr.columns.tolist(),
                                        y=corr.index.tolist(),
                                        colorscale="Plasma",
                                        colorbar=dict(title="Corr")))
        fig.update_layout(title="Temperature Correlation Between Magnets", **DARK)
        return fig_to_json(fig)
    except Exception:
        return fig_to_json(go.Figure())


def plot_anomaly_distribution(df):
    counts = df[df["detected_quench_flag"] == 1]["magnet_id"].value_counts().sort_index()
    fig = go.Figure(data=[go.Bar(x=counts.index.tolist(), y=counts.values.tolist(),
                                 marker=dict(color=counts.values.tolist(),
                                             colorscale="Reds", showscale=True))])
    fig.update_layout(title="Quench Distribution by Magnet",
                      xaxis_title="Magnet ID", yaxis_title="Quench Count", **DARK)
    return fig_to_json(fig)


def plot_quench_timeline(df):
    quenches = df[df["detected_quench_flag"] == 1]
    fig = go.Figure()
    for mid in sorted(df["magnet_id"].unique()):
        q = quenches[quenches["magnet_id"] == mid]
        fig.add_trace(go.Scatter(x=q["timestamp"], y=[mid] * len(q),
                                 mode="markers", name=mid,
                                 marker=dict(size=8, symbol="x", color="red")))
    fig.update_layout(title="Quench Event Timeline",
                      xaxis_title="Time", yaxis_title="Magnet",
                      hovermode="closest", **DARK)
    return fig_to_json(fig)


def plot_rolling_stats(df, magnet_id, param, window=50):
    mdf = df[df["magnet_id"] == magnet_id].copy()
    mdf["rm"] = mdf[param].rolling(window=window, min_periods=1).mean()
    mdf["rs"] = mdf[param].rolling(window=window, min_periods=1).std().fillna(0)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=mdf["timestamp"], y=mdf[param], mode="lines",
                             name="Raw", line=dict(color="#58a6ff", width=1)))
    fig.add_trace(go.Scatter(x=mdf["timestamp"], y=mdf["rm"], mode="lines",
                             name=f"Mean({window})", line=dict(color="#f0883e", width=2)))
    fig.add_trace(go.Scatter(x=mdf["timestamp"], y=mdf["rm"] + 2 * mdf["rs"],
                             mode="lines", name="+2σ", line=dict(color="#f85149", dash="dash")))
    fig.add_trace(go.Scatter(x=mdf["timestamp"], y=mdf["rm"] - 2 * mdf["rs"],
                             mode="lines", name="-2σ", line=dict(color="#f85149", dash="dash"),
                             fill="tonexty", fillcolor="rgba(248,81,73,0.08)"))
    fig.update_layout(title=f"{magnet_id} — {param.replace('_',' ').title()} Rolling Stats",
                      xaxis_title="Time", yaxis_title=param,
                      hovermode="closest", **DARK)
    return fig_to_json(fig)


def plot_param_histogram(df, param):
    fig = go.Figure()
    for mid in sorted(df["magnet_id"].unique()):
        fig.add_trace(go.Histogram(x=df[df["magnet_id"] == mid][param],
                                   name=mid, opacity=0.7, nbinsx=60))
    fig.update_layout(barmode="overlay",
                      title=f"Distribution: {param.replace('_',' ').title()}",
                      xaxis_title=param, yaxis_title="Count", **DARK)
    return fig_to_json(fig)


def get_kpis(df):
    total = len(df)
    quenches = int(df["detected_quench_flag"].sum())
    magnets = int(df["magnet_id"].nunique())
    anomaly_rate = round(quenches / total * 100, 2) if total > 0 else 0
    time_span = ""
    if "timestamp" in df.columns and not df["timestamp"].isna().all():
        t_min = df["timestamp"].min()
        t_max = df["timestamp"].max()
        time_span = (f"{t_min.strftime('%Y-%m-%d %H:%M')} "
                     f"→ {t_max.strftime('%Y-%m-%d %H:%M')}")
    return {
        "total_records":     f"{total:,}",
        "total_quenches":    f"{quenches:,}",
        "magnets_monitored": magnets,
        "anomaly_rate":      f"{anomaly_rate}%",
        "time_span":         time_span,
    }


def get_summary_table(df):
    rows = []
    for mid in sorted(df["magnet_id"].unique()):
        mdf = df[df["magnet_id"] == mid]
        rows.append({
            "magnet_id":        mid,
            "count":            f"{len(mdf):,}",
            "avg_voltage":      round(float(mdf["voltage_diff"].mean()), 6)
                                if "voltage_diff" in mdf.columns else "N/A",
            "avg_resistance":   round(float(mdf["resistance"].mean()), 8)
                                if "resistance" in mdf.columns else "N/A",
            "avg_temp":         round(float(mdf["temperature"].mean()), 3)
                                if "temperature" in mdf.columns else "N/A",
            "true_quenches":    int(mdf["quench_flag"].sum())
                                if "quench_flag" in mdf.columns else "N/A",
            "detected_quenches": int(mdf["detected_quench_flag"].sum()),
        })
    return rows


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("upload.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename or not allowed_file(f.filename):
        return jsonify({"error": "Invalid file. Please upload a .csv file"}), 400

    file_id = str(uuid.uuid4())
    save_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.csv")
    f.save(save_path)

    try:
        peek = pd.read_csv(save_path, nrows=5)
        col_map = resolve_columns(peek.columns.tolist())
        missing = [k for k in ["magnet_id", "timestamp", "voltage_diff",
                                "resistance", "temperature"]
                   if k not in col_map]
        return jsonify({
            "file_id":           file_id,
            "columns":           peek.columns.tolist(),
            "col_map":           col_map,
            "missing_canonical": missing,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/analyze/<file_id>")
def analyze(file_id):
    safe_id = os.path.basename(file_id)
    try:
        df = get_processed_df(safe_id)
    except FileNotFoundError as e:
        return str(e), 404
    except Exception as e:
        return f"Processing error: {e}", 500

    params = [p for p in ["voltage_diff", "resistance", "temperature"]
              if p in df.columns]
    magnet_ids = sorted(df["magnet_id"].unique().tolist())

    multi_param_graphs  = {p: plot_multi_param(df, p) for p in params}
    correlation_heatmap = plot_correlation_heatmap(df) if "temperature" in params else None
    anomaly_distribution = plot_anomaly_distribution(df)
    quench_timeline      = plot_quench_timeline(df)
    histograms           = {p: plot_param_histogram(df, p) for p in params}
    rolling_graphs       = {mid: {p: plot_rolling_stats(df, mid, p)
                                  for p in params}
                            for mid in magnet_ids}

    return render_template(
        "advanced_analysis.html",
        kpis=get_kpis(df),
        summary_stats=get_summary_table(df),
        magnet_ids=magnet_ids,
        params=params,
        multi_param_graphs=multi_param_graphs,
        correlation_heatmap=correlation_heatmap,
        anomaly_distribution=anomaly_distribution,
        quench_timeline=quench_timeline,
        histograms=histograms,
        rolling_graphs=rolling_graphs,
        file_id=safe_id,
    )


@app.route("/report/csv/<file_id>")
def report_csv(file_id):
    """Download all detected anomaly rows as a CSV file."""
    safe_id = os.path.basename(file_id)
    try:
        df = get_processed_df(safe_id)
    except FileNotFoundError as e:
        return str(e), 404
    except Exception as e:
        return f"Error: {e}", 500

    anomalies = df[df["detected_quench_flag"] == 1].copy()
    if anomalies.empty:
        # Return a CSV with just the header if no anomalies found
        anomalies = df.iloc[0:0].copy()

    if "timestamp" in anomalies.columns:
        anomalies["timestamp"] = anomalies["timestamp"].astype(str)

    buf = io.StringIO()
    anomalies.to_csv(buf, index=False)
    buf.seek(0)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition":
                f"attachment; filename=lhc_anomalies_{safe_id[:8]}.csv"
        }
    )


@app.route("/report/full/<file_id>")
def report_full(file_id):
    """Download the full analysis report (all rows + detection flag) as CSV."""
    safe_id = os.path.basename(file_id)
    try:
        df = get_processed_df(safe_id)
    except FileNotFoundError as e:
        return str(e), 404
    except Exception as e:
        return f"Error: {e}", 500

    out = df.copy()
    if "timestamp" in out.columns:
        out["timestamp"] = out["timestamp"].astype(str)

    buf = io.StringIO()
    out.to_csv(buf, index=False)
    buf.seek(0)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition":
                f"attachment; filename=lhc_full_report_{safe_id[:8]}.csv"
        }
    )


@app.route("/api/data/<file_id>")
def api_data(file_id):
    safe_id = os.path.basename(file_id)
    try:
        df = get_processed_df(safe_id)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    page     = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))
    total    = len(df)
    start    = (page - 1) * per_page
    page_df  = df.iloc[start: start + per_page].copy()
    if "timestamp" in page_df.columns:
        page_df["timestamp"] = page_df["timestamp"].astype(str)

    return jsonify({
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "data":     page_df.to_dict(orient="records"),
    })


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5002)
