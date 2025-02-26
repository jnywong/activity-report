import os
import re
import argparse
import requests
from pathlib import Path
from textwrap import dedent
from dotenv import load_dotenv
from dateparser import parse as dateparser_parse
from prometheus_pandas.query import Prometheus
import pandas as pd
from math import ceil
from rich.progress import track
from matplotlib import pyplot as plt

load_dotenv(override=False)
try:
    GRAFANA_TOKEN = os.environ["GRAFANA_TOKEN"]
except KeyError:
    print("Please set the GRAFANA_TOKEN environment variable.")


def get_prometheus_datasources(grafana_url: str, grafana_token: str) -> pd.DataFrame:
    """
    List the datasources available in a Grafana instance.

    Parameters
    ----------
    grafana_url: str
        API URL of Grafana for querying. Must end in a trailing slash.

    grafana_token: str
        Service account token with appropriate rights to make this API call.
    """
    api_url = f"{grafana_url}/api/datasources"
    datasources = requests.get(
        api_url,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {grafana_token}",
        },
    )
    # Convert to a DF so that we can manipulate more easily
    df = pd.DataFrame.from_dict(datasources.json())
    # Move "name" to the first column by setting and resetting it as the index
    df = df.set_index("name").reset_index()
    return df


def get_pandas_prometheus(grafana_url: str, grafana_token: str, prometheus_uid: str):
    """
    Create a Prometheus client and format the result as a pandas data stucture.

    Parameters
    ----------
    grafana_url: str
        URL of Grafana for querying. Must end in a trailing slash.

    grafana_token: str
        Service account token with appropriate rights to make this API call.

    prometheus_uid: str
        uid of Prometheus datasource within grafana to query.
    """

    session = requests.Session()  # Session to use for requests
    session.headers = {"Authorization": f"Bearer {grafana_token}"}

    proxy_url = f"{grafana_url}/api/datasources/proxy/uid/{prometheus_uid}/"  # API URL to query server
    return Prometheus(proxy_url, session)


def query_prometheus(queries, query_start, query_end, step="1d"):
    activity = []
    errors = []
    for queryname, query in queries.items():
        for uid, idata in track(list(df.groupby("uid"))):
            try:
                # Set up prometheus for this cluster and grab the activity
                prometheus = get_pandas_prometheus(
                    "https://grafana.pilot.2i2c.cloud", GRAFANA_TOKEN, uid
                )
                iactivity = prometheus.query_range(
                    query,
                    query_start,
                    query_end,
                    step,
                )
                # Extract hub name from the brackets
                iactivity.columns = [
                    re.findall(r'[^"]+', col)[1] for col in iactivity.columns
                ]
                iactivity.columns.name = "hub"

                # Clean up the timestamp
                iactivity.index.name = "date"

                # Re-work so that we're tidy
                iactivity = iactivity.stack("hub").to_frame("value")
                iactivity = iactivity.reset_index()

                # Add metadata so that we can track this later
                iactivity["cluster"] = idata["name"].squeeze()
                iactivity["query"] = queryname

                # Add to our list so that we concatenate across all clusters
                activity.append(iactivity)
            except Exception as e:
                errors.append((uid, idata["name"].squeeze(), e))
    return activity, errors


def main(args):
    data_path = Path(args.data_path)
    cluster = args.cluster
    hub = args.hub
    date_start = dateparser_parse(args.date_start)
    date_end = dateparser_parse(args.date_end)

    # Fetch all available data sources for our Grafana
    datasources = get_prometheus_datasources(
        "https://grafana.pilot.2i2c.cloud", GRAFANA_TOKEN
    )

    # Filter out only the datasources associated with Prometheus.
    datasources = datasources.query("type == 'prometheus'")

    # Filter out only the datasources associated with the cluster we are interested in
    df = datasources[datasources.name == cluster]

    # Daily users
    queries = {
        "daily": dedent(
            """
            max(
                jupyterhub_active_users{period="24h", namespace=~".*"}
            ) by (namespace)
            """
        ),
    }
    if not data_path.joinpath("daily.csv").exists():
        activity, errors = query_prometheus(queries, query_start=date_start, query_end=date_end, step="1d")
        df_daily = pd.concat(activity)
        df_daily.to_csv(data_path.joinpath("daily.csv"), index=False)
    else:
        df_daily = pd.read_csv(data_path.joinpath("daily.csv"))
    df_daily = df_daily.set_index("date")
    df_daily.index = pd.to_datetime(df_daily.index)
    df_daily.index = df_daily.index.floor("D")
    df_daily = df_daily[df_daily["hub"] == hub]
    df_daily = df_daily.resample("W").sum()[:-1]


    # CPU usage
    queries = {
        "cpu": dedent(
            """
            sum(
            irate(container_cpu_usage_seconds_total{name!="", instance=~".*"}[5m])
            * on (namespace, pod) group_left(container)
            group(
                kube_pod_labels{label_app="jupyterhub", label_component="singleuser-server", namespace=~".*", pod=~".*"}
            ) by (pod, namespace)
            ) by (pod, namespace)
            """
        ),
    }
    query_start = date_start
    list_cpu = []
    # Loop over each month because Prometheus only allows < 11,000 points per time series
    while query_start < date_end:
        query_end = query_start + pd.DateOffset(months=1)
        if not data_path.joinpath(f"cpu_{query_start.strftime('%Y-%m-%d')}_{query_end.strftime('%Y-%m-%d')}.csv").exists():
            activity, errors = query_prometheus(queries, query_start=query_start, query_end=query_end, step="5m") 
            df_cpu = pd.concat(activity)
            list_cpu.append(df_cpu)
            df_cpu.to_csv(data_path.joinpath(f"cpu_{query_start.strftime('%Y-%m-%d')}_{query_end.strftime('%Y-%m-%d')}.csv"), index=False)
        else:
            list_cpu.append(pd.read_csv(data_path.joinpath(f"cpu_{query_start.strftime('%Y-%m-%d')}_{query_end.strftime('%Y-%m-%d')}.csv")))
        query_start = query_end
    df_cpu = pd.concat(list_cpu)
    df_cpu = df_cpu.set_index("date")
    df_cpu.index = pd.to_datetime(df_cpu.index)
    df_cpu = df_cpu[df_cpu["hub"] == hub]
    df_cpu = df_cpu.resample("W").sum()[:-1]


    # Plotting
    bar_width = 6.5
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    ax1.bar(df_daily.index, df_daily["value"], color = 'cornflowerblue', width = bar_width, align = 'center', alpha = 0.8)
    ax2.plot(df_cpu.index, df_cpu["value"], color = 'firebrick', lw=2)
    ax1.set_xlabel('Date', size=12)
    ax2.set_yscale('log')
    ax1.set_ylabel('Weekly Users', color='cornflowerblue',  size=12)
    ax2.set_ylabel('Total CPU usage (seconds)', color='firebrick', size=12)
    date_offset = pd.DateOffset(ceil(0.5*bar_width))
    ax1.set_xlim([df_daily.index.min()-date_offset, df_daily.index.max()+date_offset])
    ax2.set_xlim([df_cpu.index.min()-date_offset, df_cpu.index.max()+date_offset])
    plt.tight_layout()
    fig.savefig("weekly_users_cpu.png")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate activity report")
    parser.add_argument("--data_path", type=str, required=True, help="Path to data directory to store CSV files from Prometheus")
    parser.add_argument("--cluster", type=str, required=True, help="Cluster name")
    parser.add_argument("--hub", type=str, required=True, help="Hub name")
    parser.add_argument("--date_start", type=str, required=True, help="Start date (format: YYYY-MM-DD)")
    parser.add_argument("--date_end", type=str, required=True, help="End date (format: YYYY-MM-DD)")
    args = parser.parse_args()
    main(args)