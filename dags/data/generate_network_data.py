import pandas as pd
import numpy as np


np.random.seed(42)

number_of_rows = 1000

devices = [
    "ROUTER-01",
    "ROUTER-02",
    "ROUTER-03",
    "ROUTER-04",
    "ROUTER-05",
]

timestamps = pd.date_range(
    start="2026-09-05 08:00:00",
    periods=number_of_rows,
    freq="30s",
)

device_values = np.random.choice(
    devices,
    size=number_of_rows,
)

latency = np.random.normal(
    loc=50,
    scale=15,
    size=number_of_rows,
)

packet_loss = np.random.normal(
    loc=0.5,
    scale=0.3,
    size=number_of_rows,
)

throughput = np.random.normal(
    loc=500,
    scale=80,
    size=number_of_rows,
)

# Prevent impossible negative values
latency = np.maximum(latency, 1)

packet_loss = np.maximum(packet_loss, 0)

throughput = np.maximum(throughput, 1)


# -------------------------------------------------
# Inject artificial anomalies
# -------------------------------------------------

anomaly_indices = np.random.choice(
    number_of_rows,
    size=50,
    replace=False,
)

latency[anomaly_indices] += np.random.uniform(
    150,
    400,
    size=50,
)

packet_loss[anomaly_indices] += np.random.uniform(
    5,
    20,
    size=50,
)

throughput[anomaly_indices] *= np.random.uniform(
    0.1,
    0.4,
    size=50,
)


data = pd.DataFrame(
    {
        "event_time": timestamps,
        "device": device_values,
        "latency_ms": latency.round(2),
        "packet_loss_pct": packet_loss.round(2),
        "throughput_mbps": throughput.round(2),
    }
)


data.to_csv(
    "network_metrics.csv",
    index=False,
)

print("Dataset generated successfully.")
print()
print(data.head())
print()
print(f"Rows created: {len(data)}")