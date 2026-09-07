def calculate_anomaly_score(
    latency_z,
    packet_loss_z,
    throughput_z
):
    throughput_drop_score = -throughput_z

    return (
        max(latency_z, 0)
        + max(packet_loss_z, 0)
        + max(throughput_drop_score, 0)
    )


def classify_anomaly(score):
    if score >= 6:
        return "ANOMALY"
    elif score >= 3:
        return "WARNING"
    return "NORMAL"


def test_normal_event():
    score = calculate_anomaly_score(
        latency_z=0.5,
        packet_loss_z=0.2,
        throughput_z=0.3,
    )

    assert classify_anomaly(score) == "NORMAL"


def test_warning_event():
    score = calculate_anomaly_score(
        latency_z=2.0,
        packet_loss_z=1.5,
        throughput_z=0,
    )

    assert classify_anomaly(score) == "WARNING"


def test_anomaly_event():
    score = calculate_anomaly_score(
        latency_z=3.0,
        packet_loss_z=2.5,
        throughput_z=-2.0,
    )

    assert classify_anomaly(score) == "ANOMALY"


def test_throughput_drop_increases_score():
    score = calculate_anomaly_score(
        latency_z=0,
        packet_loss_z=0,
        throughput_z=-4,
    )

    assert score == 4