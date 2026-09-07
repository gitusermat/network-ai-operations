import boto3

REGION = "us-east-1"
MODEL_ID = "amazon.nova-lite-v1:0"

client = boto3.client(
    "bedrock-runtime",
    region_name=REGION,
)

incident = """
DEVICE:
ROUTER-03

CURRENT METRICS:
Latency: 310 ms
Packet loss: 9.8%
Throughput: 95 Mbps

RECENT BASELINE:
Latency: approximately 52 ms
Packet loss: approximately 0.4%
Throughput: approximately 500 Mbps

RELATED LOGS:
14:31:02 - BGP neighbor 10.20.1.2 changed from ESTABLISHED to IDLE
14:31:07 - Packet loss increased to 9.8%
14:31:12 - Interface Gi0/1 input errors increasing
14:31:20 - Latency increased to 310 ms
"""

prompt = f"""
You are an AI assistant supporting a network operations engineer.

Analyze the following network incident.

{incident}

Return:
1. Incident summary
2. Three plausible root-cause hypotheses ranked by plausibility
3. Evidence supporting each hypothesis
4. Evidence that is still missing
5. Recommended investigation steps

Rules:
- Do not claim that a root cause is confirmed unless the evidence proves it.
- Clearly separate observations from hypotheses.
- Keep the response technical and concise.
"""

response = client.converse(
    modelId=MODEL_ID,
    messages=[
        {
            "role": "user",
            "content": [
                {
                    "text": prompt
                }
            ],
        }
    ],
    inferenceConfig={
        "maxTokens": 700,
        "temperature": 0.2,
        "topP": 0.9,
    },
)

analysis = response["output"]["message"]["content"][0]["text"]

print("\n========================================")
print("AI-ASSISTED NETWORK ROOT-CAUSE ANALYSIS")
print("========================================\n")

print(analysis)

usage = response.get("usage", {})

print("\n========================================")
print("TOKEN USAGE")
print("========================================")

print(f"Input tokens: {usage.get('inputTokens')}")
print(f"Output tokens: {usage.get('outputTokens')}")
print(f"Total tokens: {usage.get('totalTokens')}")