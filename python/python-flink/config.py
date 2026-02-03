# config.py
MSK_BOOTSTRAP_SERVERS = (
    "b-1.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
    "b-2.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098,"
    "b-3.workingmultitableclus.nhe1pt.c2.kafka.ap-south-1.amazonaws.com:9098"
)
KAFKA_TOPIC = "bid-events"
S3_WAREHOUSE = "arn:aws:s3tables:ap-south-1:149815625933:bucket/python-saren"
NAMESPACE = "sink"
EVENT_TYPES = [
    "bid_booking_timeout",
    "ride_requested",
    "driver_assigned"
]