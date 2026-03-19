import json
from typing import Any
import uuid
from confluent_kafka import Producer

producer_config = { 'bootstrap.servers': 'localhost:9092' }

producer = Producer(producer_config)

def delivery_report(error: Any, message: Any):
  if error:
    print(f'❌ Delivery failed: {error}')
  else:
    print(f'✅ Delivered: {message.value().decode('utf-8')}')
    print(f'Delivered to topic "{message.topic()}", partition: {message.partition()}, at offset {message.offset()}')

order: dict[str, Any] = {
  'order_id': str(uuid.uuid4()),
  'user': 'guilhermepk',
  'item': 'x doritos',
  'quantity': 2
}

string_order = json.dumps(order).encode('utf-8')

producer.produce(
  topic = 'orders',
  value = string_order,
  callback = delivery_report
)

producer.flush()