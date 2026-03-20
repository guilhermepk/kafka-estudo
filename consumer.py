import json
from typing import Any, Literal
from confluent_kafka import Consumer

consumer_config: dict[str, Any] = {
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'order-tracker',
  'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print('🟢 Consumer is running and subscribed to the "orders" topic')

try:
  while True:
    message = consumer.poll(1.0)

    if message is None:
      continue

    else:
      if message.error():
        print(f'❌ Consumer error: {message.error()}')
        continue
      
      else:
        message_value: bytes | None = message.value()

        if message_value is None:
          print(f'\n💥 The received message is empty')
        
        else:
          value: str = message_value.decode('utf-8')

          data: dict[str, Any] = json.loads(value)

          user: str = data.get('user', 'N/A')
          item: str = data.get('item', 'N/A')
          quantity: int | Literal['N/A'] = data.get('quantity', 'N/A')
          order_id: str = data.get('order_id', 'N/A')

          print(f'\n📜 The user "{user}" ordered {quantity} "{item}". Order ID: {order_id}')

except KeyboardInterrupt:
  print('\n💔 Consumer shutting down...')

finally:
  consumer.close()