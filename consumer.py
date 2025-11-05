import asyncio
import json
import random
import sys
import argparse

BROKER_HOST = '127.0.0.1'
BROKER_PORT = 8888

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="A simple message broker consumer.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        'topic', 
        type=str, 
        help="The topic to subscribe to."
    )
    
    parser.add_argument(
        '-id', '--consumer_id', 
        type=str, 
        default=None, 
        help="A unique, persistent ID for the consumer. If omitted, a random ID is generated."
    )
    
    parser.add_argument(
        '--fail', 
        action='store_true', 
        help="If set, the consumer simulates a 50%% chance of NOT sending an ACK."
    )
    
    return parser.parse_args()

async def main():
    args = parse_arguments()
    
    TOPIC_NAME = args.topic
    fail_rate = 0.5 if args.fail else 0.0

    if args.consumer_id:
        consumer_id = f"consumer_{args.consumer_id}"
        print(f"--- Using persistent consumer_id: {consumer_id} ---")
    else:
        consumer_id = f"consumer_{random.randint(1000, 9999)}"
        print(f"--- No ID provided. Using random consumer_id: {consumer_id} ---")
    
    if fail_rate > 0:
        print(f"--- Starting in 'FAIL' mode ({fail_rate} ACK failure) ---")
    else:
        print(f"--- Starting in 'NORMAL' mode ---")
        
    print(f"Connecting {consumer_id} to broker at {BROKER_HOST}:{BROKER_PORT}...")
    try:
        reader, writer = await asyncio.open_connection(
            BROKER_HOST, BROKER_PORT)
    except Exception as e:
        print(f"Failed to connect to broker: {e}")
        return

    subscribe_msg = {
        "type": "subscribe",
        "topic": TOPIC_NAME,
        "consumer_id": consumer_id
    }
    writer.write((json.dumps(subscribe_msg) + '\n').encode())
    await writer.drain()
    print(f"Subscribed to topic '{TOPIC_NAME}'")

    try:
        while True:
            data = await reader.readline()
            if not data:
                print("Connection to broker lost.")
                break
                
            msg = json.loads(data.decode())

            if msg.get('type') == 'message':
                offset = msg.get('offset')
                payload = msg.get('payload')
                print(f"\n[Offset {offset}] Received: {payload}")
                
                process_time = random.uniform(0.5, 2.0)
                print(f"Processing for {process_time:.1f}s...")
                await asyncio.sleep(process_time)
                
                if random.random() < fail_rate:
                    print(f"*** SIMULATING FAILURE: Not sending ACK for offset {offset} ***")
                    continue 

                print(f"Processing complete. Sending ACK for offset {offset}.")
                ack_msg = {
                    "type": "ack_consume",
                    "topic": TOPIC_NAME,
                    "consumer_id": consumer_id,
                    "offset": offset
                }
                writer.write((json.dumps(ack_msg) + '\n').encode())
                await writer.drain()
            
            elif msg.get('type') == 'error':
                print(f"Broker error: {msg.get('message')}")
            
            else:
                print(f"Received unknown message: {msg}")

    except asyncio.CancelledError:
        print("Consumer task cancelled.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Closing connection...")
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\nConsumer shutting down.")

