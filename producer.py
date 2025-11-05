import asyncio
import json
import sys

BROKER_HOST = '127.0.0.1'
BROKER_PORT = 8888

async def send_message(writer, reader):
    TOPIC_NAME = sys.argv[1]
    try:
        loop = asyncio.get_event_loop()
        message_text = await loop.run_in_executor(None, input, f"Enter message to send to topic '{TOPIC_NAME}' (or 'exit'): ")

        if message_text.lower() == 'exit':
            return False # Signal to exit
        
        msg = {
            "type": "publish",
            "topic": TOPIC_NAME,
            "payload": message_text
        }
        
        writer.write((json.dumps(msg) + '\n').encode())
        await writer.drain()
        print(f"Sent: {message_text}")

        ack_data = await reader.readline()
        if not ack_data:
            print("Connection to broker lost (ACK).")
            return False
            
        ack = json.loads(ack_data.decode())
        
        if ack.get('type') == 'ack_publish':
            print(f"Success: Message ACKed by broker (Offset: {ack.get('offset')})")
        else:
            print(f"Error: Received unexpected response from broker: {ack}")
            
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

async def main():
    print(f"Connecting to broker at {BROKER_HOST}:{BROKER_PORT}...")
    try:
        reader, writer = await asyncio.open_connection(
            BROKER_HOST, BROKER_PORT)
    except Exception as e:
        print(f"Failed to connect to broker: {e}")
        return

    print("Connected! Type your message and press Enter.")
    
    while True:
        continue_running = await send_message(writer, reader)
        if not continue_running:
            break
            
    print("Closing connection...")
    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProducer shutting down.")
