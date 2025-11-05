import asyncio
import json
import os
import time
from collections import defaultdict

BROKER_HOST = '127.0.0.1'
BROKER_PORT = 8888
WAL_LOG_FILE = 'broker_wal.log'
OFFSETS_FILE = 'broker_offsets.json'
DLQ_LOG_FILE = 'broker_dlq.log' 
ACK_TIMEOUT = 10  
POLL_SLEEP = 0.1 
MAX_RETRIES = 3   

class Broker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.topics = defaultdict(list)
        self.offsets = defaultdict(lambda: defaultdict(int))
        self.un_acked = {}
        self.consumer_tasks = {}
        self.retry_counts = {}
        self.lock = asyncio.Lock()
        
        print(f"Broker starting up. PID: {os.getpid()}")

    async def load_state(self):
        async with self.lock:
            if os.path.exists(WAL_LOG_FILE):
                print(f"Loading state from {WAL_LOG_FILE}...")
                with open(WAL_LOG_FILE, 'r') as f:
                    for line in f:
                        try:
                            msg = json.loads(line.strip())
                            topic = msg.get('topic')
                            if topic:
                                self.topics[topic].append(msg['payload'])
                        except json.JSONDecodeError:
                            print(f"Warning: Skipping corrupt log line: {line.strip()}")
                
                topic_counts = {t: len(m) for t, m in self.topics.items()}
                print(f"Loaded {sum(topic_counts.values())} messages across {len(topic_counts)} topics.")
                if topic_counts:
                    print(f"Topic counts: {topic_counts}")

            
            if os.path.exists(OFFSETS_FILE):
                print(f"Loading offsets from {OFFSETS_FILE}...")
                with open(OFFSETS_FILE, 'r') as f:
                    try:
                        
                        loaded_offsets = json.load(f)
                        for consumer_id, topics in loaded_offsets.items():
                            for topic, offset in topics.items():
                                self.offsets[consumer_id][topic] = offset
                        print(f"Loaded offsets for {len(self.offsets)} consumers.")
                    except json.JSONDecodeError:
                        print(f"Warning: Could not parse {OFFSETS_FILE}. Starting with fresh offsets.")
            
            print("State load complete.")

    async def _save_offsets(self):        
        try:
            temp_file = f"{OFFSETS_FILE}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.offsets, f, indent=2)
            
            
            if os.path.exists(OFFSETS_FILE):
                os.replace(temp_file, OFFSETS_FILE)
            else:
                os.rename(temp_file, OFFSETS_FILE)
                
        except Exception as e:
            print(f"CRITICAL: Failed to save offsets! Error: {e}")
            

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"New connection from {addr}")
        consumer_id = None 
        sender_task = None 
        try:
            while True:
                data = await reader.readline()
                if not data:
                    break  
                
                try:
                    msg = json.loads(data.decode())
                    msg_type = msg.get('type')
                    
                    if msg_type == 'publish':
                        await self.handle_publish(msg, writer)
                    
                    elif msg_type == 'subscribe':
                        
                        consumer_id = msg.get('consumer_id')
                        topic = msg.get('topic')

                        if not topic or not consumer_id:
                            await self.send_error(writer, "Subscribe message requires 'topic' and 'consumer_id'.")
                            break 

                        
                        sender_task = asyncio.create_task(
                            self.run_consumer_sender(msg, writer)
                        )
                        
                        self.consumer_tasks[consumer_id] = sender_task
                        
                        
                        print(f"Consumer '{consumer_id}' subscribed. This connection is now for ACKs.")

                    elif msg_type == 'ack_consume':
                        
                        await self.handle_ack_consume(msg)
                    
                    else:
                        await self.send_error(writer, f"Unknown message type: {msg_type}")
                        
                except json.JSONDecodeError:
                    await self.send_error(writer, "Invalid JSON message.")
                except Exception as e:
                    print(f"Error handling client {addr}: {e}")
                    await self.send_error(writer, f"Internal server error: {e}")

        except asyncio.CancelledError:
            print(f"Connection {addr} cancelled.")
        except ConnectionResetError:
            print(f"Connection {addr} reset by peer.")
        finally:
            print(f"Closing connection from {addr}")
            
            if sender_task and not sender_task.done():
                print(f"Stopping sender task for {consumer_id}")
                sender_task.cancel()
            if consumer_id and consumer_id in self.consumer_tasks:
                del self.consumer_tasks[consumer_id]
                
            writer.close()
            await writer.wait_closed()

    async def handle_publish(self, msg, writer):
        topic = msg.get('topic')
        payload = msg.get('payload')
        
        if not topic or payload is None:
            await self.send_error(writer, "Publish message requires 'topic' and 'payload'.")
            return

        async with self.lock:
            
            try:
                with open(WAL_LOG_FILE, 'a') as f:
                    f.write(json.dumps(msg) + '\n')
                    f.flush() 
            except Exception as e:
                print(f"CRITICAL: Failed to write to WAL! Error: {e}")
                await self.send_error(writer, "Failed to persist message.")
                return

            
            self.topics[topic].append(payload)
            offset = len(self.topics[topic]) - 1

            
            ack_msg = {
                "type": "ack_publish",
                "topic": topic,
                "offset": offset
            }
            await self.send_json(writer, ack_msg)
            

    async def run_consumer_sender(self, msg, writer):
        topic = msg.get('topic')
        consumer_id = msg.get('consumer_id')

        print(f"Starting sender task for '{consumer_id}' on topic '{topic}'")
        
        try:
            while True:
                
                async with self.lock:
                    current_offset = self.offsets[consumer_id][topic]
                    un_acked_key = (consumer_id, topic, current_offset)
                    
                    
                    
                    if un_acked_key in self.un_acked:
                        last_sent_time = self.un_acked[un_acked_key]
                        if (time.time() - last_sent_time) > ACK_TIMEOUT:
                            self.retry_counts[un_acked_key] = self.retry_counts.get(un_acked_key, 0) + 1
                            attempt_count = self.retry_counts[un_acked_key]
                            
                            payload = self.topics[topic][current_offset]

                            if attempt_count > MAX_RETRIES:
                                
                                print(f"CRITICAL: Max retries ({MAX_RETRIES}) exceeded for {un_acked_key}. Moving to DLQ.")
                                
                                
                                self.move_to_dlq(consumer_id, topic, current_offset, payload)
                                
                                
                                del self.un_acked[un_acked_key]
                                del self.retry_counts[un_acked_key]
                                self.offsets[consumer_id][topic] = current_offset + 1
                                await self._save_offsets()
                                print(f"Queue unblocked. New offset for {consumer_id} is {current_offset + 1}")

                            else:
                                
                                
                                print(f"Timeout (Attempt {attempt_count}/{MAX_RETRIES}): Re-sending {topic} offset {current_offset} to {consumer_id}")
                                self.un_acked[un_acked_key] = time.time() 
                                await self.send_message(writer, topic, current_offset, payload)
                    
                    
                    elif current_offset < len(self.topics[topic]):
                        
                        payload = self.topics[topic][current_offset]
                        
                        self.un_acked[un_acked_key] = time.time()
                        
                        await self.send_message(writer, topic, current_offset, payload)

                await asyncio.sleep(POLL_SLEEP)

        except (ConnectionResetError, asyncio.CancelledError) as e:
            print(f"Sender task for {consumer_id} stopping ({type(e).__name__}).")
        except Exception as e:
            print(f"CRITICAL Error in consumer sender for {consumer_id}: {e}")
            
        finally:
            print(f"Consumer sender for {consumer_id} has finished.")
            
    def move_to_dlq(self, consumer_id, topic, offset, payload):
        try:
            dlq_message = {
                "timestamp": time.time(),
                "failed_consumer": consumer_id,
                "topic": topic,
                "offset": offset,
                "payload": payload,
                "reason": f"Exceeded {MAX_RETRIES} retries."
            }
            with open(DLQ_LOG_FILE, 'a') as f:
                f.write(json.dumps(dlq_message) + '\n')
                f.flush()
        except Exception as e:
            print(f"CRITICAL: FAILED TO WRITE TO DLQ FILE! Error: {e}")

    async def handle_ack_consume(self, msg):
        """Handles an 'ack_consume' message from a consumer."""
        topic = msg.get('topic')
        consumer_id = msg.get('consumer_id')
        offset = msg.get('offset')

        if not all([topic, consumer_id, offset is not None]):
            
            print(f"Warning: Received invalid ACK: {msg}")
            return
        
        async with self.lock:
            un_acked_key = (consumer_id, topic, offset)
            
            if un_acked_key in self.un_acked:
                del self.un_acked[un_acked_key]
                if un_acked_key in self.retry_counts:
                    del self.retry_counts[un_acked_key]
                
                if self.offsets[consumer_id][topic] == offset:
                    self.offsets[consumer_id][topic] = offset + 1
                    print(f"ACK received: {consumer_id}, {topic}, {offset}. New offset: {offset+1}")
                
                    await self._save_offsets()
            else:
                #stale or duplicate ACK
                pass
                

    async def send_json(self, writer, data):
        """Helper to send a JSON message, ending with a newline."""
        try:
            payload = json.dumps(data) + '\n'
            writer.write(payload.encode())
            await writer.drain()
        except ConnectionResetError:
            print("Failed to send: Connection reset")

    async def send_error(self, writer, error_message):
        """Helper to send an error message."""
        print(f"Sending error: {error_message}")
        await self.send_json(writer, {"type": "error", "message": error_message})
        
    async def send_message(self, writer, topic, offset, payload):
        """Helper to send a 'message' to a consumer."""
        await self.send_json(writer, {
            "type": "message",
            "topic": topic,
            "offset": offset,
            "payload": payload
        })

    async def run(self):
        """Loads state and starts the main server."""
        await self.load_state()
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)

        addrs = ', '.join(str(s.getsockname()) for s in server.sockets)
        print(f'Broker started. Serving on {addrs}...')

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    broker = Broker(BROKER_HOST, BROKER_PORT)
    try:
        asyncio.run(broker.run())
    except KeyboardInterrupt:
        print("\nBroker shutting down.")