
import time
import random
from typing import Dict, Any, Callable

# Mock database and message queue
mock_db = {} 
mock_queue = [{"id": 1, "data": 10}, {"id": 2, "data": 20}, {"id": 3, "data": 30}]
messages_to_commit = [] # List of message IDs awaiting commit confirmation

class StreamWeaverProcessor:
    def __init__(self, processor_name: str):
        self.name = processor_name

    def _process_message(self, message: Dict[str, Any]) -> Any:
        """The actual business logic (e.g., transformation, validation)."""
        if random.random() < 0.15 and message['id'] == 2:
            # Simulate a transient failure during processing for message 2
            print(f"🚨 Transient processing failure for Message {message['id']}...")
            raise RuntimeError("Processing failed.")
            
        time.sleep(random.uniform(0.001, 0.003)) # Simulate sub-5ms processing
        new_value = message['data'] * 1.5
        return new_value

    def _db_persist(self, message_id: int, processed_data: Any):
        """MOCK: Persist to storage (guaranteed durability)."""
        # In a real system, this is an ACID transaction
        mock_db[message_id] = {"value": processed_data, "processor": self.name}
        print(f"    [DB] Persisted ID {message_id}. Value: {processed_data:.2f}")

    def consume_and_process(self, message: Dict[str, Any]):
        """
        Implements the exactly-once atomic transactional logic.
        """
        message_id = message['id']
        
        # 1. Check for duplicates (Idempotency Key Check)
        if message_id in mock_db:
            print(f"    [SKIP] Message {message_id} already processed (Exactly-Once check).")
            # Immediately commit the offset if it's a known duplicate
            messages_to_commit.append(message_id)
            return

        try:
            print(f"[START] Processing Message {message_id}...")
            # 2. Process Business Logic
            processed_result = self._process_message(message)
            
            # 3. Persist (Must succeed before commit)
            self._db_persist(message_id, processed_result)
            
            # 4. Success: Flag message for commit
            messages_to_commit.append(message_id)
            print(f"[SUCCESS] Message {message_id} is ready for commit.")

        except Exception as e:
            # 5. Failure: Do NOT flag for commit. Message remains in the queue 
            # and will be re-delivered and re-processed on next run.
            print(f"[FAIL] Message {message_id} failed. Will be retried. Error: {e}")
            pass

    def commit_offsets_mock(self):
        """MOCK: Batch commit the offsets to the Message Queue."""
        global messages_to_commit
        if messages_to_commit:
            print(f"\n[COMMIT] Committing offsets for: {messages_to_commit}")
            # The message queue would now remove these messages
            messages_to_commit = []
        else:
            print("[COMMIT] No new offsets to commit.")

# --- Demonstration ---

processor = StreamWeaverProcessor("FinancialTradeEngine")

# Run 1: Message 2 fails due to transient error
print("--- Run 1: Initial Processing ---")
for msg in mock_queue:
    processor.consume_and_process(msg)
processor.commit_offsets_mock()
print(f"DB State after Run 1: {mock_db}")

# Run 2: Message 2 is re-delivered (it wasn't committed)
print("\n--- Run 2: Retry and Completion ---")
processor.consume_and_process(mock_queue[1]) # Re-process Message 2
processor.commit_offsets_mock()
print(f"DB State after Run 2: {mock_db}")
              
