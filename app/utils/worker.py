from queue import Queue, Empty
import asyncio
import threading
from Parallelization.scheduler import scheduler

global task_queue
task_queue = Queue()

def worker_main(stop_event: threading.Event):
    asyncio.run(worker(stop_event))

async def worker(stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            args = task_queue.get(timeout=1)
            print("From worker (processing task):", args)
            try:
                await scheduler(*args)
            except Exception as e:
                print(f"Task failed: {e}")
            finally:
                task_queue.task_done()
        except Empty:
            pass
        except Exception as e:
            print(f"An unexpected error occurred in worker loop: {e}")
            if 'args' in locals():
                task_queue.task_done()
    
    print("Worker: Stop event received.")