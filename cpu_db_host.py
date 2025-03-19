import asyncio
import logging
import grpc
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
import random
from concurrent import futures
import smalltalk_pb2
import smalltalk_pb2_grpc

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

mongo_client = AsyncIOMotorClient(
    host=DB_HOST,
    username=DB_USER,
    password=DB_PASSWORD,
    authSource=DB_NAME
)
db = mongo_client[DB_NAME]
ranges_collection = db["ranges"]

pending_ranges = []
completed_ranges = []


async def load_ranges():
    global pending_ranges
    limit = 1_000_000  # Ограничиваем 1M участков
    pending_docs = await ranges_collection.find({"status": "pending"}).limit(limit).to_list(length=limit)
    pending_ranges = [{"start": doc["start"], "end": doc["end"]} for doc in pending_docs]
    logger.info("Загружено %d участков в RAM", len(pending_ranges))


async def save_to_db():
    global completed_ranges
    while True:
        if completed_ranges:
            bulk_ops = [UpdateOne({"start": r["start"]}, {"$set": {"status": "completed"}}) for r in completed_ranges]
            await ranges_collection.bulk_write(bulk_ops, ordered=False)
            logger.debug("Сохранено %d завершённых участков", len(completed_ranges))
            completed_ranges = []
        await asyncio.sleep(60)


class SmallTalkServiceServicer(smalltalk_pb2_grpc.SmallTalkServiceServicer):
    async def GetTask(self, request, context):
        if not pending_ranges:
            return smalltalk_pb2.TaskResponse(start="", end="")

        task = random.choice(pending_ranges)
        pending_ranges.remove(task)
        return smalltalk_pb2.TaskResponse(start=task["start"], end=task["end"])

    async def CompleteTask(self, request, context):
        completed_ranges.append({"start": request.start, "end": request.end})
        return smalltalk_pb2.CompleteResponse(status="success")


async def serve():
    await load_ranges()
    asyncio.create_task(save_to_db())

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    smalltalk_pb2_grpc.add_BitCrackServiceServicer_to_server(SmallTalkServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    logger.info("Сервер gRPC запущен на [::]:50051")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
