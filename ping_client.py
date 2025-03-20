import asyncio
import grpc
import smalltalk_pb2
import smalltalk_pb2_grpc
import logging

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def ping_server():
    async with grpc.aio.insecure_channel('[::]:50051') as channel:
        stub = smalltalk_pb2_grpc.SmallTalkServiceStub(channel)
        request = smalltalk_pb2.PingRequest(message="Hello, SmallTalk!")
        try:
            response = await stub.Ping(request)
            logger.info("Ответ от сервера: %s", response.message)
        except grpc.aio.AioRpcError as e:
            logger.error("Ошибка gRPC: %s", e.details())

if __name__ == "__main__":
    asyncio.run(ping_server())