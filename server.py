import argparse
import asyncio
import logging
import os
import random
from asyncio import subprocess
from asyncio.subprocess import create_subprocess_exec
from pathlib import Path

import aiofiles
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectionResetError
from aiohttp.web_request import Request
from dotenv import load_dotenv

CHUNK_SIZE = 250 * 1024
LOG_FORMAT = "%(name)s %(asctime)s %(levelname)s %(message)s"

logger = logging.getLogger("download_service")

config = {}


def parse_arguments():
    parser = argparse.ArgumentParser(description="File archive microservice")
    parser.add_argument(
        "--files_path", "-p",
        default=os.getenv("FILES_PATH", "test_photos"),
        help="Path to file directory (default: test_photos)",
    )
    parser.add_argument(
        "--enable-delay", "-d",
        action="store_true",
        default=os.getenv("ENABLE_DELAY", "false").lower() == "true",
        help="Enable response delay (default: false)",
    )
    parser.add_argument(
        "--no-logs", "-l",
        action="store_true",
        default=os.getenv("NO_LOGS", "false").lower() == "true",
        help="Disable logging (default: false)",
    )
    return parser.parse_args()


async def archive(request: Request) -> web.StreamResponse:
    archive_hash = request.match_info.get("archive_hash")
    path = Path(config["files_path"]).joinpath(archive_hash)
    if not path.exists():
        raise web.HTTPNotFound(text="The archive does not exist or has been deleted")

    cmd = ["zip", "-r", "-9", "-", "."]
    process = await create_subprocess_exec(*cmd, cwd=path, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    logger.info(f"process[{process.pid}].created")

    response = web.StreamResponse()
    response.headers["Content-Disposition"] = "attachment; filename=archive.zip"
    await response.prepare(request)

    try:
        while True:
            logger.info("Sending archive chunk...")
            chunk = await process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break

            if config["enable_delay"]:
                await asyncio.sleep(random.uniform(2.2, 4.1))

            await response.write(chunk)
    except (ClientConnectionResetError, ConnectionResetError):
        logger.info("Download was interrupted")
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass

        return response


async def handle_index_page(request):
    async with aiofiles.open("index.html", mode="r") as index_file:
        index_contents = await index_file.read()
    return web.Response(text=index_contents, content_type="text/html")


if __name__ == "__main__":
    load_dotenv()
    args = parse_arguments()
    config.update({
        "files_path": args.files_path,
        "enable_delay": args.enable_delay,
        "no_logs": args.no_logs,
    })

    if not args.no_logs:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    else:
        logging.disable(level=logging.CRITICAL)

    app = web.Application()
    app.add_routes([
        web.get("/", handle_index_page),
        web.get("/archive/{archive_hash}/", archive),
    ])
    web.run_app(app)
