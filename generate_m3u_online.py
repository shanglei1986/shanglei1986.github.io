import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

# 本地 JSON 数据文件
INPUT_JSON_FILE = (
    "/docker-compose-files/File_server/"
    "IPTVAgentCode/ppv_live_go/streams.json"
)

TMP_JSON_FILE = INPUT_JSON_FILE + ".tmp"

DOWNLOAD_URLS = (
    "https://api.ppv.to/api/streams",
    "https://api.ppv.st/api/streams",
    "https://api.ppv.cx/api/streams",
    "https://api.ppv.is/api/streams",
    "https://api.ppv.lc/api/streams",
)

DOWNLOAD_RETRY_COUNT = 3
DOWNLOAD_RETRY_DELAY = 5
DOWNLOAD_TIMEOUT = 120

BASE_STREAM_URL = "http://192.168.123.159:11458/stream?uri="

# 输出目录
OUTPUT_DIR = "/docker-compose-files/File_server/IPTVAgentCode/m3u_files"
OUTPUT_FILE = "ppv_live.m3u"

# 北京时间 UTC+8
TZ = timezone(timedelta(hours=8))


def log(message):
    """输出带北京时间时间戳的简洁日志。"""
    timestamp = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {message}", flush=True)


def short_host(url):
    """从 URL 中提取主机名，用于日志显示。"""
    return urlparse(url).netloc


def cleanup_tmp_json():
    """清理下载临时文件。"""
    try:
        os.remove(TMP_JSON_FILE)
    except FileNotFoundError:
        pass


def download_streams_json():
    """下载 streams.json，校验 JSON 成功后原子替换本地文件。"""
    os.makedirs(os.path.dirname(INPUT_JSON_FILE), exist_ok=True)
    cleanup_tmp_json()

    last_error = None

    for url in DOWNLOAD_URLS:
        host = short_host(url)

        for attempt in range(1, DOWNLOAD_RETRY_COUNT + 1):
            cleanup_tmp_json()

            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"}
                )

                with urllib.request.urlopen(
                    request,
                    timeout=DOWNLOAD_TIMEOUT
                ) as response:
                    content = response.read()

                if not content:
                    raise ValueError("下载内容为空")

                # 先校验 JSON，成功后才写入并替换正式文件
                json.loads(content.decode("utf-8"))

                with open(TMP_JSON_FILE, "wb") as file:
                    file.write(content)

                os.replace(TMP_JSON_FILE, INPUT_JSON_FILE)
                size = os.path.getsize(INPUT_JSON_FILE)

                return host, size

            except Exception as error:
                last_error = error
                cleanup_tmp_json()

                if attempt < DOWNLOAD_RETRY_COUNT:
                    time.sleep(DOWNLOAD_RETRY_DELAY)

    raise RuntimeError(f"所有 API 下载 streams.json 均失败：{last_error}")


def load_streams():
    """从本地 streams.json 读取数据。"""
    if not os.path.isfile(INPUT_JSON_FILE):
        raise FileNotFoundError(f"找不到 JSON 文件：{INPUT_JSON_FILE}")

    with open(INPUT_JSON_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def format_time_range(starts_at, ends_at):
    """
    输出格式：
    02/23 08:30-11:00
    """
    if not starts_at or not ends_at:
        return ""

    start_time = datetime.fromtimestamp(starts_at, TZ)
    end_time = datetime.fromtimestamp(ends_at, TZ)

    date_part = start_time.strftime("%m/%d")
    time_part = (
        f"{start_time.strftime('%H:%M')}-"
        f"{end_time.strftime('%H:%M')}"
    )

    return f"{date_part} {time_part}"


def add_m3u_entry(lines, category_name, item, parent=None):
    name = item.get("name") or (parent or {}).get("name", "No Name")
    uri_name = item.get("uri_name")

    if not uri_name:
        return

    # substream 通常没有 poster / starts_at / ends_at，所以从父级继承
    logo = item.get("poster") or (parent or {}).get("poster", "")
    starts_at = item.get("starts_at") or (parent or {}).get("starts_at")
    ends_at = item.get("ends_at") or (parent or {}).get("ends_at")

    source_tag = item.get("source_tag", "")

    # 避免多个 substream 都显示同一个名字
    if source_tag:
        name = f"{name} [{source_tag}]"

    time_range = format_time_range(starts_at, ends_at)

    if time_range:
        name = f"{time_range} {name}"

    stream_url = BASE_STREAM_URL + uri_name

    extinf = (
        f'#EXTINF:-1 '
        f'group-title="{category_name}" '
        f'tvg-logo="{logo}",'
        f'{name}'
    )

    lines.append(extinf)
    lines.append(stream_url)


def generate_m3u(data):
    lines = ["#EXTM3U"]

    for category_block in data.get("streams", []):
        category_name = category_block.get("category", "Other")

        for stream in category_block.get("streams", []):
            # 先生成父级 stream
            add_m3u_entry(lines, category_name, stream)

            # 再生成 substreams
            for substream in stream.get("substreams", []) or []:
                add_m3u_entry(
                    lines,
                    category_name,
                    substream,
                    parent=stream
                )

    return "\n".join(lines) + "\n"


def save_m3u(content):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    temp_path = file_path + ".tmp"

    # 先写临时文件，成功后再替换正式文件
    with open(temp_path, "w", encoding="utf-8", newline="\n") as file:
        file.write(content)

    os.replace(temp_path, file_path)

    return file_path


def main():
    try:
        host, size = download_streams_json()
        data = load_streams()
        m3u_content = generate_m3u(data)
        save_m3u(m3u_content)

        log(f"OK {host} -> streams.json {size} bytes -> 生成{OUTPUT_FILE}")

    except FileNotFoundError as error:
        log(f"FAIL {error}")
        raise SystemExit(1)

    except json.JSONDecodeError as error:
        log(
            f"FAIL JSON 格式错误：第 {error.lineno} 行，"
            f"第 {error.colno} 列：{error.msg}"
        )
        raise SystemExit(1)

    except Exception as error:
        log(f"FAIL {error}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
