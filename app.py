import json
import os
import socket
import sqlite3
import subprocess
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DOCKERSYNC_DATA_DIR", BASE_DIR / "data"))
DB_PATH = DATA_DIR / "dockersync.db"
HOST = os.environ.get("DOCKERSYNC_HOST", "0.0.0.0")
PORT = int(os.environ.get("DOCKERSYNC_PORT", "8080"))
RUNNING_PROCESSES = {}
PROCESS_LOCK = threading.Lock()


def log_stdout(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS target_registries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                provider TEXT DEFAULT 'Custom',
                registry_url TEXT NOT NULL,
                project TEXT DEFAULT '',
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                is_default INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_image TEXT NOT NULL,
                target_registry_id INTEGER NOT NULL,
                target_image TEXT NOT NULL,
                use_proxy INTEGER DEFAULT 0,
                status TEXT DEFAULT 'PENDING',
                current_step TEXT DEFAULT '等待开始',
                progress INTEGER DEFAULT 0,
                log_output TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(target_registry_id) REFERENCES target_registries(id)
            );
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(target_registries)")}
        migrations = {
            "provider": "ALTER TABLE target_registries ADD COLUMN provider TEXT DEFAULT 'Custom'",
            "project": "ALTER TABLE target_registries ADD COLUMN project TEXT DEFAULT ''",
            "is_default": "ALTER TABLE target_registries ADD COLUMN is_default INTEGER DEFAULT 0",
        }
        for column, sql in migrations.items():
            if column not in columns:
                conn.execute(sql)
        task_columns = {row[1] for row in conn.execute("PRAGMA table_info(sync_tasks)")}
        task_migrations = {
            "current_step": "ALTER TABLE sync_tasks ADD COLUMN current_step TEXT DEFAULT '等待开始'",
            "progress": "ALTER TABLE sync_tasks ADD COLUMN progress INTEGER DEFAULT 0",
            "use_proxy": "ALTER TABLE sync_tasks ADD COLUMN use_proxy INTEGER DEFAULT 0",
        }
        for column, sql in task_migrations.items():
            if column not in task_columns:
                conn.execute(sql)


def rows_to_dicts(rows):
    return [dict(row) for row in rows]


def get_settings():
    with db() as conn:
        rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    data = {row["key"]: row["value"] for row in rows}
    return {
        "proxy_enabled": data.get("proxy_enabled", "false") == "true",
        "proxy_url": normalize_proxy_url(data.get("proxy_url", "")),
        "extra_no_proxy": data.get("extra_no_proxy", "localhost,127.0.0.1"),
        "src_tls_verify": data.get("src_tls_verify", "true") == "true",
        "dest_tls_verify": data.get("dest_tls_verify", "true") == "true",
    }


def normalize_proxy_url(proxy_url):
    proxy_url = proxy_url.strip().split()[0] if proxy_url and proxy_url.strip() else ""
    if proxy_url.startswith("socks5:") and not proxy_url.startswith("socks5://"):
        return "socks5://" + proxy_url.removeprefix("socks5:").lstrip("/")
    return proxy_url


def parse_proxy_endpoint(proxy_url):
    parsed = urlparse(normalize_proxy_url(proxy_url))
    if parsed.scheme not in ("http", "https", "socks5") or not parsed.hostname or not parsed.port:
        raise ValueError("代理地址格式应为 http://host:port 或 socks5://host:port")
    return parsed.hostname, parsed.port


def check_proxy_connectivity(proxy_url):
    parsed = urlparse(normalize_proxy_url(proxy_url))
    host, port = parse_proxy_endpoint(proxy_url)
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.settimeout(5)
        if parsed.scheme == "socks5":
            sock.sendall(b"\x05\x01\x00")
            reply = sock.recv(2)
            if len(reply) != 2 or reply[0] != 5 or reply[1] == 0xFF:
                raise ValueError("SOCKS5 握手失败")
    return True


def save_settings(payload):
    values = {
        "proxy_enabled": "true" if payload.get("proxy_enabled") else "false",
        "proxy_url": normalize_proxy_url(payload.get("proxy_url", "")),
        "extra_no_proxy": payload.get("extra_no_proxy", "localhost,127.0.0.1").strip(),
        "src_tls_verify": "true" if payload.get("src_tls_verify", True) else "false",
        "dest_tls_verify": "true" if payload.get("dest_tls_verify", True) else "false",
    }
    with db() as conn:
        conn.executemany(
            "INSERT INTO app_settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            values.items(),
        )


def normalize_source_image(image):
    image = image.strip()
    first = image.split("/", 1)[0]
    has_registry = "." in first or ":" in first or first == "localhost"
    return image if has_registry else f"docker.io/{image}"


def build_target_image(registry_url, project, target_image):
    image = target_image.strip().lstrip("/")
    project = (project or "").strip().strip("/")
    parts = [registry_url.strip().rstrip("/")]
    if project and not image.startswith(project + "/"):
        parts.append(project)
    parts.append(image)
    return "/".join(part for part in parts if part)


def append_log(task_id, text):
    with db() as conn:
        conn.execute(
            "UPDATE sync_tasks SET log_output = COALESCE(log_output, '') || ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (text, task_id),
        )


def update_task(task_id, status=None, current_step=None, progress=None):
    fields = []
    values = []
    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if current_step is not None:
        fields.append("current_step = ?")
        values.append(current_step)
    if progress is not None:
        fields.append("progress = ?")
        values.append(progress)
    if not fields:
        return
    values.append(task_id)
    with db() as conn:
        conn.execute(
            f"UPDATE sync_tasks SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values,
        )


def set_task_status(task_id, status):
    update_task(task_id, status=status)


def run_sync(task_id):
    with db() as conn:
        task = conn.execute(
            """
            SELECT t.*, r.registry_url, r.project, r.username, r.password
            FROM sync_tasks t
            JOIN target_registries r ON r.id = t.target_registry_id
            WHERE t.id = ?
            """,
            (task_id,),
        ).fetchone()

    if not task:
        return

    settings = get_settings()
    source = normalize_source_image(task["source_image"])
    target = build_target_image(task["registry_url"], task["project"], task["target_image"])
    command = [
        "skopeo",
        "copy",
        f"--src-tls-verify={'true' if settings['src_tls_verify'] else 'false'}",
        f"--dest-tls-verify={'true' if settings['dest_tls_verify'] else 'false'}",
        "--dest-creds",
        f"{task['username']}:{task['password']}",
        f"docker://{source}",
        f"docker://{target}",
    ]

    env = os.environ.copy()
    proxy_url = normalize_proxy_url(settings["proxy_url"])
    use_proxy = bool(task["use_proxy"])
    proxy_active = use_proxy and proxy_url
    if proxy_active:
        env["HTTP_PROXY"] = proxy_url
        env["HTTPS_PROXY"] = proxy_url
        env["http_proxy"] = proxy_url
        env["https_proxy"] = proxy_url
        no_proxy = [task["registry_url"].split("/", 1)[0], settings["extra_no_proxy"]]
        env["NO_PROXY"] = ",".join([item for item in no_proxy if item])
        env["no_proxy"] = env["NO_PROXY"]

    update_task(task_id, status="RUNNING", current_step="准备执行 skopeo copy", progress=10)
    log_stdout(f"任务 #{task_id} 开始同步：docker://{source} -> docker://{target}")
    log_stdout(f"任务 #{task_id} 代理：{'本任务启用，仅源仓库拉取生效；目标仓库走 NO_PROXY' if proxy_active else '本任务未启用'}")
    append_log(task_id, f"开始同步：docker://{source} -> docker://{target}\n")
    append_log(task_id, f"代理：{'本任务启用，仅源仓库拉取生效；目标仓库走 NO_PROXY' if proxy_active else '本任务未启用'}\n")
    if use_proxy and not proxy_url:
        log_stdout(f"任务 #{task_id} 已勾选代理，但代理地址为空")
        append_log(task_id, "代理地址为空：请先在设置中配置代理地址。\n")
    if proxy_active:
        log_stdout(f"任务 #{task_id} 代理地址：{proxy_url}，NO_PROXY：{env.get('NO_PROXY', '')}")
        append_log(task_id, f"代理地址：{proxy_url}\nNO_PROXY：{env.get('NO_PROXY', '')}\n")
    append_log(task_id, "\n")

    try:
        update_task(task_id, current_step="启动 skopeo 进程", progress=20)
        log_stdout(f"任务 #{task_id} 启动 skopeo copy 进程")
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        with PROCESS_LOCK:
            RUNNING_PROCESSES[task_id] = proc
        update_task(task_id, current_step="复制镜像层", progress=35)
        for line in proc.stdout:
            append_log(task_id, line)
            log_stdout(f"任务 #{task_id} skopeo: {line.rstrip()}")
            lower = line.lower()
            if "copying blob" in lower:
                update_task(task_id, current_step="复制镜像层", progress=55)
            elif "copying config" in lower:
                update_task(task_id, current_step="复制镜像配置", progress=75)
            elif "writing manifest" in lower or "copying manifest" in lower:
                update_task(task_id, current_step="写入镜像清单", progress=90)
        code = proc.wait()
        with PROCESS_LOCK:
            RUNNING_PROCESSES.pop(task_id, None)
        if code == 0:
            log_stdout(f"任务 #{task_id} 同步成功")
            append_log(task_id, "\n同步完成。\n")
            update_task(task_id, status="SUCCESS", current_step="同步完成", progress=100)
        elif code < 0:
            log_stdout(f"任务 #{task_id} 已取消，skopeo 退出码：{code}")
            append_log(task_id, "\n任务已取消。\n")
            update_task(task_id, status="CANCELLED", current_step="任务已取消", progress=0)
        else:
            log_stdout(f"任务 #{task_id} 同步失败，skopeo 退出码：{code}")
            append_log(task_id, f"\nskopeo 退出码：{code}\n")
            update_task(task_id, status="FAILED", current_step="同步失败，请查看日志", progress=100)
    except FileNotFoundError:
        with PROCESS_LOCK:
            RUNNING_PROCESSES.pop(task_id, None)
        log_stdout(f"任务 #{task_id} 同步失败：未找到 skopeo")
        append_log(task_id, "错误：未找到 skopeo，请先安装 skopeo 并确保它在 PATH 中。\n")
        update_task(task_id, status="FAILED", current_step="未找到 skopeo", progress=100)
    except Exception as exc:
        with PROCESS_LOCK:
            RUNNING_PROCESSES.pop(task_id, None)
        log_stdout(f"任务 #{task_id} 执行异常：{exc}")
        append_log(task_id, f"错误：{exc}\n")
        update_task(task_id, status="FAILED", current_step="执行异常，请查看日志", progress=100)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            body = INDEX_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/registries":
            with db() as conn:
                rows = conn.execute(
                    "SELECT id, name, provider, registry_url, project, username, password, is_default, created_at FROM target_registries ORDER BY is_default DESC, id DESC"
                ).fetchall()
            self.send_json(rows_to_dicts(rows))
            return

        if path == "/api/settings":
            self.send_json(get_settings())
            return

        if path == "/api/tasks":
            with db() as conn:
                rows = conn.execute(
                    """
                    SELECT t.*, r.name AS registry_name, r.registry_url, r.project
                    FROM sync_tasks t
                    JOIN target_registries r ON r.id = t.target_registry_id
                    ORDER BY t.id DESC
                    """
                ).fetchall()
            self.send_json(rows_to_dicts(rows))
            return

        if path.startswith("/api/tasks/"):
            task_id = path.rsplit("/", 1)[-1]
            with db() as conn:
                row = conn.execute("SELECT * FROM sync_tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                self.send_json({"error": "任务不存在"}, 404)
                return
            self.send_json(dict(row))
            return

        self.send_json({"error": "Not found"}, 404)

    def do_POST(self):
        path = urlparse(self.path).path
        payload = self.read_json()

        if path == "/api/registries":
            required = ["name", "registry_url", "username", "password"]
            if any(not payload.get(key) for key in required):
                self.send_json({"error": "请填写完整仓库信息"}, 400)
                return
            is_default = 1 if payload.get("is_default") else 0
            with db() as conn:
                if is_default:
                    conn.execute("UPDATE target_registries SET is_default = 0")
                cursor = conn.execute(
                    """
                    INSERT INTO target_registries(name, provider, registry_url, project, username, password, is_default)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["name"].strip(),
                        payload.get("provider", "Custom").strip(),
                        payload["registry_url"].strip().removeprefix("https://").removeprefix("http://"),
                        payload.get("project", "").strip().strip("/"),
                        payload["username"].strip(),
                        payload["password"],
                        is_default,
                    ),
                )
            self.send_json({"id": cursor.lastrowid})
            return

        if path == "/api/settings":
            save_settings(payload)
            self.send_json(get_settings())
            return

        if path == "/api/settings/proxy-check":
            proxy_url = normalize_proxy_url(payload.get("proxy_url", ""))
            try:
                check_proxy_connectivity(proxy_url)
            except Exception as exc:
                self.send_json({"ok": False, "proxy_url": proxy_url, "error": str(exc)}, 400)
                return
            self.send_json({"ok": True, "proxy_url": proxy_url})
            return

        if path.startswith("/api/registries/") and path.endswith("/default"):
            item_id = path.split("/")[-2]
            with db() as conn:
                conn.execute("UPDATE target_registries SET is_default = 0")
                conn.execute("UPDATE target_registries SET is_default = 1 WHERE id = ?", (item_id,))
            self.send_json({"ok": True})
            return

        if path.startswith("/api/registries/") and path.endswith("/update"):
            item_id = path.split("/")[-2]
            required = ["name", "registry_url", "username", "password"]
            if any(not payload.get(key) for key in required):
                self.send_json({"error": "请填写完整仓库信息"}, 400)
                return
            is_default = 1 if payload.get("is_default") else 0
            with db() as conn:
                if is_default:
                    conn.execute("UPDATE target_registries SET is_default = 0")
                conn.execute(
                    """
                    UPDATE target_registries
                    SET name = ?, provider = ?, registry_url = ?, project = ?, username = ?, password = ?, is_default = ?
                    WHERE id = ?
                    """,
                    (
                        payload["name"].strip(),
                        payload.get("provider", "Custom").strip(),
                        payload["registry_url"].strip().removeprefix("https://").removeprefix("http://"),
                        payload.get("project", "").strip().strip("/"),
                        payload["username"].strip(),
                        payload["password"],
                        is_default,
                        item_id,
                    ),
                )
            self.send_json({"ok": True})
            return

        if path == "/api/tasks/cancel":
            task_ids = [int(item) for item in payload.get("ids", [])]
            with db() as conn:
                rows = conn.execute(
                    f"SELECT id, status FROM sync_tasks WHERE id IN ({','.join(['?'] * len(task_ids))})" if task_ids else "SELECT id, status FROM sync_tasks WHERE 0",
                    task_ids,
                ).fetchall()
            for row in rows:
                task_id = row["id"]
                if row["status"] in ("SUCCESS", "FAILED", "CANCELLED"):
                    continue
                with PROCESS_LOCK:
                    proc = RUNNING_PROCESSES.get(task_id)
                if proc and proc.poll() is None:
                    proc.terminate()
                    log_stdout(f"任务 #{task_id} 收到取消请求，正在终止 skopeo 进程")
                    append_log(task_id, "\n收到取消请求，正在终止 skopeo 进程。\n")
                    update_task(task_id, status="CANCELLING", current_step="正在取消", progress=0)
                else:
                    log_stdout(f"任务 #{task_id} 已取消：未找到运行中的 skopeo 进程")
                    update_task(task_id, status="CANCELLED", current_step="任务已取消", progress=0)
            self.send_json({"ok": True})
            return

        if path == "/api/tasks":
            required = ["source_image", "target_registry_id", "target_image"]
            if any(not payload.get(key) for key in required):
                self.send_json({"error": "请填写完整同步任务"}, 400)
                return
            with db() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO sync_tasks(source_image, target_registry_id, target_image, use_proxy, current_step, progress)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (payload["source_image"].strip(), int(payload["target_registry_id"]), payload["target_image"].strip(), 1 if payload.get("use_proxy") else 0, "等待启动", 0),
                )
                task_id = cursor.lastrowid
            log_stdout(f"任务 #{task_id} 已提交：{payload['source_image'].strip()} -> {payload['target_image'].strip()}，代理：{'启用' if payload.get('use_proxy') else '未启用'}")
            threading.Thread(target=run_sync, args=(task_id,), daemon=True).start()
            self.send_json({"id": task_id})
            return

        self.send_json({"error": "Not found"}, 404)

    def do_DELETE(self):
        path = urlparse(self.path).path
        if path.startswith("/api/tasks/"):
            item_id = int(path.rsplit("/", 1)[-1])
            with PROCESS_LOCK:
                proc = RUNNING_PROCESSES.pop(item_id, None)
            if proc and proc.poll() is None:
                proc.terminate()
                log_stdout(f"任务 #{item_id} 被删除，已终止运行中的 skopeo 进程")
            with db() as conn:
                conn.execute("DELETE FROM sync_tasks WHERE id = ?", (item_id,))
            self.send_json({"ok": True})
            return
        if path.startswith("/api/registries/"):
            item_id = path.rsplit("/", 1)[-1]
            with db() as conn:
                conn.execute("DELETE FROM target_registries WHERE id = ?", (item_id,))
            self.send_json({"ok": True})
            return
        self.send_json({"error": "Not found"}, 404)


INDEX_HTML = r'''
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DockerSync</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
  <main class="max-w-6xl mx-auto px-4 py-8">
    <header class="mb-8 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
      <div>
        <h1 class="text-3xl font-bold">DockerSync</h1>
        <p class="text-slate-400 mt-2">通过 skopeo 将国外镜像同步到你的私有镜像仓库；代理只作用于同步子进程。</p>
      </div>
      <button onclick="openSettings()" class="btn-secondary">设置</button>
    </header>

    <section class="bg-slate-900 rounded-2xl p-5 border border-slate-800 mb-6">
      <h2 class="text-xl font-semibold mb-4">新建同步任务</h2>
      <div class="grid gap-4">
        <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <label class="field-card">
            <span class="field-title">源镜像地址</span>
            <input id="sourceImage" class="input w-full" value="nginx:latest" placeholder="例如：nginx:latest" oninput="syncTargetImageDefault()" />
            <span class="flex items-center gap-2 text-xs text-slate-400"><input id="taskUseProxy" type="checkbox" /> 本任务使用代理</span>
          </label>
          <label class="field-card">
            <span class="field-title">目标仓库地址</span>
            <select id="targetRegistry" class="input w-full min-w-0" onchange="syncTargetImageDefault()"></select>
          </label>
          <label class="field-card md:col-span-2 xl:col-span-1">
            <span class="field-title">目标镜像地址</span>
            <input id="targetImage" class="input w-full" placeholder="默认按源镜像自动生成" oninput="targetImageTouched = true; syncTargetImageDefault()" />
          </label>
        </div>
        <button onclick="createTask()" class="btn w-full sm:w-auto sm:px-10 justify-self-start">开始同步</button>
      </div>
      <p id="targetPreview" class="text-sm text-slate-400 mt-2 break-all"></p>
      <p class="text-sm text-slate-500 mt-1">源镜像未写 registry 时会自动补全为 docker.io。目标镜像不填时，会从源镜像取最后一级名称和 tag，例如 nginx:1.30 → 仓库地址/nginx:1.30，emqx/emqx:5.10.0 → 仓库地址/emqx:5.10.0。</p>
    </section>

    <section class="bg-slate-900 rounded-2xl p-5 border border-slate-800">
      <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mb-4">
        <h2 class="text-xl font-semibold">任务列表</h2>
        <div class="flex flex-wrap items-center gap-3 text-sm">
          <label class="flex items-center gap-2 text-slate-300"><input id="selectAllTasks" type="checkbox" onchange="toggleAllTasks(this.checked)" /> 全选</label>
          <button onclick="cancelSelectedTasks()" class="text-amber-400">取消选中</button>
          <button onclick="deleteSelectedTasks()" class="text-red-400">删除选中</button>
          <button onclick="loadAll()" class="text-sky-400">刷新</button>
        </div>
      </div>
      <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between mb-3 text-sm text-slate-400">
        <div id="taskPageInfo">暂无任务</div>
        <div class="flex flex-wrap items-center gap-2">
          <span>每页</span>
          <select id="taskPageSize" class="input py-1.5 px-2 text-sm" onchange="changeTaskPageSize()">
            <option value="10" selected>10</option>
            <option value="20">20</option>
            <option value="50">50</option>
          </select>
          <button id="prevTaskPageBtn" onclick="prevTaskPage()" class="btn-secondary py-1.5 px-3 text-sm" type="button">上一页</button>
          <button id="nextTaskPageBtn" onclick="nextTaskPage()" class="btn-secondary py-1.5 px-3 text-sm" type="button">下一页</button>
        </div>
      </div>
      <div id="tasks" class="space-y-3"></div>
    </section>
  </main>

  <div id="settingsModal" class="modal hidden">
    <div class="modal-panel">
      <div class="flex items-center justify-between mb-5">
        <h2 class="text-xl font-semibold">设置</h2>
        <button onclick="closeSettings()" class="text-slate-400 hover:text-white text-2xl leading-none">×</button>
      </div>

      <div class="flex gap-2 mb-5 border-b border-slate-800">
        <button id="registryTab" onclick="switchSettingsTab('registry')" class="tab active">仓库设置</button>
        <button id="proxyTab" onclick="switchSettingsTab('proxy')" class="tab">代理设置</button>
      </div>

      <section id="registryPanel">
        <h3 class="font-semibold mb-3">目标仓库凭证</h3>
        <div class="grid gap-4">
          <input id="regId" type="hidden" />
          <label class="form-row">
            <span>默认使用</span>
            <input id="regDefault" type="checkbox" />
          </label>
          <label class="form-row required">
            <span>提供商</span>
            <select id="regProvider" class="input">
              <option value="">请选择提供商</option>
              <option value="Harbor">Harbor</option>
              <option value="Aliyun ACR">阿里云 ACR</option>
              <option value="Docker Hub">Docker Hub</option>
              <option value="Custom">自定义</option>
            </select>
          </label>
          <label class="form-row required">
            <span>名称</span>
            <input id="regName" class="input" placeholder="例如：阿里云杭州" />
          </label>
          <label class="form-row required">
            <span>地址</span>
            <input id="regUrl" class="input" placeholder="https://harbor.example.com 或 registry.cn-hangzhou.aliyuncs.com" />
          </label>
          <label class="form-row">
            <span>项目</span>
            <input id="regProject" class="input" placeholder="例如：magiclab 或 zhengyu1992" />
          </label>
          <label class="form-row required">
            <span>Docker 用户名</span>
            <input id="regUser" class="input" placeholder="Docker 用户名" />
          </label>
          <label class="form-row required">
            <span>Docker 密码</span>
            <input id="regPass" class="input" placeholder="密码 / Token" type="password" />
          </label>
          <div class="grid sm:grid-cols-2 gap-3">
            <button id="saveRegistryBtn" onclick="saveRegistry()" class="btn">保存仓库</button>
            <button onclick="newRegistry()" class="btn-secondary" type="button">新增仓库</button>
          </div>
        </div>
        <div id="registries" class="mt-5 space-y-2"></div>
      </section>

      <section id="proxyPanel" class="hidden">
        <h3 class="font-semibold mb-3">代理与 TLS</h3>
        <div class="grid gap-3">
          <p class="text-sm text-slate-400">这里只配置代理地址；是否使用代理在每个同步任务里勾选。</p>
          <input id="proxyUrl" class="input" placeholder="http://127.0.0.1:7890 或 socks5://10.204.12.243:20170" />
          <input id="extraNoProxy" class="input" placeholder="额外 NO_PROXY" value="localhost,127.0.0.1" />
          <label class="flex gap-2 items-center"><input id="srcTls" type="checkbox" checked /> 校验源仓库 TLS</label>
          <label class="flex gap-2 items-center"><input id="destTls" type="checkbox" checked /> 校验目标仓库 TLS</label>
          <div class="grid sm:grid-cols-2 gap-3">
            <button onclick="saveSettings()" class="btn">保存代理设置</button>
            <button onclick="checkProxy()" class="btn-secondary" type="button">检查连通性</button>
          </div>
          <div id="proxyCheckResult" class="text-sm text-slate-400"></div>
        </div>
      </section>
    </div>
  </div>

  <style>
    .input { background:#020617; border:1px solid #334155; border-radius:0.75rem; padding:0.75rem 1rem; color:#f8fafc; outline:none; min-width:0; }
    .input:focus { border-color:#38bdf8; }
    .field-card { display:grid; gap:.5rem; min-width:0; padding:.75rem; border:1px solid #1e293b; border-radius:1rem; background:rgba(15,23,42,.58); }
    .field-title { font-size:.875rem; color:#cbd5e1; }
    .btn { background:#0284c7; color:white; border-radius:0.75rem; padding:0.75rem 1rem; font-weight:600; }
    .btn:hover { background:#0369a1; }
    .btn-secondary { background:#1e293b; border:1px solid #334155; color:white; border-radius:0.75rem; padding:0.75rem 1rem; font-weight:600; }
    .btn-secondary:hover { background:#334155; }
    .modal { position:fixed; inset:0; z-index:50; background:rgba(2,6,23,.78); backdrop-filter:blur(6px); display:flex; align-items:flex-start; justify-content:center; padding:4rem 1rem; }
    .modal.hidden { display:none; }
    .modal-panel { width:100%; max-width:720px; max-height:calc(100vh - 8rem); overflow:auto; background:#0f172a; border:1px solid #1e293b; border-radius:1rem; padding:1.25rem; box-shadow:0 25px 60px rgba(0,0,0,.35); }
    .tab { padding:.75rem 1rem; color:#94a3b8; border-bottom:2px solid transparent; }
    .tab.active { color:#38bdf8; border-bottom-color:#38bdf8; }
    .form-row { display:grid; grid-template-columns:8rem 1fr; gap:1rem; align-items:center; color:#cbd5e1; }
    .form-row.required span::after { content:'•'; color:#ef4444; margin-left:.75rem; }
    @media (max-width:640px) { .form-row { grid-template-columns:1fr; gap:.5rem; } }
  </style>

  <script>
    async function api(path, options = {}) {
      const res = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...options });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || '请求失败');
      return data;
    }

    function openSettings() {
      settingsModal.classList.remove('hidden');
    }

    function closeSettings() {
      settingsModal.classList.add('hidden');
    }

    function switchSettingsTab(name) {
      const isRegistry = name === 'registry';
      registryPanel.classList.toggle('hidden', !isRegistry);
      proxyPanel.classList.toggle('hidden', isRegistry);
      registryTab.classList.toggle('active', isRegistry);
      proxyTab.classList.toggle('active', !isRegistry);
    }

    function registryPath(r) {
      return [r.registry_url, r.project].filter(Boolean).join('/');
    }

    function targetPath(t) {
      const image = (t.target_image || '').replace(/^\/+/, '');
      const project = (t.project || '').replace(/^\/+|\/+$/g, '');
      return [t.registry_url, project && !image.startsWith(project + '/') ? project : '', image].filter(Boolean).join('/');
    }

    let targetImageTouched = false;

    function getSelectedRegistry() {
      return registriesCache.find(r => String(r.id) === String(targetRegistry.value));
    }

    function defaultTargetImageFromSource(source) {
      let image = String(source || '').trim().replace(/^docker:\/\//, '').split('@')[0];
      if (!image) return '';
      const parts = image.split('/').filter(Boolean);
      let name = parts.pop() || '';
      if (!name.includes(':')) name += ':latest';
      return name;
    }

    function fullTargetPreview(registry, image) {
      if (!registry || !image) return '';
      const cleanImage = image.replace(/^\/+/, '');
      const project = (registry.project || '').replace(/^\/+|\/+$/g, '');
      return [registry.registry_url, project && !cleanImage.startsWith(project + '/') ? project : '', cleanImage].filter(Boolean).join('/');
    }

    function syncTargetImageDefault() {
      const defaultImage = defaultTargetImageFromSource(sourceImage.value);
      if ((!targetImageTouched || !targetImage.value.trim()) && defaultImage) {
        targetImage.value = defaultImage;
      }
      const image = targetImage.value.trim() || defaultImage;
      const preview = fullTargetPreview(getSelectedRegistry(), image);
      targetPreview.textContent = preview ? `同步目标：${preview}` : '';
    }

    function fillRegistryForm(r) {
      regId.value = r?.id || '';
      regDefault.checked = r ? Boolean(r.is_default) : false;
      regProvider.value = r?.provider || '';
      regName.value = r?.name || '';
      regUrl.value = r?.registry_url || '';
      regProject.value = r?.project || '';
      regUser.value = r?.username || '';
      regPass.value = r?.password || '';
      saveRegistryBtn.textContent = r ? '更新仓库' : '保存仓库';
    }

    function newRegistry() {
      fillRegistryForm(null);
    }

    let registriesCache = [];

    function fillRegistryById(id) {
      fillRegistryForm(registriesCache.find(r => String(r.id) === String(id)) || null);
    }

    async function loadRegistries(preferredId = '') {
      const list = await api('/api/registries');
      registriesCache = list;
      registries.innerHTML = list.map(r => `<div class="bg-slate-950 border border-slate-800 rounded-xl p-3 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div>
          <button class="font-medium flex items-center gap-2 text-left" onclick="fillRegistryById(${r.id})">${r.name} ${r.is_default ? '<span class="px-2 py-0.5 rounded bg-sky-600 text-xs">默认</span>' : ''}</button>
          <div class="text-sm text-slate-400">${r.provider || 'Custom'} · ${registryPath(r)} · ${r.username}</div>
        </div>
        <div class="flex gap-3 text-sm">
          <button class="text-sky-400" onclick="fillRegistryById(${r.id})">编辑</button>
          ${r.is_default ? '' : `<button class="text-sky-400" onclick="setDefaultRegistry(${r.id})">设为默认</button>`}
          <button class="text-red-400" onclick="deleteRegistry(${r.id})">删除</button>
        </div>
      </div>`).join('') || '<div class="text-slate-500 text-sm">还没有保存凭证。</div>';
      targetRegistry.innerHTML = list.map(r => `<option value="${r.id}">${r.is_default ? '默认 - ' : ''}${r.name} - ${registryPath(r)}</option>`).join('') || '<option value="">请先在设置中添加仓库</option>';
      const selectedRegistry = list.find(r => String(r.id) === String(preferredId)) || list.find(r => r.is_default) || list[0];
      if (selectedRegistry) {
        targetRegistry.value = selectedRegistry.id;
        fillRegistryForm(selectedRegistry);
      } else {
        fillRegistryForm(null);
      }
      syncTargetImageDefault();
    }

    async function saveRegistry() {
      const payload = {
        name: regName.value,
        provider: regProvider.value,
        registry_url: regUrl.value,
        project: regProject.value,
        username: regUser.value,
        password: regPass.value,
        is_default: regDefault.checked
      };
      const currentId = regId.value;
      const path = currentId ? `/api/registries/${currentId}/update` : '/api/registries';
      const result = await api(path, { method: 'POST', body: JSON.stringify(payload) });
      await loadRegistries(currentId || result.id);
    }

    async function setDefaultRegistry(id) {
      await api('/api/registries/' + id + '/default', { method: 'POST' });
      await loadRegistries();
    }

    async function deleteRegistry(id) {
      if (!confirm('确定删除这个凭证？')) return;
      await api('/api/registries/' + id, { method: 'DELETE' });
      await loadRegistries();
    }

    async function loadSettings() {
      const s = await api('/api/settings');
      proxyUrl.value = s.proxy_url;
      extraNoProxy.value = s.extra_no_proxy;
      srcTls.checked = s.src_tls_verify;
      destTls.checked = s.dest_tls_verify;
    }

    async function saveSettings() {
      const s = await api('/api/settings', { method: 'POST', body: JSON.stringify({
        proxy_url: proxyUrl.value,
        extra_no_proxy: extraNoProxy.value,
        src_tls_verify: srcTls.checked,
        dest_tls_verify: destTls.checked
      })});
      proxyUrl.value = s.proxy_url;
      alert('设置已保存');
    }

    async function checkProxy() {
      proxyCheckResult.textContent = '正在检查...';
      proxyCheckResult.className = 'text-sm text-slate-400';
      try {
        const result = await api('/api/settings/proxy-check', { method: 'POST', body: JSON.stringify({ proxy_url: proxyUrl.value }) });
        proxyUrl.value = result.proxy_url;
        proxyCheckResult.textContent = `连通正常：${result.proxy_url}`;
        proxyCheckResult.className = 'text-sm text-emerald-400';
      } catch (err) {
        proxyCheckResult.textContent = `连通失败：${err.message}`;
        proxyCheckResult.className = 'text-sm text-red-400';
      }
    }

    async function createTask() {
      if (!targetRegistry.value) return alert('请先点击右上角“设置”，添加目标仓库凭证');
      const defaultImage = defaultTargetImageFromSource(sourceImage.value);
      const image = targetImage.value.trim() || defaultImage;
      targetImage.value = image;
      syncTargetImageDefault();
      await api('/api/tasks', { method: 'POST', body: JSON.stringify({
        source_image: sourceImage.value,
        target_registry_id: targetRegistry.value,
        target_image: image,
        use_proxy: taskUseProxy.checked
      })});
      await loadTasks();
    }

    function badge(status) {
      const colors = { PENDING:'bg-slate-700', RUNNING:'bg-amber-600', CANCELLING:'bg-orange-600', CANCELLED:'bg-slate-600', SUCCESS:'bg-emerald-600', FAILED:'bg-red-600' };
      return `<span class="px-2 py-1 rounded-lg text-xs ${colors[status] || 'bg-slate-700'}">${status}</span>`;
    }

    function escapeHtml(text) {
      return String(text || '').replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
    }

    function escapeAttr(text) {
      return String(text || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    function parseLayerProgress(log, taskStatus) {
      const lines = String(log || '').split('\n');
      const layers = [];
      const indexByDigest = new Map();
      let phase = '准备中';
      for (const line of lines) {
        const blob = line.match(/Copying blob\s+(sha256:[a-f0-9]+)/i);
        const config = line.match(/Copying config\s+(sha256:[a-f0-9]+)/i);
        const manifest = /Writing manifest|Copying manifest/i.test(line);
        if (blob) {
          const digest = blob[1];
          if (!indexByDigest.has(digest)) {
            indexByDigest.set(digest, layers.length);
            layers.push({ digest, status: '复制中' });
          }
          for (let i = 0; i < layers.length - 1; i++) {
            if (layers[i].status === '复制中') layers[i].status = '已完成';
          }
          phase = '复制镜像层';
        } else if (config) {
          layers.forEach(layer => layer.status = '已完成');
          phase = '复制镜像配置';
        } else if (manifest) {
          layers.forEach(layer => layer.status = '已完成');
          phase = '写入镜像清单';
        }
      }
      if (taskStatus === 'SUCCESS') {
        layers.forEach(layer => layer.status = '已完成');
        phase = '同步完成';
      }
      const done = layers.filter(layer => layer.status === '已完成').length;
      return { layers, done, total: layers.length, phase };
    }

    function renderLayerProgress(t) {
      const result = parseLayerProgress(t.log_output, t.status);
      if (!result.total) return '';
      const percent = Math.round((result.done / result.total) * 100);
      const rows = result.layers.map((layer, idx) => {
        const active = layer.status === '复制中';
        const color = layer.status === '已完成' ? 'text-emerald-400' : active ? 'text-sky-400' : 'text-slate-400';
        const bar = layer.status === '已完成' ? 100 : active ? 55 : 0;
        return `<div class="grid sm:grid-cols-[3rem_1fr_5rem] gap-2 items-center text-xs">
          <div class="text-slate-500">#${idx + 1}</div>
          <div class="min-w-0">
            <div class="truncate font-mono">${escapeHtml(layer.digest)}</div>
            <div class="mt-1 h-1.5 bg-slate-800 rounded-full overflow-hidden"><div class="h-full ${active ? 'bg-sky-500' : 'bg-emerald-500'}" style="width:${bar}%"></div></div>
          </div>
          <div class="${color}">${layer.status}</div>
        </div>`;
      }).join('');
      return `<div class="mt-3 p-3 bg-slate-900/80 border border-slate-800 rounded-xl">
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-1 mb-3">
          <div class="text-sm font-medium">镜像层进度：${result.done}/${result.total} 层 · ${percent}%</div>
          <div class="text-xs text-slate-400">${result.phase}</div>
        </div>
        <div class="space-y-2">${rows}</div>
      </div>`;
    }

    function selectedTaskIds() {
      return [...document.querySelectorAll('.task-check:checked')].map(item => Number(item.value));
    }

    function toggleAllTasks(checked) {
      document.querySelectorAll('.task-check').forEach(item => item.checked = checked);
    }

    async function cancelSelectedTasks() {
      const ids = selectedTaskIds();
      if (!ids.length) return alert('请先勾选任务');
      await api('/api/tasks/cancel', { method: 'POST', body: JSON.stringify({ ids }) });
      await loadTasks();
    }

    async function deleteSelectedTasks() {
      const ids = selectedTaskIds();
      if (!ids.length) return alert('请先勾选任务');
      if (!confirm(`确定删除 ${ids.length} 个任务？运行中的任务会先取消。`)) return;
      for (const id of ids) await api('/api/tasks/' + id, { method: 'DELETE' });
      await loadTasks();
    }

    let taskCurrentPage = 1;
    let taskTotalPages = 1;
    let latestTaskCount = 0;
    const openedTaskIds = new Set();
    const closedTaskIds = new Set();
    const autoOpenStatuses = ['RUNNING', 'FAILED', 'CANCELLING'];

    function taskPageLimit() {
      return Number(taskPageSize?.value || 10);
    }

    function changeTaskPageSize() {
      taskCurrentPage = 1;
      loadTasks();
    }

    function prevTaskPage() {
      if (taskCurrentPage <= 1) return;
      taskCurrentPage -= 1;
      loadTasks();
    }

    function nextTaskPage() {
      if (taskCurrentPage >= taskTotalPages) return;
      taskCurrentPage += 1;
      loadTasks();
    }

    async function copyText(text, button) {
      try {
        await navigator.clipboard.writeText(text);
      } catch {
        const textarea = document.createElement('textarea');
        textarea.value = text;
        textarea.style.position = 'fixed';
        textarea.style.left = '-9999px';
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        textarea.remove();
      }
      const oldText = button.textContent;
      button.textContent = '已复制';
      setTimeout(() => button.textContent = oldText, 1200);
    }

    function copyFromButton(button) {
      copyText(button.dataset.copy || '', button);
    }

    function rememberTaskDetailsState() {
      document.querySelectorAll('#tasks details[data-task-id]').forEach(item => {
        const id = Number(item.dataset.taskId);
        if (item.open) {
          openedTaskIds.add(id);
          closedTaskIds.delete(id);
        } else {
          closedTaskIds.add(id);
          openedTaskIds.delete(id);
        }
      });
    }

    function setTaskDetailsState(id, open) {
      if (open) {
        openedTaskIds.add(id);
        closedTaskIds.delete(id);
      } else {
        closedTaskIds.add(id);
        openedTaskIds.delete(id);
      }
    }

    async function loadTasks() {
      rememberTaskDetailsState();
      const list = await api('/api/tasks');
      latestTaskCount = list.length;
      const limit = taskPageLimit();
      taskTotalPages = Math.max(1, Math.ceil(latestTaskCount / limit));
      taskCurrentPage = Math.min(Math.max(1, taskCurrentPage), taskTotalPages);
      const start = (taskCurrentPage - 1) * limit;
      const pageItems = list.slice(start, start + limit);
      const currentIds = new Set(list.map(t => Number(t.id)));
      [...openedTaskIds].forEach(id => { if (!currentIds.has(id)) openedTaskIds.delete(id); });
      [...closedTaskIds].forEach(id => { if (!currentIds.has(id)) closedTaskIds.delete(id); });
      if (selectAllTasks) selectAllTasks.checked = false;
      taskPageInfo.textContent = latestTaskCount ? `第 ${taskCurrentPage}/${taskTotalPages} 页 · 共 ${latestTaskCount} 个任务 · 当前显示 ${start + 1}-${start + pageItems.length}` : '暂无任务';
      prevTaskPageBtn.disabled = taskCurrentPage <= 1;
      nextTaskPageBtn.disabled = taskCurrentPage >= taskTotalPages;
      prevTaskPageBtn.classList.toggle('opacity-50', prevTaskPageBtn.disabled);
      nextTaskPageBtn.classList.toggle('opacity-50', nextTaskPageBtn.disabled);
      tasks.innerHTML = pageItems.map(t => {
        const taskId = Number(t.id);
        const progress = Number(t.progress || 0);
        const sourceText = t.source_image || '';
        const targetText = targetPath(t);
        const escapedLog = escapeHtml(t.log_output);
        const layerProgress = renderLayerProgress(t);
        const shouldOpen = openedTaskIds.has(taskId) || (autoOpenStatuses.includes(t.status) && !closedTaskIds.has(taskId));
        return `<details data-task-id="${taskId}" ontoggle="setTaskDetailsState(${taskId}, this.open)" class="bg-slate-950 border border-slate-800 rounded-xl p-3" ${shouldOpen ? 'open' : ''}>
          <summary class="cursor-pointer list-none">
            <div class="flex items-start justify-between gap-4">
              <div class="flex gap-3 min-w-0">
                <input class="task-check mt-1" type="checkbox" value="${t.id}" onclick="event.stopPropagation()" />
                <div class="min-w-0">
                  <div><span class="font-medium">${escapeHtml(sourceText)}</span><span class="text-slate-500"> → ${escapeHtml(targetText)}</span></div>
                  <div class="text-xs text-slate-400 mt-1">当前步骤：${t.current_step || '等待开始'} · 代理：${Number(t.use_proxy || 0) ? '本任务启用' : '未启用'}</div>
                </div>
              </div>
              <div class="flex items-center gap-3 shrink-0">${badge(t.status)}<button class="text-amber-400 text-sm" onclick="event.preventDefault(); event.stopPropagation(); cancelOneTask(${t.id})">取消</button><button class="text-red-400 text-sm" onclick="event.preventDefault(); event.stopPropagation(); deleteOneTask(${t.id})">删除</button></div>
            </div>
            <div class="mt-3 h-2 bg-slate-800 rounded-full overflow-hidden"><div class="h-full bg-sky-500" style="width:${Math.max(0, Math.min(100, progress))}%"></div></div>
            <div class="text-xs text-slate-500 mt-1">进度：${progress}%</div>
          </summary>
          <div class="mt-3 grid gap-2 text-sm">
            <div class="flex flex-col gap-2 rounded-xl border border-slate-800 bg-slate-900/70 p-3 sm:flex-row sm:items-center sm:justify-between">
              <div class="min-w-0">
                <div class="text-xs text-slate-500">源镜像地址</div>
                <div class="font-mono text-slate-200 break-all">${escapeHtml(sourceText)}</div>
              </div>
              <button class="btn-secondary py-1.5 px-3 text-sm shrink-0" type="button" data-copy="${escapeAttr(sourceText)}" onclick="copyFromButton(this)">复制</button>
            </div>
            <div class="flex flex-col gap-2 rounded-xl border border-slate-800 bg-slate-900/70 p-3 sm:flex-row sm:items-center sm:justify-between">
              <div class="min-w-0">
                <div class="text-xs text-slate-500">目标镜像地址</div>
                <div class="font-mono text-slate-200 break-all">${escapeHtml(targetText)}</div>
              </div>
              <button class="btn-secondary py-1.5 px-3 text-sm shrink-0" type="button" data-copy="${escapeAttr(targetText)}" onclick="copyFromButton(this)">复制</button>
            </div>
          </div>
          ${layerProgress}
          <pre class="mt-3 p-3 bg-black rounded-xl overflow-auto text-sm whitespace-pre-wrap max-h-96">${escapedLog}</pre>
        </details>`;
      }).join('') || '<div class="text-slate-500 text-sm">暂无任务。</div>';
    }

    async function cancelOneTask(id) {
      await api('/api/tasks/cancel', { method: 'POST', body: JSON.stringify({ ids: [id] }) });
      await loadTasks();
    }

    async function deleteOneTask(id) {
      if (!confirm('确定删除这个任务？运行中的任务会先取消。')) return;
      await api('/api/tasks/' + id, { method: 'DELETE' });
      await loadTasks();
    }

    async function loadAll() {
      await Promise.all([loadRegistries(), loadSettings(), loadTasks()]);
    }

    settingsModal.addEventListener('click', event => {
      if (event.target === settingsModal) closeSettings();
    });

    loadAll();
    setInterval(loadTasks, 2500);
  </script>
</body>
</html>
'''


if __name__ == "__main__":
    init_db()
    pod_ip = socket.gethostbyname(socket.gethostname())
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    log_stdout("欢迎使用 DockerSync 服务")
    log_stdout(f"数据目录：{DATA_DIR}")
    log_stdout(f"数据库文件：{DB_PATH}")
    log_stdout(f"请打开 http://{pod_ip}:{PORT} 访问控制台；本机调试可访问 http://127.0.0.1:{PORT}")
    log_stdout(f"服务监听：{HOST}:{PORT}")
    server.serve_forever()
