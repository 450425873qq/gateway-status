# -*- coding: utf-8 -*-
"""云探针：GitHub Actions 每 10 分钟探测一次网关，结果（脱敏）写入 data/。

敏感信息全部来自环境变量（仓库 Secrets），任何输出都不含真实域名与密钥。
"""
import io, os, sys, json, time, socket, glob, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE_URL = os.environ.get("GATEWAY_BASE_URL", "").rstrip("/")
API_KEY = os.environ.get("GATEWAY_API_KEY", "")
SERIES = [s.strip().lower() for s in
          os.environ.get("SERIES", "deepseek,kimi,glm").split(",") if s.strip()]
RETAIN_DAYS = 30
TIMEOUT = 20
DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
TZ = timezone(timedelta(hours=8))  # 北京时间
HOST = urllib.parse.urlparse(BASE_URL).netloc if BASE_URL else ""

NON_CHAT = ("embed", "rerank", "tts", "asr", "whisper", "audio", "moderation",
            "dall-e", "image", "vision-ocr", "video", "flux", "stable-diffusion",
            "midjourney", "sora", "voice", "tts-1")


def scrub(s):
    """脱敏：任何输出文本里的真实域名替换成 gateway"""
    if not s:
        return ""
    return s.replace(HOST, "gateway") if HOST else s


def classify(status, body, err):
    low = (body or "").lower()
    if err:
        if isinstance(err, socket.timeout) or "timed out" in str(err).lower():
            return "timeout"
        return "network"
    if status == 200:
        return ""
    if status == 401:
        return "auth"
    if status == 429:
        return "rate_limited"
    if status == 503:
        return "service_unavailable"
    if any(k in low for k in ("insufficient balance", "quota", "额度", "欠费", "余额不足")):
        return "billing"
    if "cpu overload" in low or "负载" in low or "过载" in low:
        return "cpu_overloaded"
    if ("无可用渠道" in low or "no available channel" in low
            or "当前分组" in low or "无可用渠道" in body):
        return "no_channel"
    if status >= 500:
        return "http_500"
    return "http_%s" % status if status else "network"


def http_call(url, payload=None):
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Authorization": "Bearer " + API_KEY,
                 "Content-Type": "application/json",
                 "User-Agent": "gateway-status-probe/1.0"},
        method="POST" if data else "GET")
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            body = r.read(8192).decode("utf-8", "replace")
            return r.status, body, int((time.time() - t0) * 1000), None
    except urllib.error.HTTPError as e:
        try:
            body = e.read(8192).decode("utf-8", "replace")
        except Exception:
            body = ""
        return e.code, body, int((time.time() - t0) * 1000), None
    except Exception as e:
        return 0, "", int((time.time() - t0) * 1000), e


def now_bj():
    return datetime.now(TZ)


def day_file(ts=None):
    d = datetime.fromtimestamp(ts, TZ) if ts else now_bj()
    return d.strftime("%Y-%m-%d") + ".json"


def load_day(fname):
    p = os.path.join(DATA, fname)
    if os.path.exists(p):
        try:
            with io.open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def save_day(fname, obj):
    p = os.path.join(DATA, fname)
    tmp = p + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, p)


def probe_gateway():
    """网关级：/v1/models 可用性 + 发现模型清单"""
    st, body, ms, err = http_call(BASE_URL + "/models")
    et = classify(st, body, err)
    ok = (st == 200 and not et)
    ids = []
    if ok:
        try:
            ids = sorted(m.get("id", "") for m in json.loads(body).get("data", [])
                         if m.get("id"))
        except Exception:
            pass
    row = [int(time.time()), "gateway", "/v1/models", 1 if ok else 0, ms,
           st, et, ("OK | %d models" % len(ids)) if ok else scrub((body or str(err or ""))[:200])]
    return row, ids


def discover_models(ids):
    """series 白名单 + 排除非对话模型，动态跟随网关更新"""
    out = []
    for m in ids:
        low = m.lower()
        if any(p in low for p in NON_CHAT):
            continue
        if SERIES and not any(s in low for s in SERIES):
            continue
        out.append(m)
    return out


def probe_model(model):
    st, body, ms, err = http_call(BASE_URL + "/chat/completions", {
        "model": model,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 1,
    })
    et = classify(st, body, err)
    ok = (st == 200 and not et)
    msg = "OK" if ok else scrub((body or str(err or ""))[:200])
    return [int(time.time()), "model", model, 1 if ok else 0, ms, st, et, msg]


def main():
    if not BASE_URL or not API_KEY:
        print("缺少 GATEWAY_BASE_URL / GATEWAY_API_KEY")
        return 1
    os.makedirs(DATA, exist_ok=True)

    g_row, ids = probe_gateway()
    targets = discover_models(ids)
    print("网关: %s | 模型总数 %d | 探测目标 %d 个（series=%s）"
          % (g_row[3], len(ids), len(targets), ",".join(SERIES)))

    rows = [g_row]
    with ThreadPoolExecutor(max_workers=10) as ex:
        rows.extend(ex.map(probe_model, targets))

    nok = sum(1 for r in rows if r[3] == 1)
    print("本轮: %d/%d ok" % (nok, len(rows)))

    # 合并进当天文件（同一天多轮追加）
    fname = day_file()
    obj = load_day(fname) or {"date": fname[:-5], "records": []}
    obj["records"].extend(rows)
    if len(obj["records"]) > 60000:
        obj["records"] = obj["records"][-60000:]
    save_day(fname, obj)

    # 清理过期文件 + 重建 index.json
    cutoff = (now_bj() - timedelta(days=RETAIN_DAYS)).strftime("%Y-%m-%d")
    files = []
    for p in glob.glob(os.path.join(DATA, "????-??-??.json")):
        b = os.path.basename(p)
        if b[:10] < cutoff:
            os.remove(p)
            print("清理过期:", b)
        else:
            files.append(b)
    files.sort(reverse=True)
    idx = {"files": files, "updated": int(time.time()), "retain_days": RETAIN_DAYS}
    save_day("index.json", idx)
    print("data 文件数:", len(files))
    return 0


if __name__ == "__main__":
    sys.exit(main())
