# ASR Gateway（FunASR → 流式转写接口）

把 `fun-asr-nano` 容器自带的**原生 FunASR WebSocket 协议**（握手 JSON + 二进制 PCM）
封装成对外易用的流式转写接口，调用方不用再实现 FunASR 那套协议。

```
调用方（自研客户端 / 其他项目 / 调试脚本）
        │  ws://<IP>:10097/v1/realtime 或 /v1/asr
        ▼
   asr-gateway  (FastAPI + uvicorn, 容器名 asr-gateway, 端口 10097)
        │  ws://fun-asr-nano:10096      ← FunASR 原生协议
        ▼
   fun-asr-nano 容器 (wangshengjj/fun-asr-nano-2512, GPU)
```

## 一、目录内容

| 文件 | 作用 |
| --- | --- |
| `app.py` | 网关主程序 |
| `ws_test_client.py` | 命令行测试客户端（wav → 流式识别，自动转 16k 单声道） |
| `Dockerfile` / `requirements.txt` | 镜像定义（python:3.10-slim + fastapi/uvicorn/websockets） |
| `../docker-compose.yml` | 单文件编排，已包含 `fun-asr-nano` 与 `asr-gateway` |

## 二、启动

```bash
cd /home/eason/work/fun-asr-nano
docker compose up -d                # 单文件 compose，不需要 -f 指定
docker compose ps
docker logs -f asr-gateway

# 只重建网关
docker compose up -d --build asr-gateway

# 健康检查
curl -s http://127.0.0.1:10097/health | python3 -m json.tool
curl -s "http://127.0.0.1:10097/health?deep=1" | python3 -m json.tool   # 含上游握手

# 真实语音回归
pip3 install websockets
python3 asr-gateway/ws_test_client.py --wav 你的音频.wav --realtime
```

## 三、接口

### 连接

| 项 | 值 |
| --- | --- |
| `/v1/realtime`、`/v1/audio/stream` | **OpenAI Realtime 风格事件**（默认） |
| `/v1/asr` | **精简事件**：`asr.ready` / `asr.partial` / `asr.final` |
| 音频格式 | **PCM 16000Hz / 16bit / 单声道（小端）**，建议每帧 60~100ms（1920~3200 字节） |
| 查询参数 | `wav_name=xxx`、`style=simple`；其余键（`hotwords`、`itn`、`svs_lang`…）会透传为 session 覆盖项 |
| 健康检查 | `GET http://<ip>:10097/health` |

### 客户端 → 网关（两种风格完全一致）

| 消息 | 说明 |
| --- | --- |
| **二进制帧** | 直接就是 PCM 16k/s16le/mono（最省事） |
| `{"type":"input_audio_buffer.append","audio":"<base64>"}` | 音频放 JSON 里 |
| `{"type":"input_audio_buffer.commit","finalize_timeout_ms":2500}` | 结束**当前这一句**。`finalize_timeout_ms` 可选，`0` = 一直等上游 |
| `{"type":"session.update","session":{...}}` | **仅在发音频前**有效，可改 `hotwords`/`itn`/`mode`/`chunk_size`/`wav_name` 等 |
| `{"type":"session.close"}` 或纯文本 `EOS` | 结束整个会话 |

### 网关 → 客户端

Realtime 风格：

| 事件 | 主要字段 |
| --- | --- |
| `session.created` | `session.{id, model, upstream, mode, audio_fs, itn, hotwords, chunk_size, finalize_timeout_ms}` |
| `conversation.item.input_audio_transcription.delta` | `delta` 上游原文；`text` / `utterance_text` **本句聚合文本**；`is_final:false`；`item_index` |
| `conversation.item.input_audio_transcription.completed` | `transcript` / `text` **整句文本**；`is_final:true`；`item_index`；`resolved_from`；`synthetic`；`segments[]` |
| `session.updated` / `error` | — |

精简风格（`/v1/asr`）：`asr.ready`、`asr.partial`（`text`/`delta`）、`asr.final`（`text`/`transcript`），字段与上面一一对应。

**两条必须知道的语义**

1. `delta.delta` 是上游滚动窗口原文（可能重复），**要显示请用 `delta.text`（本句聚合文本）**。
2. `completed` 是"这一句说完了"的唯一信号，**保证带得动文本**：
   - 上游句尾包有文本 → `resolved_from: "offline"`
   - 上游句尾包为空（本镜像实测恒为空且来得慢）→ 用聚合的 online 文本，`resolved_from: "online"`
   - `synthetic: true` = 网关在 `finalize_timeout_ms` 到点后自己合成的（上游还没给包）
   - 真静音 → 空文本 + `resolved_from: "none"`

> 实测 `fun-asr-nano-2512`：真实文本**全在 `2pass-online`** 里；句尾 `2pass-offline` 包
> **text 恒为空**，且 8 秒音频约 +2s、15 秒音频约 +28s 才到。所以别直接吃上游的 offline 文本。

### 调用示例

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://10.0.22.37:10097/v1/realtime?wav_name=demo") as ws:
        print(json.loads(await ws.recv()))               # session.created
        with open("pcm16k_mono.raw", "rb") as fp:
            while chunk := fp.read(3200):                # 100ms
                await ws.send(chunk)
        await ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
        async for raw in ws:
            ev = json.loads(raw)
            t = ev.get("type", "")
            if t.endswith("transcription.delta"):
                print("实时:", ev["text"])               # 本句聚合文本
            elif t.endswith("transcription.completed"):
                print("整句:", ev["transcript"], f'(来源={ev["resolved_from"]})')
                break

asyncio.run(main())
```

```javascript
const ws = new WebSocket("ws://10.0.22.37:10097/v1/realtime");
ws.binaryType = "arraybuffer";
ws.onmessage = (e) => {
  const ev = JSON.parse(e.data);
  if (ev.type?.endsWith("delta"))     show(ev.text);
  if (ev.type?.endsWith("completed")) finish(ev.transcript);
};
// 结束一句： ws.send(JSON.stringify({type: "input_audio_buffer.commit"}));
```

## 四、配置项（`docker-compose.yml` 里 `asr-gateway.environment`）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `ASR_WS_URL` | `ws://fun-asr-nano:10096` | 上游地址（同 compose 网络内用服务名） |
| `ASR_MODE` | `2pass` | `2pass` / `online` |
| `ASR_CHUNK_SIZE` | `[5,10,5]` | `[编码器历史, 当前块, 编码器未来]` |
| `ASR_CHUNK_INTERVAL` | `10` | 发送间隔 ms |
| `ASR_ENCODER_CHUNK_LOOK_BACK` | `4` | 编码器回溯步数 |
| `ASR_DECODER_CHUNK_LOOK_BACK` | `1` | 解码器回溯步数 |
| `ASR_AUDIO_FS` | `16000` | FunASR 要求必须是 16000 |
| `ASR_ITN` | `true` | 逆文本标准化 |
| `ASR_HOTWORDS` | 空 | `'{"机器人":20,"导览":15}'` 或 `"机器人:20,导览:15"` |
| `ASR_FINALIZE_TIMEOUT_MS` | `2500` | commit 后兜底合成句尾结果的等待毫秒数；`0` = 关闭兜底 |
| `ASR_EVENT_STYLE` | `realtime` | `realtime` / `simple`（决定 `/v1/realtime` 用哪套事件名） |
| `ASR_PCM_STATS_EVERY` | `100` | 每多少帧打印一次音频电平 |
| `ASR_EXTRA_CONFIG` | 空 | JSON 对象，透传上游握手的附加字段（换镜像时用） |
| `ASR_CONNECT_TIMEOUT` | `10` | 连上游超时秒 |
| `ASR_RECV_TIMEOUT` | `30` | 上游静默轮询间隔（不会因此断连） |
| `LOG_LEVEL` | `INFO` | 日志级别 |

换 FunASR 镜像时（例如 SenseVoiceSmall-onnx 要 `svs_lang`/`svs_itn`/`wav_format`）：

```yaml
- 'ASR_EXTRA_CONFIG={"svs_lang":"auto","svs_itn":true,"wav_format":"pcm"}'
```

## 五、让局域网其他机器访问（WSL2 是 NAT 网络）

Windows 本机用 `127.0.0.1:10097` 即可；**局域网其他机器访问不到 WSL 内部 IP**，二选一：

```powershell
# A. Windows 10：管理员 PowerShell 端口转发（WSL IP 在 WSL 里用 hostname -I 取）
netsh interface portproxy add v4tov4 listenaddress=0.0.0.0 listenport=10097 connectaddress=<WSL_IP> connectport=10097
netsh advfirewall firewall add rule name="FunASR-10097" dir=in action=allow protocol=TCP localport=10097
netsh interface portproxy show all
```

```ini
; B. Windows 11 22H2+ / WSL 2.0+：C:\Users\<你>\.wslconfig
[wsl2]
networkingMode=mirrored
```

WSL 重启后 IP 会变，portproxy 需重设。

## 六、排障：先看日志

```bash
docker logs -f asr-gateway
```

| 日志 | 含义 |
| --- | --- |
| `pcm frame=... dur=... rms=0 peak=0` | **送进来的是静音**——先查采集端的采样率/声道/音量 |
| `pcm frame=... rms=几百~几千` | 正常语音（16bit 满幅 32767） |
| `upstream <- mode=2pass-online text="..."` | 上游吐字了，说明模型在工作 |
| `upstream <- ... is_final=True text=""` | 正常：本镜像句尾包为空，文本在 online 里 |
| `event -> ...text="..."` | 已下发给调用方；**有这行却说没收到 → 调用方事件名没对上** |
| 一条 `upstream <-` 都没有 | 上游没吐字：音频格式/音量问题，或 GPU 首次加载慢 |

| 现象 | 处理 |
| --- | --- |
| `/health` 返回 503 | 上游不可达：`docker logs fun-asr-nano`，确认 10096 在听、GPU 正常 |
| 连上就 `upstream_unreachable` | 网关与 ASR 不在同一 compose 网络，或 `ASR_WS_URL` 写错 |
| 收到事件但取不到文本 | 认准 `completed.transcript` 或 `delta.text`；`delta.delta` 是上游原文 |
| `completed` 文本为空 | `resolved_from: "none"` = 该段真没识别到语音，先看 rms |
| `invalid_audio` | 帧长不是 2 字节整数倍（不是 16bit PCM） |
| `session.update` 被忽略 | 必须在发音频之前调用 |
| 第一次识别很慢 | GPU 冷启动加载模型，之后正常 |
| 局域网连不上 | 见第五节 |

## 七、与 robot_edge / interact 的关系（当前状态）

- `robot_edge` 的 `text_llm` 链路里 ASR 在 Android 侧完成，**不经过本网关**。
- `full_audio` 场景的 ASR provider 由 interact 决定（`robot_edge` 只把
  `INTERACT_ASR_PROVIDER` / `INTERACT_ASR_MODEL` 透传到 `/ws/interaction` 的 `session.create.v1`）。
  目前 interact 用阿里云（`aliyun` / `qwen3-asr-flash-realtime`），**暂未切换**。
- 将来要切本地：在 interact 里加一个实现 `src/domain/ports/asr.py::IAsrProvider` 的 provider
  （`connect` / `close` / `transcribe`），注册进 `src/infrastructure/asr_providers/factory.py`，
  目标指向 `ws://<网关IP>:10097/v1/realtime`，事件名按本文第三节对接。
