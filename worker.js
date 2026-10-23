
const GPT_OSS_URL = "https://api.gpt-oss.com/chatkit";
const MODELS = ["gpt-oss-120b", "gpt-oss-20b"];

const BASE_HEADERS = {
  accept: "text/event-stream",
  "content-type": "application/json",
  origin: "https://gpt-oss.com",
  referer: "https://gpt-oss.com/",
  "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/132.0.0.0 Safari/537.36",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "Content-Type, X-API-Key, X-Reasoning-Effort",
    },
  });
}

function extractText(messages) {
  if (!Array.isArray(messages)) return "";
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i]?.role !== "user") continue;
    const c = messages[i].content;
    if (typeof c === "string") return c;
    if (Array.isArray(c)) {
      return c.map(p => typeof p === "string" ? p : p?.text || "").join("");
    }
  }
  return "";
}

function parseEvent(evt, state) {
  if (!evt || typeof evt !== "object") return;

  if (evt.threadId) state.threadId = evt.threadId;
  if (evt.thread?.id) state.threadId = evt.thread.id;

  const type = evt.type || evt.event || "";
  const update = evt.update || {};
  const item = evt.item || update.item || {};
  const delta = evt.delta ?? update.delta;

  if (typeof delta === "string" && (
    type.includes("text_delta") ||
    type.includes("content.delta") ||
    type === "response.output_text.delta"
  )) {
    state.answer += delta;
  }

  if (type === "response.output_text.done" && typeof evt.text === "string" && !state.answer) {
    state.answer = evt.text;
  }

  if (type === "thread.item_done" && item.type === "assistant_message") {
    const content = item.content || [];
    for (const part of content) {
      if (typeof part.text === "string") state.answer += part.text;
      else if (typeof part === "string") state.answer += part;
    }
  }

  if (typeof evt.text === "string" && /assistant|output_text/.test(type) && !delta) {
    state.answer += evt.text;
  }
}

async function readSSE(response) {
  const state = { answer: "", threadId: null };
  if (!response.body) return state;

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  async function processLine(line) {
    line = line.trim();
    if (!line.startsWith("data:")) return;
    const payload = line.slice(5).trim();
    if (!payload || payload === "[DONE]") return;

    try {
      parseEvent(JSON.parse(payload), state);
    } catch {
      // Ignore non-JSON SSE events.
    }
  }

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    let index;
    while ((index = buffer.indexOf("\n")) !== -1) {
      const line = buffer.slice(0, index).replace(/\r$/, "");
      buffer = buffer.slice(index + 1);
      await processLine(line);
    }
  }

  buffer += decoder.decode();
  if (buffer.trim()) await processLine(buffer);

  return state;
}

async function askModel(text, model, reasoning = "medium") {
  const upstream = await fetch(GPT_OSS_URL, {
    method: "POST",
    headers: {
      ...BASE_HEADERS,
      "x-selected-model": model,
      "x-reasoning-effort": reasoning,
      "x-show-reasoning": "false",
    },
    body: JSON.stringify({
      op: "threads.create",
      params: {
        input: {
          text,
          content: [{ type: "input_text", text }],
          quoted_text: "",
          attachments: [],
        },
        threadId: null,
      },
    }),
  });

  if (!upstream.ok) {
    const detail = await upstream.text().catch(() => "");
    throw new Error(`Upstream HTTP ${upstream.status}: ${detail.slice(0, 500)}`);
  }

  const result = await readSSE(upstream);
  return result;
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);

    if (req.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "access-control-allow-origin": "*",
          "access-control-allow-methods": "GET, POST, OPTIONS",
          "access-control-allow-headers": "Content-Type, X-API-Key, X-Reasoning-Effort",
        },
      });
    }

    if (url.pathname === "/v1/models" && req.method === "GET") {
      return json({
        object: "list",
        data: MODELS.map(id => ({
          id,
          object: "model",
          owned_by: "gpt-oss",
        })),
      });
    }

    let text = "";
    let model = "gpt-oss-120b";
    let reasoning = "medium";

    if (url.pathname === "/ask" && req.method === "GET") {
      if (!env.API_KEY || url.searchParams.get("key") !== env.API_KEY) {
        return json({ ok: false, error: "unauthorized" }, 401);
      }

      text = (url.searchParams.get("text") || "").trim();
      model = url.searchParams.get("model") || model;
    } else if (url.pathname === "/v1/chat/completions" && req.method === "POST") {
      const body = await req.json().catch(() => null);
      if (!body) return json({ error: "invalid_json" }, 400);

      text = extractText(body.messages).trim();
      model = body.model || model;
      reasoning = body.metadata?.reasoning_effort || req.headers.get("X-Reasoning-Effort") || reasoning;
    } else if (url.pathname === "/") {
      return json({ ok: true, service: "GPT-OSS Worker", models: MODELS });
    } else {
      return json({ ok: false, error: "not_found" }, 404);
    }

    if (!text) return json({ ok: false, error: "text_required" }, 400);
    if (text.length > 12000) return json({ ok: false, error: "text_too_long" }, 413);
    if (!MODELS.includes(model)) return json({ ok: false, error: "unsupported_model" }, 400);

    try {
      const result = await askModel(text, model, reasoning);

      if (!result.answer.trim()) {
        console.error("No assistant text received. Thread:", result.threadId);
        return json({
          ok: false,
          error: "empty_ai_response",
          message: "The upstream service returned no recognizable answer text.",
          thread_id: result.threadId,
        }, 502);
      }

      if (url.pathname === "/ask") {
        return json({ ok: true, answer: result.answer.trim() });
      }

      return json({
        id: "chatcmpl_" + crypto.randomUUID().replace(/-/g, ""),
        object: "chat.completion",
        model,
        choices: [{
          index: 0,
          message: { role: "assistant", content: result.answer.trim() },
          finish_reason: "stop",
        }],
      });
    } catch (error) {
      console.error("AI request failed:", String(error));
      return json({ ok: false, error: "upstream_error" }, 502);
    }
  },
};
