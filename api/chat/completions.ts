export const config = {
  runtime: 'edge',
};

const MAX_RETRIES = 2;
const INITIAL_RETRY_DELAY = 200;
const HEARTBEAT_INTERVAL_MS = 3000;

export default async function handler(req: Request) {
  const url = new URL(req.url);
  const path = url.pathname.replace(/^\/(api\/)?(v1\/)?/, '/');
  const targetUrl = `https://generativelanguage.googleapis.com/v1beta/openai${path}`;

  console.log(`[INBOUND] ${req.method} ${url.pathname} -> ${targetUrl}`);

  const headers = new Headers(req.headers);
  headers.delete('host');
  headers.delete('origin');

  const apiKey = process.env.GOOGLE_API_KEY;
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'Missing GOOGLE_API_KEY' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
  headers.set('Authorization', `Bearer ${apiKey}`);
  if (!headers.has('content-type')) {
    headers.set('Content-Type', 'application/json');
  }

  let body: string | null = null;
  let clientWantsStream = false;

  if (req.method !== 'GET' && req.method !== 'HEAD') {
    body = await req.text();
    if (body) {
      try {
        const parsed = JSON.parse(body);
        clientWantsStream = parsed.stream === true;
        // Force streaming from upstream for keep-alive benefits
        parsed.stream = true;
        // Always force high reasoning for Gemini models
        parsed.reasoning_effort = 'high';
        body = JSON.stringify(parsed);
      } catch {}
    }
    headers.set('Content-Length', String(body?.length ?? 0));
  }

  let lastError: any = null;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      console.log(`[ATTEMPT ${attempt + 1}/${MAX_RETRIES + 1}] ${req.method} ${targetUrl}`);
      const response = await fetch(targetUrl, {
        method: req.method,
        headers,
        body: body ?? undefined,
      });

      if (!response.ok) {
        const errText = await response.text();
        console.log(`Upstream error ${response.status}: ${errText.slice(0, 200)}`);
        if (attempt < MAX_RETRIES) {
          const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt);
          await new Promise(resolve => setTimeout(resolve, delay));
          if (body) headers.set('Content-Length', String(body.length));
          continue;
        }
        return new Response(errText, {
          status: response.status,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (!response.body) {
        const data = await response.text();
        return new Response(data, {
          status: response.status,
          headers: response.headers,
        });
      }

      if (clientWantsStream) {
        return relayStreamWithHeartbeat(response);
      } else {
        return await bufferStreamToJson(response);
      }
    } catch (error: any) {
      lastError = error;
      if (attempt < MAX_RETRIES) {
        const delay = INITIAL_RETRY_DELAY * Math.pow(2, attempt);
        console.log(`Fetch error: ${error.message}, retrying in ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }

  console.error('All retries exhausted:', lastError);
  return new Response(JSON.stringify({ error: 'Proxy Error', message: lastError?.message || 'Unknown error' }), {
    status: 502,
    headers: { 'Content-Type': 'application/json' },
  });
}

function relayStreamWithHeartbeat(upstream: Response): Response {
  const reader = upstream.body!.getReader();
  const encoder = new TextEncoder();
  let heartbeatId: ReturnType<typeof setInterval>;
  let firstChunkReceived = false;

  const stream = new ReadableStream({
    start(controller) {
      heartbeatId = setInterval(() => {
        if (!firstChunkReceived) {
          controller.enqueue(encoder.encode(': keep-alive\n\n'));
          console.log('[HEARTBEAT] sent keep-alive');
        }
      }, HEARTBEAT_INTERVAL_MS);

      (async () => {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            firstChunkReceived = true;
            controller.enqueue(value);
          }
        } catch (err) {
          console.error('[STREAM ERROR]', err);
        } finally {
          clearInterval(heartbeatId);
          controller.close();
        }
      })();
    },
    cancel() {
      clearInterval(heartbeatId);
      reader.cancel();
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
}

async function bufferStreamToJson(upstream: Response): Promise<Response> {
  const reader = upstream.body!.getReader();
  const decoder = new TextDecoder();

  const sseChunks: string[] = [];
  let firstChunkReceived = false;

  // Start a heartbeat that writes to a dummy writer
  // The real purpose is to keep the Vercel function alive by doing periodic work
  let heartbeatId: ReturnType<typeof setInterval>;
  const heartbeatPromise = new Promise<void>((resolve) => {
    heartbeatId = setInterval(() => {
      if (!firstChunkReceived) {
        // Just logging to keep the function event loop active
        console.log('[HEARTBEAT] keep-alive tick');
      }
    }, HEARTBEAT_INTERVAL_MS);
  });

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      firstChunkReceived = true;
      const text = decoder.decode(value, { stream: true });
      sseChunks.push(text);
    }
  } catch (err) {
    console.error('[BUFFER ERROR]', err);
  } finally {
    clearInterval(heartbeatId!);
  }

  const fullText = sseChunks.join('');
  const assembled = reassembleSseToJson(fullText);

  return new Response(JSON.stringify(assembled), {
    status: 200,
    headers: {
      'Content-Type': 'application/json',
    },
  });
}

function reassembleSseToJson(sseText: string): any {
  const lines = sseText.split('\n');
  let content = '';
  let model = '';
  let finishReason = '';
  let promptTokens = 0;
  let completionTokens = 0;
  let id = '';
  let hasThought = false;

  for (const line of lines) {
    if (!line.startsWith('data: ') || line === 'data: [DONE]') continue;
    try {
      const chunk = JSON.parse(line.slice(6));
      if (chunk.model) model = chunk.model;
      if (chunk.id) id = chunk.id;

      const choice = chunk.choices?.[0];
      if (choice) {
        if (choice.finish_reason) finishReason = choice.finish_reason;
        const delta = choice.delta;
        if (delta) {
          // Check if this is a thinking chunk
          if (delta.extra_content?.google?.thought) {
            hasThought = true;
            // Skip adding thinking content to the main content
            continue;
          }
          if (delta.content) content += delta.content;
        }
        if (chunk.usage) {
          promptTokens = chunk.usage.prompt_tokens || promptTokens;
          completionTokens = chunk.usage.completion_tokens || completionTokens;
        }
      }
    } catch {}
  }

  // Strip <thought>...</thought> tags and any leftover partial tags
  content = content.replace(/<thought>[\s\S]*?<\/thought>/g, '');
  content = content.replace(/<\/thought>/g, '');
  content = content.replace(/<thought>/g, '');
  content = content.trim();

  const message: any = {
    role: 'assistant',
    content,
  };

  if (hasThought) {
    message.extra_content = { google: { thought: true } };
  }

  return {
    id: id || `proxy-${Date.now()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: model || 'gemma-4-31b-it',
    choices: [
      {
        index: 0,
        message,
        finish_reason: finishReason || 'stop',
      },
    ],
    usage: {
      prompt_tokens: promptTokens,
      completion_tokens: completionTokens,
      total_tokens: promptTokens + completionTokens,
    },
  };
}
