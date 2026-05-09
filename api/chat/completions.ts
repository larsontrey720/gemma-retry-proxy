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
        parsed.stream = true;
        // Only force high reasoning if no tools are specified (tools + reasoning requires thought signatures)
        if (!parsed.tools || parsed.tools.length === 0) {
          parsed.reasoning_effort = 'high';
        }
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
        return relayStreamWithThoughtTransform(response);
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

// Streaming path for thought transformation
function relayStreamWithThoughtTransform(upstream: Response): Response {
  const reader = upstream.body!.getReader();
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  let heartbeatId: ReturnType<typeof setInterval>;
  let firstChunkReceived = false;
  let sseBuffer = '';
  let hadToolCalls = false;

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

            sseBuffer += decoder.decode(value, { stream: true });
            const lines = sseBuffer.split('\n');
            sseBuffer = lines.pop() ?? '';

            for (const line of lines) {
              if (!line.startsWith('data: ')) {
                if (line.startsWith(':') || line === '') {
                  controller.enqueue(encoder.encode(line + '\n'));
                }
                continue;
              }

              if (line === 'data: [DONE]') {
                controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                continue;
              }

              let chunk: any;
              try {
                chunk = JSON.parse(line.slice(6));
              } catch {
                controller.enqueue(encoder.encode(line + '\n\n'));
                continue;
              }

              const choice = chunk.choices?.[0];
              const delta = choice?.delta;

              if (!delta) {
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
                continue;
              }

              // Detect reasoning content via new native field OR legacy flag
              const hasReasoning = delta.reasoning_content !== undefined && delta.reasoning_content !== null;
              const legacyThought = delta.extra_content?.google?.thought === true;
              const isThought = hasReasoning || legacyThought;

              // Tool calls - always emit first, ignoring any text/reasoning on same chunk
              const hasToolCalls = Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0;
              if (hasToolCalls) {
                hadToolCalls = true;
                // Filter tool calls to ensure they have valid structure (avoid thought signature errors)
                const validToolCalls = delta.tool_calls.filter((tc: any) => {
                  if (!tc.function?.name || !tc.id) return false;
                  return true;
                });
                const toolChunk = {
                  ...chunk,
                  choices: [{
                    ...choice,
                    delta: {
                      role: 'assistant',
                      content: null,
                      tool_calls: validToolCalls,
                    },
                    finish_reason: 'tool_calls',
                  }],
                };
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(toolChunk)}\n\n`));
                continue;
              }

              // Reasoning token
              if (isThought) {
                const reasoningText = delta.reasoning_content ?? delta.content ?? '';
                const reasoningChunk = {
                  ...chunk,
                  choices: [{
                    ...choice,
                    delta: {
                      role: 'assistant',
                      reasoning_content: reasoningText,
                      content: null,
                    },
                  }],
                };
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(reasoningChunk)}\n\n`));
                continue;
              }

              // Regular text content
              const rawContent = delta.content ?? '';
              if (rawContent) {
                const contentChunk = {
                  ...chunk,
                  choices: [{
                    ...choice,
                    delta: {
                      role: 'assistant',
                      content: rawContent,
                    },
                  }],
                };
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(contentChunk)}\n\n`));
                continue;
              }

              // Metadata chunk
              if (hadToolCalls && choice?.finish_reason === 'stop') {
                const fixedChunk = {
                  ...chunk,
                  choices: [{
                    ...choice,
                    finish_reason: 'tool_calls',
                  }],
                };
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(fixedChunk)}\n\n`));
              } else {
                controller.enqueue(encoder.encode(`data: ${JSON.stringify(chunk)}\n\n`));
              }
            }
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

// Non-streaming (buffered) path
async function bufferStreamToJson(upstream: Response): Promise<Response> {
  const reader = upstream.body!.getReader();
  const decoder = new TextDecoder();
  const sseChunks: string[] = [];
  let firstChunkReceived = false;
  let heartbeatId: ReturnType<typeof setInterval>;

  heartbeatId = setInterval(() => {
    if (!firstChunkReceived) {
      console.log('[HEARTBEAT] keep-alive tick');
    }
  }, HEARTBEAT_INTERVAL_MS);

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      firstChunkReceived = true;
      sseChunks.push(decoder.decode(value, { stream: true }));
    }
  } catch (err) {
    console.error('[BUFFER ERROR]', err);
  } finally {
    clearInterval(heartbeatId);
  }

  const assembled = reassembleSseToJson(sseChunks.join(''));

  return new Response(JSON.stringify(assembled), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
}

function reassembleSseToJson(sseText: string): any {
  const lines = sseText.split('\n');
  let reasoningContent = '';
  let content = '';
  let model = '';
  let finishReason = '';
  let promptTokens = 0;
  let completionTokens = 0;
  let id = '';
  const toolCalls: any[] = [];
  let hadToolCalls = false;

  for (const line of lines) {
    if (!line.startsWith('data: ') || line === 'data: [DONE]') continue;
    try {
      const chunk = JSON.parse(line.slice(6));
      if (chunk.model) model = chunk.model;
      if (chunk.id) id = chunk.id;

      if (chunk.usage) {
        promptTokens = chunk.usage.prompt_tokens || promptTokens;
        completionTokens = chunk.usage.completion_tokens || completionTokens;
      }

      const choice = chunk.choices?.[0];
      if (!choice) continue;

      if (choice.finish_reason) finishReason = choice.finish_reason;

      const delta = choice.delta;
      if (!delta) continue;

      // Skip everything if this delta contains tool calls - filter out invalid ones
      const hasToolCalls = Array.isArray(delta.tool_calls) && delta.tool_calls.length > 0;
      if (hasToolCalls) {
        hadToolCalls = true;
        for (const tc of delta.tool_calls) {
          if (!tc.function?.name || !tc.id) continue;
          const idx = tc.index ?? 0;
          if (!toolCalls[idx]) {
            toolCalls[idx] = {
              id: tc.id,
              type: tc.type ?? 'function',
              function: { name: tc.function.name, arguments: '' },
            };
          }
          if (tc.function?.arguments) toolCalls[idx].function.arguments += tc.function.arguments;
        }
        continue;
      }

      // Detect reasoning content
      const hasReasoning = delta.reasoning_content !== undefined && delta.reasoning_content !== null;
      const legacyThought = delta.extra_content?.google?.thought === true;
      if (hasReasoning || legacyThought) {
        reasoningContent += delta.reasoning_content ?? delta.content ?? '';
      } else if (delta.content) {
        content += delta.content;
      }
    } catch {}
  }

  content = content.trim();
  reasoningContent = reasoningContent.trim();

  if (hadToolCalls) finishReason = 'tool_calls';

  const message: any = {
    role: 'assistant',
    content: content || null,
  };

  if (reasoningContent) message.reasoning_content = reasoningContent;
  if (toolCalls.length > 0) message.tool_calls = toolCalls;

  return {
    id: id || `proxy-${Date.now()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: model || 'gemma-4-31b-it',
    choices: [{ index: 0, message, finish_reason: finishReason || 'stop' }],
    usage: {
      prompt_tokens: promptTokens,
      completion_tokens: completionTokens,
      total_tokens: promptTokens + completionTokens,
    },
  };
}
