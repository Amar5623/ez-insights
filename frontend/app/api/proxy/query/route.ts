/**
 * app/api/proxy/query/route.ts
 *
 * Server-side SSE proxy for the backend /api/query endpoint.
 *
 * WHY THIS EXISTS:
 *   The backend requires an X-API-Key header. If the frontend calls the backend
 *   directly, the key must be exposed as NEXT_PUBLIC_API_KEY — readable by
 *   anyone who opens DevTools. This proxy runs on the Next.js server, reads the
 *   key from a server-only env var (BACKEND_API_KEY), adds it to the request,
 *   and streams the backend's SSE response back to the browser.
 *
 *   The browser never sees BACKEND_API_KEY. It only talks to /api/proxy/query.
 *
 * ENV VARS REQUIRED (in frontend .env.local):
 *   BACKEND_URL=http://localhost:8000        ← where the Python backend is
 *   BACKEND_API_KEY=your-secret-key          ← never prefixed with NEXT_PUBLIC_
 *
 * REMOVE from .env.local:
 *   NEXT_PUBLIC_API_KEY (delete this entirely)
 *   NEXT_PUBLIC_API_URL (delete this entirely — frontend now uses /api/proxy/*)
 */

import { NextRequest } from 'next/server'
import { cookies } from 'next/headers'

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8000'
const BACKEND_API_KEY = process.env.BACKEND_API_KEY || ''

export async function POST(req: NextRequest) {
  const body = await req.text()

  // Forward the user's JWT so the backend can identify the user
  const authHeader = req.headers.get('authorization') || ''

  // Forward chat context headers (set by the frontend when a chat is active)
  const chatId = req.headers.get('x-chat-id') || ''
  const userId = req.headers.get('x-user-id') || ''

  const backendHeaders: Record<string, string> = {
    'Content-Type': 'application/json',
    'X-API-Key': BACKEND_API_KEY,      // server-side only — never exposed to browser
  }
  if (authHeader) backendHeaders['Authorization'] = authHeader
  if (chatId) backendHeaders['X-Chat-Id'] = chatId
  if (userId) backendHeaders['X-User-Id'] = userId

  let backendRes: Response
  try {
    backendRes = await fetch(`${BACKEND_URL}/api/query`, {
      method: 'POST',
      headers: backendHeaders,
      body,
    })
  } catch (err) {
    return new Response(
      JSON.stringify({ error: 'Backend unreachable', detail: String(err) }),
      { status: 502, headers: { 'Content-Type': 'application/json' } },
    )
  }

  if (!backendRes.ok || !backendRes.body) {
    const errorText = await backendRes.text().catch(() => 'Unknown error')
    return new Response(errorText, {
      status: backendRes.status,
      headers: { 'Content-Type': 'application/json' },
    })
  }

  // Stream the SSE response straight through to the browser.
  // Next.js App Router supports streaming Response bodies natively.
  return new Response(backendRes.body, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  })
}