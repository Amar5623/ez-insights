/**
 * app/api/proxy/[...path]/route.ts
 *
 * Generic reverse proxy for all non-streaming backend routes.
 * Covers: /api/chats/*, /api/history/*, /api/health
 *
 * Usage: frontend calls /api/proxy/chats?user_id=...
 *        → this route forwards to BACKEND_URL/api/chats?user_id=...
 *        → adds X-API-Key server-side
 *        → returns the response
 *
 * USER AUTHORIZATION:
 *   For chat routes that accept user_id as a query param, we validate that
 *   the requested user_id matches the authenticated user from the JWT.
 *   This prevents user A from reading user B's chats by changing the query param.
 */

import { NextRequest, NextResponse } from 'next/server'
import { verifyToken } from '@/lib/auth'

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8000'
const BACKEND_API_KEY = process.env.BACKEND_API_KEY || ''

// Routes that require user_id ownership validation
const USER_SCOPED_PREFIXES = ['/api/chats']

async function getAuthenticatedUserId(req: NextRequest): Promise<string | null> {
  const authHeader = req.headers.get('authorization') || ''
  if (!authHeader.startsWith('Bearer ')) return null
  const token = authHeader.slice(7)
  const session = await verifyToken(token)
  return session?.id ?? null
}

async function handler(
  req: NextRequest,
  { params }: { params: Promise<{ path: string[] }> },
) {
  const { path } = await params
  const backendPath = '/api/' + path.join('/')
  const search = req.nextUrl.search  // preserve query string

  // ── User-level authorization ──────────────────────────────────────────────
  // For chat routes: verify the user_id in the query matches the JWT owner.
  if (USER_SCOPED_PREFIXES.some(p => backendPath.startsWith(p))) {
    const requestedUserId = req.nextUrl.searchParams.get('user_id')
    if (requestedUserId) {
      const authenticatedUserId = await getAuthenticatedUserId(req)
      if (!authenticatedUserId) {
        return NextResponse.json({ detail: 'Not authenticated' }, { status: 401 })
      }
      if (authenticatedUserId !== requestedUserId) {
        return NextResponse.json({ detail: 'Forbidden' }, { status: 403 })
      }
    }
  }

  // ── Forward request to backend ─────────────────────────────────────────────
  const targetUrl = `${BACKEND_URL}${backendPath}${search}`

  const forwardHeaders: Record<string, string> = {
    'X-API-Key': BACKEND_API_KEY,
  }

  // Forward Content-Type for POST/PATCH bodies
  const contentType = req.headers.get('content-type')
  if (contentType) forwardHeaders['Content-Type'] = contentType

  // Forward auth header
  const authHeader = req.headers.get('authorization')
  if (authHeader) forwardHeaders['Authorization'] = authHeader

  let body: string | null = null
  if (['POST', 'PUT', 'PATCH'].includes(req.method)) {
    body = await req.text()
  }

  let backendRes: Response
  try {
    backendRes = await fetch(targetUrl, {
      method: req.method,
      headers: forwardHeaders,
      body: body ?? undefined,
    })
  } catch (err) {
    return NextResponse.json(
      { detail: 'Backend unreachable', error: String(err) },
      { status: 502 },
    )
  }

  const responseBody = await backendRes.text()
  const responseContentType = backendRes.headers.get('content-type') || 'application/json'

  return new Response(responseBody, {
    status: backendRes.status,
    headers: { 'Content-Type': responseContentType },
  })
}

export const GET = handler
export const POST = handler
export const PATCH = handler
export const DELETE = handler