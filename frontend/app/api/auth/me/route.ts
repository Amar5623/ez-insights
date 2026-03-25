import { NextResponse } from 'next/server'
import { getSessionFromCookie } from '@/lib/auth'

export async function GET() {
  const user = await getSessionFromCookie()
  if (!user) {
    return NextResponse.json({ detail: 'Not authenticated' }, { status: 401 })
  }
  return NextResponse.json({ user })
}