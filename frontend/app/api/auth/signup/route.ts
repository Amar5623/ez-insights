import { NextRequest, NextResponse } from 'next/server'
import bcrypt from 'bcryptjs'
import { createUser, findUserByEmail, signToken, setSessionCookie } from '@/lib/auth'
import { MongoServerError } from 'mongodb'

export async function POST(req: NextRequest) {
  try {
    const { email, password, name } = await req.json()

    // Validate
    if (!email || !password || !name) {
      return NextResponse.json({ detail: 'All fields are required' }, { status: 400 })
    }
    if (typeof email !== 'string' || !email.includes('@')) {
      return NextResponse.json({ detail: 'Invalid email address' }, { status: 400 })
    }
    if (password.length < 8) {
      return NextResponse.json({ detail: 'Password must be at least 8 characters' }, { status: 400 })
    }
    if (name.trim().length < 2) {
      return NextResponse.json({ detail: 'Name must be at least 2 characters' }, { status: 400 })
    }

    // Check duplicate
    const existing = await findUserByEmail(email)
    if (existing) {
      return NextResponse.json({ detail: 'An account with this email already exists' }, { status: 409 })
    }

    // Hash password & create user
    const hashed = await bcrypt.hash(password, 12)
    const userId = await createUser(email, hashed, name.trim())

    // Sign JWT & set cookie
    const token = await signToken({ id: userId, email: email.toLowerCase(), name: name.trim() })
    await setSessionCookie(token)

    return NextResponse.json({
      user: { id: userId, email: email.toLowerCase(), name: name.trim() },
    }, { status: 201 })

  } catch (err) {
    if (err instanceof MongoServerError && err.code === 11000) {
      return NextResponse.json({ detail: 'An account with this email already exists' }, { status: 409 })
    }
    console.error('[signup]', err)
    return NextResponse.json({ detail: 'Something went wrong' }, { status: 500 })
  }
}