import { NextRequest, NextResponse } from 'next/server'
import bcrypt from 'bcryptjs'
import { findUserByEmail, signToken, setSessionCookie } from '@/lib/auth'

export async function POST(req: NextRequest) {
  try {
    const { email, password } = await req.json()

    if (!email || !password) {
      return NextResponse.json({ detail: 'Email and password are required' }, { status: 400 })
    }

    const user = await findUserByEmail(email)

    // Use constant-time comparison to avoid timing attacks
    const passwordMatch = user ? await bcrypt.compare(password, user.password) : false
    if (!user || !passwordMatch) {
      return NextResponse.json({ detail: 'Invalid email or password' }, { status: 401 })
    }

    const userId = user._id.toString()
    const token = await signToken({ id: userId, email: user.email, name: user.name })
    await setSessionCookie(token)

    // Return token in body so auth-context can store it in localStorage
    // (same pattern as signup route — required for proxyFetch Authorization header)
    return NextResponse.json({
      user: { id: userId, email: user.email, name: user.name },
      token,
    })

  } catch (err) {
    console.error('[login]', err)
    return NextResponse.json({ detail: 'Something went wrong' }, { status: 500 })
  }
}