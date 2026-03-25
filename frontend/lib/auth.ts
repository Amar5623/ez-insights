import { SignJWT, jwtVerify } from 'jose'
import { cookies } from 'next/headers'
import clientPromise from './db'

const JWT_SECRET = new TextEncoder().encode(process.env.JWT_SECRET!)
const COOKIE_NAME = 'ez_session'
const DB_NAME = process.env.MONGODB_DB_NAME_AUTH || 'ez_insights_auth'

// ── Types ────────────────────────────────────────────────────────────────────

export interface SessionUser {
  id: string
  email: string
  name: string
}

// ── JWT ──────────────────────────────────────────────────────────────────────

export async function signToken(payload: SessionUser): Promise<string> {
  return new SignJWT({ ...payload })
    .setProtectedHeader({ alg: 'HS256' })
    .setIssuedAt()
    .setExpirationTime('7d')
    .sign(JWT_SECRET)
}

export async function verifyToken(token: string): Promise<SessionUser | null> {
  try {
    const { payload } = await jwtVerify(token, JWT_SECRET)
    return {
      id: payload.id as string,
      email: payload.email as string,
      name: payload.name as string,
    }
  } catch {
    return null
  }
}

// ── Session cookie ────────────────────────────────────────────────────────────

export async function setSessionCookie(token: string) {
  const cookieStore = await cookies()
  cookieStore.set(COOKIE_NAME, token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 60 * 60 * 24 * 7, // 7 days
    path: '/',
  })
}

export async function clearSessionCookie() {
  const cookieStore = await cookies()
  cookieStore.delete(COOKIE_NAME)
}

export async function getSessionFromCookie(): Promise<SessionUser | null> {
  const cookieStore = await cookies()
  const token = cookieStore.get(COOKIE_NAME)?.value
  if (!token) return null
  return verifyToken(token)
}

// ── MongoDB user operations ───────────────────────────────────────────────────

export async function getUsersCollection() {
  const client = await clientPromise
  const db = client.db(DB_NAME)
  const users = db.collection('users')
  // Ensure unique index on email
  await users.createIndex({ email: 1 }, { unique: true })
  return users
}

export async function findUserByEmail(email: string) {
  const users = await getUsersCollection()
  return users.findOne({ email: email.toLowerCase() })
}

export async function createUser(email: string, hashedPassword: string, name: string) {
  const users = await getUsersCollection()
  const result = await users.insertOne({
    email: email.toLowerCase(),
    password: hashedPassword,
    name,
    createdAt: new Date(),
  })
  return result.insertedId.toString()
}