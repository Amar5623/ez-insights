'use client'
import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import type { User } from './types'

interface AuthContextType {
  user: User | null
  token: string | null
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  signup: (email: string, password: string, name: string) => Promise<void>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextType | null>(null)

const TOKEN_KEY = 'ez_insights_token'

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  // On mount, restore token from localStorage and verify with /api/auth/me
  useEffect(() => {
    const storedToken = localStorage.getItem(TOKEN_KEY)
    if (!storedToken) {
      setIsLoading(false)
      return
    }
    setToken(storedToken)
    fetch('/api/auth/me', {
      headers: { Authorization: `Bearer ${storedToken}` },
    })
      .then(res => (res.ok ? res.json() : null))
      .then(data => {
        if (data?.user) {
          setUser(data.user)
        } else {
          // Token is stale — clear it
          setToken(null)
          localStorage.removeItem(TOKEN_KEY)
        }
      })
      .catch(() => {
        setToken(null)
        localStorage.removeItem(TOKEN_KEY)
      })
      .finally(() => setIsLoading(false))
  }, [])

  const login = async (email: string, password: string) => {
    const res = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || 'Login failed')
    setUser(data.user)
    setToken(data.token)
    localStorage.setItem(TOKEN_KEY, data.token)
  }

  const signup = async (email: string, password: string, name: string) => {
    const res = await fetch('/api/auth/signup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password, name }),
    })
    const data = await res.json()
    if (!res.ok) throw new Error(data.detail || 'Signup failed')
    setUser(data.user)
    setToken(data.token)
    localStorage.setItem(TOKEN_KEY, data.token)
  }

  const logout = async () => {
    await fetch('/api/auth/logout', {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    })
    setUser(null)
    setToken(null)
    localStorage.removeItem(TOKEN_KEY)
    localStorage.removeItem('ez_insights_chats')
  }

  return (
    <AuthContext.Provider value={{ user, token, isLoading, login, signup, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) throw new Error('useAuth must be used within an AuthProvider')
  return context
}