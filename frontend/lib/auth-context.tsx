'use client'

import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'
import type { User } from './types'

interface AuthContextType {
  user: User | null
  isLoading: boolean
  login: (email: string, password: string) => Promise<void>
  signup: (email: string, password: string, name: string) => Promise<void>
  logout: () => void
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    // Check for stored session on mount
    const storedUser = localStorage.getItem('ez_insights_user')
    if (storedUser) {
      try {
        setUser(JSON.parse(storedUser))
      } catch {
        localStorage.removeItem('ez_insights_user')
      }
    }
    setIsLoading(false)
  }, [])

  const login = async (email: string, password: string) => {
    // Simulate API call - replace with real backend auth
    await new Promise(resolve => setTimeout(resolve, 500))
    
    if (!email || !password) {
      throw new Error('Email and password are required')
    }

    // Demo login - in production, this would validate against backend
    const newUser: User = {
      id: crypto.randomUUID(),
      email,
      name: email.split('@')[0],
    }
    
    setUser(newUser)
    localStorage.setItem('ez_insights_user', JSON.stringify(newUser))
  }

  const signup = async (email: string, password: string, name: string) => {
    // Simulate API call - replace with real backend auth
    await new Promise(resolve => setTimeout(resolve, 500))
    
    if (!email || !password || !name) {
      throw new Error('All fields are required')
    }

    if (password.length < 6) {
      throw new Error('Password must be at least 6 characters')
    }

    const newUser: User = {
      id: crypto.randomUUID(),
      email,
      name,
    }
    
    setUser(newUser)
    localStorage.setItem('ez_insights_user', JSON.stringify(newUser))
  }

  const logout = () => {
    setUser(null)
    localStorage.removeItem('ez_insights_user')
    localStorage.removeItem('ez_insights_chats')
  }

  return (
    <AuthContext.Provider value={{ user, isLoading, login, signup, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}
