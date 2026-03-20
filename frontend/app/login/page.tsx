'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { useAuth } from '@/lib/auth-context'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Field, FieldLabel, FieldGroup } from '@/components/ui/field'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Spinner } from '@/components/ui/spinner'
import { ThemeToggle } from '@/components/theme-toggle'
import { DatabaseIcon, SparklesIcon } from 'lucide-react'

export default function LoginPage() {
  const router = useRouter()
  const { login } = useAuth()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [isLoading, setIsLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setIsLoading(true)

    try {
      await login(email, password)
      router.push('/')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Login failed')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex flex-col bg-background theme-transition">
      {/* Header with theme toggle */}
      <header className="flex justify-end p-4 animate-fade-in">
        <ThemeToggle />
      </header>

      <main className="flex flex-1 items-center justify-center px-4 pb-16">
        <div className="w-full max-w-md">
          {/* Logo */}
          <div className="flex items-center justify-center gap-3 mb-8 animate-fade-in-up">
            <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-primary shadow-lg shadow-primary/25 transition-transform duration-300 hover:scale-105">
              <DatabaseIcon className="h-6 w-6 text-primary-foreground" />
            </div>
            <span className="text-2xl font-bold text-foreground">Ez-Insights</span>
          </div>

          <Card className="border-border shadow-xl shadow-primary/5 animate-fade-in-up animation-delay-100">
            <CardHeader className="text-center pb-2">
              <CardTitle className="text-2xl font-semibold">Welcome back</CardTitle>
              <CardDescription className="text-muted-foreground">
                Sign in to your account to continue
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-4">
              <form onSubmit={handleSubmit}>
                <FieldGroup>
                  <Field className="animate-fade-in animation-delay-200">
                    <FieldLabel htmlFor="email">Email</FieldLabel>
                    <Input
                      id="email"
                      type="email"
                      placeholder="you@example.com"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      required
                      autoComplete="email"
                      className="h-11 transition-all duration-200 focus:ring-4 focus:ring-primary/10"
                    />
                  </Field>

                  <Field className="animate-fade-in animation-delay-300">
                    <FieldLabel htmlFor="password">Password</FieldLabel>
                    <Input
                      id="password"
                      type="password"
                      placeholder="Enter your password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      required
                      autoComplete="current-password"
                      className="h-11 transition-all duration-200 focus:ring-4 focus:ring-primary/10"
                    />
                  </Field>

                  {error && (
                    <div className="rounded-lg bg-destructive/10 border border-destructive/20 px-4 py-3 animate-scale-in">
                      <p className="text-sm text-destructive">{error}</p>
                    </div>
                  )}

                  <Button 
                    type="submit" 
                    className="w-full h-11 mt-2 shadow-md shadow-primary/20 transition-all duration-300 hover:shadow-lg hover:shadow-primary/30 hover:scale-[1.02] active:scale-[0.98] animate-fade-in animation-delay-400" 
                    disabled={isLoading}
                  >
                    {isLoading ? (
                      <>
                        <Spinner className="mr-2" />
                        Signing in...
                      </>
                    ) : (
                      <>
                        Sign in
                        <SparklesIcon className="ml-2 h-4 w-4" />
                      </>
                    )}
                  </Button>
                </FieldGroup>
              </form>

              <p className="mt-6 text-center text-sm text-muted-foreground animate-fade-in animation-delay-500">
                {"Don't have an account? "}
                <Link 
                  href="/signup" 
                  className="font-medium text-primary hover:underline transition-colors duration-200 hover:text-primary/80"
                >
                  Sign up
                </Link>
              </p>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  )
}
