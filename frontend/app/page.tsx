'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'
import { useAuth } from '@/lib/auth-context'
import { ChatProvider } from '@/lib/chat-context'
import { ChatLayout } from '@/components/chat-layout'
import { Skeleton } from '@/components/ui/skeleton'

function LoadingScreen() {
  return (
    <div className="flex h-screen items-center justify-center bg-background">
      <div className="space-y-4 text-center">
        <Skeleton className="mx-auto h-12 w-12 rounded-lg" />
        <Skeleton className="mx-auto h-4 w-32" />
      </div>
    </div>
  )
}

export default function HomePage() {
  const router = useRouter()
  const { user, isLoading } = useAuth()

  useEffect(() => {
    if (!isLoading && !user) {
      router.push('/login')
    }
  }, [user, isLoading, router])

  if (isLoading) {
    return <LoadingScreen />
  }

  if (!user) {
    return <LoadingScreen />
  }

  return (
    <ChatProvider>
      <ChatLayout />
    </ChatProvider>
  )
}
