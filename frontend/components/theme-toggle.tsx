'use client'

import { useTheme } from 'next-themes'
import { useEffect, useState } from 'react'
import { Button } from '@/components/ui/button'
import { SunIcon, MoonIcon } from 'lucide-react'
import { cn } from '@/lib/utils'

export function ThemeToggle({ className }: { className?: string }) {
  const { theme, setTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) {
    return (
      <Button variant="ghost" size="icon" className={cn("h-9 w-9", className)}>
        <span className="h-4 w-4" />
      </Button>
    )
  }

  return (
    <Button
      variant="ghost"
      size="icon"
      onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
      className={cn(
        "h-9 w-9 relative overflow-hidden",
        className
      )}
      title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
    >
      <SunIcon 
        className={cn(
          "h-4 w-4 absolute transition-all duration-300 ease-out",
          theme === 'dark' 
            ? "rotate-0 scale-100 opacity-100" 
            : "rotate-90 scale-0 opacity-0"
        )} 
      />
      <MoonIcon 
        className={cn(
          "h-4 w-4 absolute transition-all duration-300 ease-out",
          theme === 'dark' 
            ? "-rotate-90 scale-0 opacity-0" 
            : "rotate-0 scale-100 opacity-100"
        )} 
      />
      <span className="sr-only">Toggle theme</span>
    </Button>
  )
}
