'use client'

import { useState } from 'react'
import { ChatSidebar } from './chat-sidebar'
import { ChatMessages } from './chat-messages'
import { ChatInput } from './chat-input'

export function ChatLayout() {
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <div className="flex h-screen overflow-hidden bg-background">
      {/* Sidebar */}
      <ChatSidebar isOpen={sidebarOpen} onToggle={() => setSidebarOpen(o => !o)} />

      {/* Main Chat Area */}
      <main className="flex flex-1 flex-col overflow-hidden">
        <ChatMessages />
        <ChatInput />
      </main>
    </div>
  )
}
