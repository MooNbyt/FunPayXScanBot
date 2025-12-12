"use client";

import { useEffect } from 'react';

// This component sends a request to the server every 4 minutes
// to prevent the hosting service (like OnRender) from putting the app to sleep.
export function KeepAlive() {
  useEffect(() => {
    const interval = setInterval(() => {
      fetch('/api/status').catch(error => {
        console.error('Keep-alive ping failed:', error);
      });
    }, 240000); // 240000ms = 4 minutes

    return () => clearInterval(interval);
  }, []);

  return null; // This component does not render anything
}
