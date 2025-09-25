// src/session.ts
export async function ensureSession() {
    await fetch("/api/session/init", { method: "POST", credentials: "include" });
}
