// src/api.ts
export async function post(url: string) {
    const res = await fetch(url, { method: "POST", credentials: "include" });
    if (!res.ok && res.status !== 302) {
        const text = await res.text().catch(() => "");
        throw new Error(`POST ${url} failed: ${res.status} ${text}`);
    }
    return res;
}

export async function getJSON<T = unknown>(url: string): Promise<T> {
    const res = await fetch(url, { credentials: "include" });
    if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`GET ${url} failed: ${res.status} ${text}`);
    }
    return res.json() as Promise<T>;
}
