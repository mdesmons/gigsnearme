// src/pages/SpotifyLinked.tsx
import React, { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { getJSON } from "../api";

type SavedTracksResponse = {
    total: number;
    items: Array<{
        added_at: string;
        track: { id: string; name: string; artists: Array<{ name: string }>; album: { name: string } };
    }>;
};

export default function SpotifyLinked() {
    const [status, setStatus] = useState<"idle" | "ok" | "err">("idle");
    const [err, setErr] = useState<string>("");
    const [sample, setSample] = useState<Array<string>>([]);
    const [searchParams] = useSearchParams();

    useEffect(() => {
        const error = searchParams.get("error");
        if (error) {
            setStatus("err");
            setErr(error);
        } else {
            setStatus("ok");
        }
    }, [searchParams]);

    const fetchLiked = async () => {
        try {
            const data = await getJSON<SavedTracksResponse>("/api/spotify/liked");
            const names = (data.items || []).slice(0, 5).map(
                (it) => `${it.track.name} — ${it.track.artists.map((a) => a.name).join(", ")}`
            );
            setSample(names);
        } catch (e: any) {
            setErr(e.message || "Failed to fetch liked songs");
            setStatus("err");
        }
    };

    return (
        <div style={{ maxWidth: 680, margin: "40px auto", padding: 16 }}>
            {status === "ok" ? (
                <>
                    <h1>Spotify linked ✅</h1>
                    <p>Your Spotify account is now connected. You can close this page or try fetching a few liked songs:</p>
                    <button onClick={fetchLiked} style={{ padding: "8px 12px", borderRadius: 8, cursor: "pointer" }}>
                        Fetch liked songs (sample)
                    </button>
                    {sample.length > 0 && (
                        <div style={{ marginTop: 16 }}>
                            <h3>Sample</h3>
                            <ul>{sample.map((s, i) => <li key={i}>{s}</li>)}</ul>
                        </div>
                    )}
                </>
            ) : status === "err" ? (
                <>
                    <h1>Spotify link failed ❌</h1>
                    <p style={{ color: "crimson" }}>{err}</p>
                    <Link to="/">Back</Link>
                </>
            ) : null}
        </div>
    );
}
