// src/components/ConnectSpotifyButton.tsx
import React from "react";

export default function ConnectSpotifyButton() {
    const onClick = async () => {
        // Hit your broker start endpoint — it will set cookies and 302 to Spotify
        // We use a form POST to avoid CORS preflight surprises.
        const form = document.createElement("form");
        form.method = "POST";
        form.action = "/api/auth/spotify/start";
        document.body.appendChild(form);
        form.submit();
    };

    return (
        <button
            onClick={onClick}
            style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
                padding: "10px 14px",
                borderRadius: 999,
                border: "1px solid #1DB954",
                background: "#1DB954",
                color: "white",
                fontWeight: 600,
                cursor: "pointer",
            }}
            aria-label="Connect your Spotify account"
        >
            {/* Simple SVG so you don’t need an asset */}
            <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" aria-hidden>
                <path d="M12 0a12 12 0 1 0 .001 24.001A12 12 0 0 0 12 0Zm5.485 17.4a.936.936 0 0 1-1.288.31c-3.53-2.156-7.977-2.64-13.21-1.435a.936.936 0 1 1-.416-1.826c5.67-1.292 10.51-.74 14.35 1.57.45.276.594.872.314 1.38Zm1.69-3.76a1.17 1.17 0 0 1-1.614.388c-4.042-2.468-10.207-3.187-14.994-1.731a1.17 1.17 0 0 1-.66-2.24c5.39-1.59 12.2-.8 16.807 1.963.55.34.724 1.062.46 1.62Zm.157-3.88c-4.57-2.713-12.13-2.96-16.5-1.62a1.4 1.4 0 1 1-.8-2.68c5.02-1.5 13.33-1.2 18.53 1.86a1.4 1.4 0 0 1-1.23 2.44Z" />
            </svg>
            Connect Spotify
        </button>
    );
}
