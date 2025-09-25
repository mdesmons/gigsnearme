import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Home from './pages/Home.jsx';
import SpotifyLinked from './pages/SpotifyLinked';

// src/main.tsx or src/App.tsx (top-level)
import { useEffect } from "react";
import { ensureSession } from "./session";

export default function App() {
    useEffect(() => {
        ensureSession().catch(() => {/* ignore; retry on button click anyway */});
    }, []);

    return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/spotify/linked" element={<SpotifyLinked />} />
      </Routes>
    </BrowserRouter>
  );
}
