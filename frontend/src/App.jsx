import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Home from './pages/Home.jsx';
import SpotifyLinked from './pages/SpotifyLinked';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/spotify/linked" element={<SpotifyLinked />} />
      </Routes>
    </BrowserRouter>
  );
}
