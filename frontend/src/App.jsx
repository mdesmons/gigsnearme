import React, { useState } from 'react';

const categories = ['music', 'culture', 'sex-positive', 'workshop', 'talk', 'other'];

function MatchingForm({ onSubmit }) {
  const [form, setForm] = useState({
    start_date: '',
    end_date: '',
    category: categories[0],
    description: '',
    venues: '',
  });

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm({ ...form, [name]: value });
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const request = {
      start_date: form.start_date,
      end_date: form.end_date,
      category: form.category,
      description: form.description,
      venues: form.venues.split(',').map(v => v.trim()).filter(Boolean),
    };
    onSubmit(request);
  };

  return (
    <form onSubmit={handleSubmit}>
      <label>
        Start Date
        <input type="date" name="start_date" value={form.start_date} onChange={handleChange} />
      </label>
      <label>
        End Date
        <input type="date" name="end_date" value={form.end_date} onChange={handleChange} />
      </label>
      <label>
        Category
        <select name="category" value={form.category} onChange={handleChange}>
          {categories.map(cat => (
            <option key={cat} value={cat}>{cat}</option>
          ))}
        </select>
      </label>
      <label>
        Description
        <textarea name="description" value={form.description} onChange={handleChange} />
      </label>
      <label>
        Venues (comma separated)
        <input type="text" name="venues" value={form.venues} onChange={handleChange} />
      </label>
      <button type="submit">Find Events</button>
    </form>
  );
}

function EventCard({ event }) {
  const [open, setOpen] = useState(false);
  const image = event.images && event.images.length > 0 ? event.images[0] : null;

  return (
    <div className="card">
      {image && <img src={image} alt={event.title} />}
      <div className="card-content">
        <h3 className="card-title">{event.title}</h3>
        <p className="card-caption">{event.caption}</p>
        <p className="card-venue">{event.venue_name}</p>
        <button className="toggle" onClick={() => setOpen(!open)}>
          {open ? 'Hide' : 'Show'} description
        </button>
        {open && <p>{event.description}</p>}
      </div>
    </div>
  );
}

export default function App() {
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSubmit = async (request) => {
    setLoading(true);
    setError(null);
    setEvents([]);
    try {
      const response = await fetch('/api/match', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
      });
      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }
      const data = await response.json();
      setEvents(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="container">
      <h1>Gigs Near Me</h1>
      <MatchingForm onSubmit={handleSubmit} />
      {loading && <p>Loading...</p>}
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {events.map(event => (
        <EventCard key={`${event.source_name}-${event.title}`} event={event} />
      ))}
    </div>
  );
}
