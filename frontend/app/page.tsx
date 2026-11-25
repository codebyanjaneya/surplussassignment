'use client';
import { Fragment, useCallback, useEffect, useState } from 'react';

type Attempt = {
  attemptNumber: number; workerId: string; startedAt: string;
  finishedAt: string | null; result: string; error: string | null;
};
type EventRow = {
  eventId: string; type: string; payload: any; status: string; attempts: number;
  attemptsInBudget: number; nextRunAt: string; leaseOwner: string | null;
  lastError: string | null; receivedAt: string; history: Attempt[];
};
type Stats = Record<string, number>;

const COLORS: Record<string, string> = {
  pending: '#b58900', processing: '#268bd2', succeeded: '#2a9d4b', failed: '#d33',
  lease_expired: '#d33', running: '#268bd2',
};
const Badge = ({ s }: { s: string }) => (
  <span style={{ color: '#fff', background: COLORS[s] ?? '#666', padding: '1px 8px', borderRadius: 10, fontSize: 12 }}>{s}</span>
);
const fmt = (t: string | null) => (t ? new Date(t).toLocaleTimeString() : '—');
const th: React.CSSProperties = { textAlign: 'left', borderBottom: '2px solid #ccc', padding: '4px 8px' };
const td: React.CSSProperties = { borderBottom: '1px solid #eee', padding: '4px 8px', verticalAlign: 'top' };

export default function Page() {
  const [events, setEvents] = useState<EventRow[]>([]);
  const [stats, setStats] = useState<Stats>({});
  const [open, setOpen] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const [e, s] = await Promise.all([
        fetch('/api/events?limit=200', { cache: 'no-store' }).then((r) => r.json()),
        fetch('/api/stats', { cache: 'no-store' }).then((r) => r.json()),
      ]);
      if (!Array.isArray(e)) throw new Error(e.message ?? 'bad response');
      setEvents(e);
      setStats(s);
      setErr(null);
    } catch (x: any) {
      setErr(x.message);
    }
  }, []);

  useEffect(() => {
    load();
    const t = setInterval(load, 1500);
    return () => clearInterval(t);
  }, [load]);

  const retry = async (id: string) => {
    const r = await fetch(`/api/events/${encodeURIComponent(id)}/retry`, { method: 'POST' });
    if (!r.ok) setErr((await r.json()).message);
    load();
  };

  return (
    <main>
      <h1 style={{ marginTop: 0 }}>Webhook operations</h1>
      {err && <p style={{ color: '#d33' }}>Error: {err}</p>}
      <p>
        {['pending', 'processing', 'succeeded', 'failed'].map((k) => (
          <span key={k} style={{ marginRight: 16 }}><Badge s={k} /> {stats[k] ?? 0}</span>
        ))}
        <span><b>processed_orders rows:</b> {stats.processedOrders ?? 0}</span>
      </p>
      <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 14 }}>
        <thead>
          <tr>
            {['Event', 'Type', 'State', 'Attempts', 'Worker', 'Received', 'Last error', ''].map((h) => (
              <th key={h} style={th}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {events.map((e) => (
            <Fragment key={e.eventId}>
              <tr onClick={() => setOpen(open === e.eventId ? null : e.eventId)} style={{ cursor: 'pointer' }}>
                <td style={td}>{open === e.eventId ? '▾' : '▸'} {e.eventId}</td>
                <td style={td}>{e.type}</td>
                <td style={td}><Badge s={e.status} /></td>
                <td style={td}>{e.attempts}{e.attemptsInBudget !== e.attempts ? ` (${e.attemptsInBudget} in current budget)` : ''}</td>
                <td style={td}>{e.leaseOwner ?? '—'}</td>
                <td style={td}>{fmt(e.receivedAt)}</td>
                <td style={{ ...td, color: '#d33' }}>{e.lastError ?? ''}</td>
                <td style={td}>
                  {e.status === 'failed' && (
                    <button onClick={(ev) => { ev.stopPropagation(); retry(e.eventId); }}>Retry</button>
                  )}
                </td>
              </tr>
              {open === e.eventId && (
                <tr>
                  <td colSpan={8} style={{ ...td, background: '#fafafa' }}>
                    <b>Processing history</b>
                    <table style={{ fontSize: 13, marginTop: 4 }}>
                      <thead><tr>{['#', 'Worker', 'Started', 'Finished', 'Result', 'Error'].map((h) => <th key={h} style={th}>{h}</th>)}</tr></thead>
                      <tbody>
                        {e.history.map((a) => (
                          <tr key={a.attemptNumber}>
                            <td style={td}>{a.attemptNumber}</td><td style={td}>{a.workerId}</td>
                            <td style={td}>{fmt(a.startedAt)}</td><td style={td}>{fmt(a.finishedAt)}</td>
                            <td style={td}><Badge s={a.result} /></td><td style={td}>{a.error ?? ''}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <pre style={{ fontSize: 12 }}>{JSON.stringify(e.payload, null, 2)}</pre>
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
      {events.length === 0 && <p>No events yet. Run <code>./scripts/demo.sh</code>.</p>}
    </main>
  );
}
