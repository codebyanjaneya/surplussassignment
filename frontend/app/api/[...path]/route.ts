import { NextRequest } from 'next/server';

export const dynamic = 'force-dynamic';

const BACKEND = () => process.env.BACKEND_URL ?? 'http://localhost:3001';

async function proxy(req: NextRequest, { params }: { params: { path: string[] } }) {
  const url = `${BACKEND()}/${params.path.join('/')}${req.nextUrl.search}`;
  try {
    const res = await fetch(url, {
      method: req.method,
      headers: { 'content-type': 'application/json' },
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : await req.text(),
      cache: 'no-store',
    });
    return new Response(await res.text(), {
      status: res.status,
      headers: { 'content-type': res.headers.get('content-type') ?? 'application/json' },
    });
  } catch (e: any) {
    return Response.json({ message: `backend unreachable: ${e.message}` }, { status: 502 });
  }
}
export { proxy as GET, proxy as POST };
