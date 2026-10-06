/** REST client and the stored login. */

const TOKEN_KEY = 'auction.session';

export function session() {
  try {
    return JSON.parse(localStorage.getItem(TOKEN_KEY));
  } catch {
    return null;
  }
}

export function storeSession(value) {
  try {
    if (value) localStorage.setItem(TOKEN_KEY, JSON.stringify(value));
    else localStorage.removeItem(TOKEN_KEY);
  } catch {
    /* storage can be blocked; the session then lasts until reload */
  }
}

async function request(method, path, body) {
  const headers = { Accept: 'application/json' };
  const current = session();
  if (current?.token) headers.Authorization = `Bearer ${current.token}`;
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  const response = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    const error = new Error(data?.message ?? `Request failed (${response.status})`);
    error.status = response.status;
    error.code = data?.code;
    throw error;
  }
  return data;
}

export const api = {
  list: () => request('GET', '/api/auctions'),
  get: (id) => request('GET', `/api/auctions/${id}`),
  login: (username, password) => request('POST', '/api/auth/login', { username, password }),
  register: (username, password) => request('POST', '/api/auth/register', { username, password }),
  crowd: (id) => request('POST', `/api/demo/auctions/${id}/crowd`),
};
