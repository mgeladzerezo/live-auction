import { api, session, storeSession } from './api.js';
import { auctionView } from './auction-view.js';
import { startCountdowns } from './countdown.js';
import { h } from './util.js';
import { listView } from './list-view.js';
import { Live } from './live.js';

const live = new Live();
const view = document.getElementById('view');
const connection = document.getElementById('connection');
const instance = document.getElementById('instance');
const account = document.getElementById('account');
const dialog = document.getElementById('login');
const form = document.getElementById('login-form');
const loginError = document.getElementById('login-error');
let registering = false;
let teardown = () => {};

function renderConnection() {
  const labels = { open: 'Live', connecting: 'Connecting', reconnecting: 'Reconnecting' };
  connection.textContent = labels[live.state] ?? live.state;
  connection.className = `pill pill-${live.state === 'open' ? 'open' : 'connecting'}`;
  instance.textContent = live.instance && live.state === 'open' ? `via ${live.instance}` : '';
}

function renderAccount() {
  const current = session();
  account.replaceChildren(current
    ? h('span', { class: 'row' }, h('strong', {}, current.username),
        h('button', { class: 'btn btn-ghost', type: 'button', onclick: logout }, 'Log out'))
    : h('button', { class: 'btn btn-primary', type: 'button', onclick: openLogin }, 'Log in'));
}

function openLogin() {
  setRegistering(false);
  loginError.hidden = true;
  dialog.showModal();
}

function setRegistering(value) {
  registering = value;
  document.getElementById('login-title').textContent = value ? 'Create an account' : 'Log in';
  document.getElementById('login-submit').textContent = value ? 'Register' : 'Log in';
  document.getElementById('login-switch').textContent = value ? 'I have an account' : 'Create an account instead';
}

function logout() {
  storeSession(null);
  renderAccount();
  live.reconnect();
  route();
}

document.getElementById('login-switch').addEventListener('click', () => setRegistering(!registering));
document.getElementById('login-cancel').addEventListener('click', () => dialog.close());
form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const data = new FormData(form);
  try {
    const result = await (registering ? api.register : api.login)(data.get('username'), data.get('password'));
    storeSession({ token: result.token, username: result.username, userId: result.userId });
    dialog.close();
    form.reset();
    renderAccount();
    live.reconnect();
    route();
  } catch (error) {
    loginError.textContent = error.message;
    loginError.hidden = false;
  }
});

function route() {
  teardown();
  const match = location.hash.match(/^#\/auction\/(\d+)$/);
  teardown = match ? auctionView(view, live, Number(match[1]), openLogin) : listView(view, live);
}

function applyTheme(theme) {
  if (theme) document.documentElement.dataset.theme = theme;
  else delete document.documentElement.dataset.theme;
}

const savedTheme = (() => { try { return localStorage.getItem('auction.theme'); } catch { return null; } })();
applyTheme(savedTheme);
document.getElementById('theme').addEventListener('click', () => {
  const dark = document.documentElement.dataset.theme
    ? document.documentElement.dataset.theme === 'dark'
    : matchMedia('(prefers-color-scheme: dark)').matches;
  const next = dark ? 'light' : 'dark';
  applyTheme(next);
  try { localStorage.setItem('auction.theme', next); } catch { /* ignore */ }
});

live.addEventListener('connection', renderConnection);
window.addEventListener('hashchange', route);
renderAccount();
renderConnection();
startCountdowns(live);
live.connect();
route();
