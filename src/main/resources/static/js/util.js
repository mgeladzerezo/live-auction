/** Small DOM and formatting helpers. Text is always set via textContent, never as HTML. */

export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [name, value] of Object.entries(attrs)) {
    if (value === false || value == null) continue;
    if (name === 'class') el.className = value;
    else if (name.startsWith('on')) el.addEventListener(name.slice(2), value);
    else el.setAttribute(name, value === true ? '' : value);
  }
  for (const child of children.flat()) {
    if (child == null || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return el;
}

const currency = new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD' });

/** Amounts travel as integer minor units (cents). */
export function money(minorUnits) {
  return minorUnits == null ? '-' : currency.format(minorUnits / 100);
}

export function parseMoney(text) {
  const value = Number(String(text).replace(/[^0-9.]/g, ''));
  return Number.isFinite(value) ? Math.round(value * 100) : NaN;
}

export function clock(timeIso) {
  return new Date(timeIso).toLocaleTimeString();
}

/** mm:ss, or h:mm:ss for long durations. */
export function duration(ms) {
  const total = Math.max(0, Math.ceil(ms / 1000));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  const pad = (n) => String(n).padStart(2, '0');
  return hours > 0 ? `${hours}:${pad(minutes)}:${pad(seconds)}` : `${pad(minutes)}:${pad(seconds)}`;
}

export function randomId(prefix) {
  const bytes = crypto.getRandomValues(new Uint8Array(9));
  return prefix + Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}
