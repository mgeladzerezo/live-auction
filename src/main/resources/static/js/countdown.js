import { duration } from './util.js';

/**
 * Shows the time left in one element carrying {@code data-ends} and {@code data-state}. The end
 * time is the server's; "now" is the local clock corrected by the offset the live connection
 * measured. Views call this on a freshly built element so it never shows up empty.
 */
export function renderCountdown(el, live) {
  if (!el.dataset.ends) return;
  const left = Date.parse(el.dataset.ends) - live.now();
  if (el.dataset.state && el.dataset.state !== 'OPEN') {
    el.textContent = el.dataset.state === 'SCHEDULED' ? 'Not started' : 'Ended';
    el.classList.remove('urgent');
  } else {
    el.textContent = left <= 0 ? 'Closing...' : duration(left);
    el.classList.toggle('urgent', left > 0 && left <= 10000);
  }
}

/** Keeps every countdown on the page current. */
export function startCountdowns(live) {
  setInterval(() => {
    for (const el of document.querySelectorAll('[data-ends]')) renderCountdown(el, live);
  }, 250);
}
