import { duration } from './util.js';

/**
 * Keeps every element carrying {@code data-ends} showing the time left. The end time is the
 * server's; "now" is the local clock corrected by the offset the live connection measured.
 */
export function startCountdowns(live) {
  const tick = () => {
    for (const el of document.querySelectorAll('[data-ends]')) {
      const left = Date.parse(el.dataset.ends) - live.now();
      if (el.dataset.state && el.dataset.state !== 'OPEN') {
        el.textContent = el.dataset.state === 'SCHEDULED' ? 'Not started' : 'Ended';
        el.classList.remove('urgent');
      } else {
        el.textContent = left <= 0 ? 'Closing...' : duration(left);
        el.classList.toggle('urgent', left > 0 && left <= 10000);
      }
    }
  };
  tick();
  setInterval(tick, 250);
}
