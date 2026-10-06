import { api } from './api.js';
import { h, money } from './util.js';

const STATUS_LABEL = { SCHEDULED: 'Scheduled', OPEN: 'Live', CLOSED: 'Closing up', SETTLED: 'Sold', UNSOLD: 'Unsold' };

function card(view) {
  const badgeClass = view.status === 'OPEN' ? 'badge badge-open' : view.status === 'SETTLED' ? 'badge badge-sold' : 'badge';
  return h('a', { class: 'card', href: `#/auction/${view.id}`, 'data-id': view.id },
    h('span', { class: badgeClass }, STATUS_LABEL[view.status] ?? view.status),
    h('span', { class: 'card-title' }, view.title),
    h('div', {},
      h('div', { class: 'price' }, money(view.currentPrice ?? view.startPrice)),
      h('div', { class: 'muted small' }, view.currentPrice == null ? 'Starting price' : `${view.bidCount} bids, led by ${view.leaderName}`)),
    h('div', { class: 'card-foot' },
      h('span', { class: 'countdown', 'data-ends': view.endsAt, 'data-state': view.status }),
      view.status === 'OPEN' ? h('span', { class: 'muted small' }, `min next ${money(view.minimumNextBid)}`) : null));
}

/** Auction list with live prices: every open auction is followed over the socket. */
export function listView(root, live) {
  const grid = h('div', { class: 'grid' });
  const status = h('div', { class: 'loading' }, 'Loading auctions...');
  root.replaceChildren(
    h('div', { class: 'page-head' }, h('h1', {}, 'Auctions'),
      h('span', { class: 'muted' }, 'Prices update as bids arrive.')),
    status, grid);

  const draw = (id) => {
    const entry = live.auctions.get(id);
    if (!entry) return;
    const next = card(entry.view);
    const existing = grid.querySelector(`[data-id="${id}"]`);
    if (existing) existing.replaceWith(next);
    else grid.append(next);
  };

  const refresh = async () => {
    try {
      const { auctions } = await api.list();
      for (const view of auctions) {
        const known = live.auctions.get(view.id);
        if (!known || known.view.seq <= view.seq) live.auctions.set(view.id, { view, bids: known?.bids ?? [] });
        if (view.status === 'OPEN' || view.status === 'SCHEDULED') live.follow(view.id);
        draw(view.id);
      }
      status.className = 'empty';
      status.textContent = 'No auctions yet.';
      status.hidden = auctions.length > 0;
    } catch (error) {
      status.hidden = false;
      status.className = 'banner';
      status.textContent = `Could not load auctions: ${error.message}`;
    }
  };

  const onChange = (event) => draw(event.detail.auctionId);
  live.addEventListener('auction', onChange);
  refresh();
  const timer = setInterval(refresh, 5000);

  return () => {
    live.removeEventListener('auction', onChange);
    clearInterval(timer);
  };
}
