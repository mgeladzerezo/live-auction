import { api, session } from './api.js';
import { clock, h, money, parseMoney } from './util.js';

const REJECTIONS = {
  TOO_LOW: 'Too low',
  AUCTION_CLOSED: 'The auction has closed',
  NOT_STARTED: 'The auction has not started',
  ALREADY_LEADING: 'You already lead this auction',
  SELLER_CANNOT_BID: 'Sellers cannot bid on their own lot',
  CONTENTION: 'Too many bids at once, try again',
  NOT_CONNECTED: 'Not connected, reconnecting',
  UNAUTHENTICATED: 'Log in to bid',
};

function bidRow(bid, fresh) {
  return h('li', { class: fresh ? 'fresh' : '', 'data-bid': bid.bidId },
    h('span', {}, h('strong', {}, bid.bidderName), ' ', h('span', { class: 'muted small' }, clock(bid.at))),
    h('span', { class: 'num' }, money(bid.amount)));
}

/** One auction: live price, countdown that jumps on extension, bid form, bid feed. */
export function auctionView(root, live, auctionId, onLogin) {
  const title = h('h1');
  const badge = h('span', { class: 'badge' });
  const price = h('div', { class: 'price price-xl' });
  const leader = h('div', { class: 'muted' });
  const countdown = h('span', { class: 'countdown stat-value' });
  const countdownWrap = h('span', { class: 'countdown-wrap' }, countdown);
  const extensions = h('div', { class: 'muted small' });
  const minimum = h('div', { class: 'muted small' });
  const feedback = h('div', { class: 'feedback feedback-wait', hidden: true, role: 'status' });
  const feed = h('ul', { class: 'feed' });
  const feedEmpty = h('div', { class: 'empty' }, 'No bids yet. Be the first.');
  const amount = h('input', { name: 'amount', inputmode: 'decimal', autocomplete: 'off', 'aria-label': 'Your bid in dollars' });
  const submit = h('button', { class: 'btn btn-primary', type: 'submit' }, 'Place bid');
  const crowd = h('button', { class: 'btn', type: 'button' }, 'Simulate crowd');
  const note = h('p', { class: 'muted small' });
  let userEditedAmount = false;
  let renderedBids = 0;

  const form = h('form', { class: 'bid-form' }, h('label', {}, 'Your bid (USD)', amount), submit);
  root.replaceChildren(
    h('p', {}, h('a', { href: '#/' }, 'All auctions')),
    h('div', { class: 'layout' },
      h('section', { class: 'panel' },
        h('div', { class: 'stack' }, badge, title, h('p', { class: 'muted', id: 'description' })),
        h('div', {}, price, leader),
        h('div', { class: 'stat-row' },
          h('div', { class: 'stat' }, h('span', { class: 'stat-label' }, 'Time left'), countdownWrap, extensions),
          h('div', { class: 'stat' }, h('span', { class: 'stat-label' }, 'Next bid at least'), h('span', { class: 'stat-value' }, minimum))),
        form, feedback,
        h('div', { class: 'row' }, crowd, note)),
      h('section', { class: 'panel' }, h('h2', {}, 'Bid feed'), feedEmpty, feed)));

  amount.addEventListener('input', () => { userEditedAmount = true; });

  const showFeedback = (kind, text) => {
    feedback.hidden = false;
    feedback.className = `feedback feedback-${kind}`;
    feedback.textContent = text;
  };

  const render = (event) => {
    const entry = live.auctions.get(auctionId);
    if (!entry) return;
    const view = entry.view;
    document.title = `${view.title} - Live Auction`;
    title.textContent = view.title;
    root.querySelector('#description').textContent = view.description;
    const open = view.status === 'OPEN';
    badge.textContent = open ? 'Live' : view.status === 'SETTLED' ? 'Sold' : view.status === 'UNSOLD' ? 'Unsold' : view.status;
    badge.className = open ? 'badge badge-open' : view.status === 'SETTLED' ? 'badge badge-sold' : 'badge';
    price.textContent = money(view.currentPrice ?? view.startPrice);
    if (!open && view.status !== 'SCHEDULED') {
      leader.textContent = view.winnerName ? `Won by ${view.winnerName}`
        : view.status === 'CLOSED' ? 'Settling...' : view.currentPrice == null ? 'No bids' : 'Reserve not met';
    } else {
      leader.textContent = view.currentPrice == null ? 'Starting price' : `Led by ${view.leaderName}`;
    }
    countdown.dataset.ends = view.endsAt;
    countdown.dataset.state = view.status;
    extensions.textContent = view.antiSnipeWindowSeconds > 0
      ? `Bids in the last ${view.antiSnipeWindowSeconds}s extend it (${view.extensionCount}/${view.maxExtensions} used)` : '';
    minimum.textContent = money(view.minimumNextBid);
    if (!userEditedAmount || event?.type === 'BID_ACCEPTED') {
      amount.value = (view.minimumNextBid / 100).toFixed(2);
      userEditedAmount = false;
    }
    const bidding = open && !!session();
    amount.disabled = !bidding;
    submit.disabled = !bidding;
    note.textContent = session() ? '' : 'Log in to bid.';
    crowd.hidden = !open;

    if (event?.type === 'TIME_EXTENDED') {
      const added = Math.round((Date.parse(event.endsAt) - Date.parse(event.previousEndsAt)) / 1000);
      countdownWrap.classList.remove('flash');
      void countdownWrap.offsetWidth; // restart the animation
      countdownWrap.classList.add('flash');
      feed.prepend(h('li', { class: 'note fresh' }, h('span', {}, `Sniping protection: +${added}s`), h('span', { class: 'small' }, clock(event.at))));
    }
    if (event?.type === 'AUCTION_CLOSED') {
      showFeedback('wait', event.winnerName ? `Closed. Won by ${event.winnerName} for ${money(event.finalPrice)}.` : 'Closed without a winner.');
    }
    drawBids(entry, event?.type === 'BID_ACCEPTED');
  };

  const drawBids = (entry, announce) => {
    feedEmpty.hidden = entry.bids.length > 0;
    if (!announce || renderedBids === 0 || !feed.querySelector('[data-bid]')) {
      feed.replaceChildren(...[...entry.bids].reverse().map((bid) => bidRow(bid, false)));
    } else {
      feed.prepend(bidRow(entry.bids.at(-1), true));
    }
    renderedBids = entry.bids.length;
  };

  form.addEventListener('submit', async (submitEvent) => {
    submitEvent.preventDefault();
    const cents = parseMoney(amount.value);
    if (!Number.isInteger(cents) || cents <= 0) {
      showFeedback('no', 'Enter an amount, for example 125.00');
      return;
    }
    submit.disabled = true;
    showFeedback('wait', 'Sending...');
    const answer = await live.placeBid(auctionId, cents);
    submit.disabled = !session() || live.auctions.get(auctionId)?.view.status !== 'OPEN';
    if (answer.outcome === 'ACCEPTED') {
      showFeedback('ok', `Accepted: ${money(answer.amount)} (${answer.latencyMs} ms)${answer.extendedTo ? ', time extended' : ''}`);
    } else if (answer.outcome === 'REJECTED') {
      const detail = answer.minimumBid != null && answer.reason === 'TOO_LOW' ? `, minimum is ${money(answer.minimumBid)}` : '';
      showFeedback('no', `Rejected: ${REJECTIONS[answer.reason] ?? answer.reason}${detail}`);
    } else {
      showFeedback('no', REJECTIONS[answer.reason] ?? answer.message ?? 'Could not place the bid');
    }
  });

  crowd.addEventListener('click', async () => {
    if (!session()) { onLogin(); return; }
    try {
      await api.crowd(auctionId);
      showFeedback('wait', 'Five bots are bidding for the next 25 seconds.');
    } catch (error) {
      showFeedback('no', error.message);
    }
  });

  const onChange = (event) => {
    if (event.detail.auctionId === auctionId) render(event.detail.event);
  };
  live.addEventListener('auction', onChange);
  live.follow(auctionId);
  api.get(auctionId).then(({ auction, recentBids }) => {
    if (!live.auctions.has(auctionId)) {
      live.auctions.set(auctionId, { view: auction, bids: [...recentBids].reverse() });
      render(null);
    }
  }).catch((error) => {
    root.replaceChildren(h('div', { class: 'banner' }, error.status === 404 ? 'That auction does not exist.' : `Could not load: ${error.message}`),
      h('a', { href: '#/' }, 'Back to the list'));
  });
  render(null);

  return () => {
    live.removeEventListener('auction', onChange);
    live.unfollow(auctionId);
    document.title = 'Live Auction';
  };
}
