import { session } from './api.js';
import { randomId } from './util.js';

/**
 * The live connection and the client-side copy of every auction it follows.
 *
 * <p>Ordering contract with the server: after a SNAPSHOT at sequence n the next event for that
 * auction is n+1. An event that skips ahead means a message was lost, so the client re-subscribes
 * with its last sequence and the server replays the gap (or sends a fresh snapshot). On
 * reconnect every subscription is resumed the same way, and bids that were sent but not yet
 * answered are re-sent with their original client id, which the server treats idempotently.
 */
export class Live extends EventTarget {
  constructor() {
    super();
    this.state = 'connecting';
    this.instance = '';
    this.auctions = new Map(); // id -> { view, bids: [] }
    this.lastSeq = new Map(); // id -> last applied sequence
    this.pending = new Map(); // clientBidId -> { auctionId, amount, resolve, timer, sentAt }
    this.offset = 0; // serverTime - localTime, in ms
    this.bestRtt = Infinity;
    this.attempt = 0;
    this.socket = null;
  }

  /** Estimated server time in epoch milliseconds. */
  now() {
    return Date.now() + this.offset;
  }

  connect() {
    clearTimeout(this.retryTimer);
    this.setState(this.attempt === 0 ? 'connecting' : 'reconnecting');
    const user = session();
    const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
    const query = user?.token ? `?token=${encodeURIComponent(user.token)}` : '';
    const socket = new WebSocket(`${scheme}://${location.host}/ws${query}`);
    this.socket = socket;
    socket.onmessage = (event) => this.onMessage(JSON.parse(event.data));
    socket.onclose = () => {
      if (this.socket !== socket) return;
      this.socket = null;
      clearInterval(this.pingTimer);
      this.setState('reconnecting');
      const delay = Math.min(8000, 500 * 2 ** this.attempt++) * (0.7 + Math.random() * 0.6);
      this.retryTimer = setTimeout(() => this.connect(), delay);
    };
  }

  /** Drops the socket so the next one is opened with the current login. */
  reconnect() {
    this.attempt = 0;
    if (this.socket) this.socket.close();
    else this.connect();
  }

  setState(state) {
    this.state = state;
    this.dispatchEvent(new CustomEvent('connection'));
  }

  send(message) {
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify(message));
      return true;
    }
    return false;
  }

  follow(auctionId) {
    if (!this.lastSeq.has(auctionId)) {
      this.lastSeq.set(auctionId, null);
      this.send({ type: 'SUBSCRIBE', auctionId });
    }
  }

  unfollow(auctionId) {
    if (this.lastSeq.delete(auctionId)) {
      this.send({ type: 'UNSUBSCRIBE', auctionId });
    }
  }

  /** Sends a bid and resolves with its single definitive answer. */
  placeBid(auctionId, amount) {
    if (this.state !== 'open') {
      return Promise.resolve({ outcome: 'ERROR', reason: 'NOT_CONNECTED', message: 'Not connected. Reconnecting...' });
    }
    const clientBidId = randomId('web-');
    return new Promise((resolve) => {
      const entry = { auctionId, amount, resolve, sentAt: performance.now() };
      entry.timer = setTimeout(() => this.settle(clientBidId, {
        outcome: 'ERROR', reason: 'NO_ANSWER', message: 'No answer yet. Check the bid feed before trying again.',
      }), 15000);
      this.pending.set(clientBidId, entry);
      this.sendBid(clientBidId, entry);
    });
  }

  sendBid(clientBidId, entry) {
    this.send({ type: 'BID', auctionId: entry.auctionId, amount: entry.amount, clientBidId });
  }

  settle(clientBidId, answer) {
    const entry = this.pending.get(clientBidId);
    if (!entry) return;
    clearTimeout(entry.timer);
    this.pending.delete(clientBidId);
    entry.resolve({ ...answer, latencyMs: Math.round(performance.now() - entry.sentAt) });
  }

  onMessage(message) {
    switch (message.type) {
      case 'WELCOME': return this.onWelcome(message);
      case 'SNAPSHOT': return this.onSnapshot(message);
      case 'REPLAY': return this.onReplay(message);
      case 'PONG': return this.onPong(message);
      case 'BID_RESULT': return this.settle(message.result.clientBidId, message.result);
      case 'ERROR': return this.onError(message);
      case 'UNSUBSCRIBED': return undefined;
      default: return this.onEvent(message);
    }
  }

  onWelcome(message) {
    this.attempt = 0;
    this.instance = message.instance;
    this.offset = Date.parse(message.serverTime) - Date.now();
    this.setState('open');
    for (const [auctionId, seq] of this.lastSeq) {
      this.send(seq == null ? { type: 'SUBSCRIBE', auctionId } : { type: 'SUBSCRIBE', auctionId, lastSeq: seq });
    }
    for (const [clientBidId, entry] of this.pending) this.sendBid(clientBidId, entry);
    this.ping();
    this.pingTimer = setInterval(() => this.ping(), 20000);
  }

  ping() {
    this.send({ type: 'PING', clientTime: Date.now() });
  }

  /** Keeps the offset estimate from the round trip with the least delay: half of it is the one-way time. */
  onPong(message) {
    const rtt = Date.now() - message.clientTime;
    if (rtt <= this.bestRtt || this.bestRtt === Infinity) {
      this.bestRtt = rtt;
      this.offset = Date.parse(message.serverTime) + rtt / 2 - Date.now();
    }
  }

  onSnapshot(message) {
    this.lastSeq.set(message.auctionId, message.seq);
    this.auctions.set(message.auctionId, { view: message.auction, bids: [...message.recentBids].reverse() });
    this.changed(message.auctionId, null);
  }

  onReplay(message) {
    for (const event of message.events) this.onEvent(event);
  }

  onError(message) {
    if (!message.clientBidId) return;
    if (message.retryable) {
      const entry = this.pending.get(message.clientBidId);
      if (entry) setTimeout(() => this.sendBid(message.clientBidId, entry), 400);
      return;
    }
    this.settle(message.clientBidId, { outcome: 'ERROR', reason: message.code, message: message.message });
  }

  onEvent(event) {
    const id = event.auctionId;
    const last = this.lastSeq.get(id);
    if (last == null || event.seq <= last) return; // not subscribed yet, or a duplicate
    if (event.seq !== last + 1) {
      this.send({ type: 'SUBSCRIBE', auctionId: id, lastSeq: last }); // gap: ask for the missing events
      this.dispatchEvent(new CustomEvent('gap', { detail: { auctionId: id } }));
      return;
    }
    this.lastSeq.set(id, event.seq);
    const entry = this.auctions.get(id);
    if (!entry) return;
    this.apply(entry, event);
    this.changed(id, event);
  }

  apply(entry, event) {
    const view = entry.view;
    view.seq = event.seq;
    switch (event.type) {
      case 'AUCTION_OPENED':
        view.status = 'OPEN';
        view.endsAt = event.endsAt;
        break;
      case 'BID_ACCEPTED':
        view.currentPrice = event.price;
        view.leaderId = event.leaderId;
        view.leaderName = event.leaderName;
        view.bidCount = event.bidCount;
        view.minimumNextBid = event.minimumNextBid;
        view.endsAt = event.endsAt;
        entry.bids.push({ bidId: event.bidId, seq: event.seq, bidderId: event.leaderId,
          bidderName: event.leaderName, amount: event.price, at: event.at });
        if (entry.bids.length > 100) entry.bids.shift();
        break;
      case 'TIME_EXTENDED':
        view.endsAt = event.endsAt;
        view.extensionCount = event.extensionCount;
        break;
      case 'AUCTION_CLOSED':
        view.status = 'CLOSED';
        view.endsAt = event.endsAt;
        view.reserveMet = event.reserveMet;
        view.winnerName = event.winnerName;
        break;
      case 'AUCTION_SETTLED':
        view.status = event.status;
        view.winnerName = event.winnerName;
        break;
      default:
    }
  }

  changed(auctionId, event) {
    this.dispatchEvent(new CustomEvent('auction', { detail: { auctionId, event } }));
  }

  /** Adds or refreshes an auction known only from REST, then follows it. */
  seed(view) {
    if (!this.auctions.has(view.id)) this.auctions.set(view.id, { view, bids: [] });
    this.follow(view.id);
  }
}
