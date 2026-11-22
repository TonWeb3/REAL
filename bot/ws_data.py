import asyncio
import collections  # PATCH: longshot maker
import time
from typing import Optional, Callable, Dict, List
import aiohttp
from .config import settings
from .net_utils import get_proxy_url_for

# ── Low-Level Performance: Fast JSON Acceleration (orjson / std json fallback) ──
try:
    import orjson
    def fast_loads(s):
        if isinstance(s, (bytes, bytearray)):
            return orjson.loads(s)
        return orjson.loads(s.encode("utf-8"))
    def fast_dumps(obj):
        return orjson.dumps(obj).decode("utf-8")
except ImportError:
    import json
    def fast_loads(s):
        return json.loads(s)
    def fast_dumps(obj):
        return json.dumps(obj)


# ── Inputs (TOGGLE: inputs) ───────────────────────────────────────────────────
# config.json "inputs" (bot/config.py has the eight names; the settings page has a card "Inputs"). Each part is idle
# while its switch is off: the feeds then connect, read and keep their books as before.
#   delay_log      FEED DELAY lines: how late each feed's messages arrive, by the sender's own time stamps
#   binance_both   both of Binance's addresses at once for each stream; the first copy of a message is used
#   binance_first  the address a Binance stream tries first while one address is used
#   book_both      a second connection to Polymarket's order-book feed; the book that is further on is the one read
#   book_light     a price change is put into the sorted book where it belongs, instead of the whole book being rebuilt
#   book_thread    Polymarket's order-book feed in a thread of its own
#   top_of_book    Polymarket also sends a best_bid_ask message when the top of a book changes
import bisect
import random
import threading

IN_HOSTS = ("stream.binance.com", "data-stream.binance.vision")   # Binance's two addresses for the same streams


def in_cfg() -> Dict:
    """The "inputs" block as it is in use right now (a save from the settings page replaces it)."""
    return getattr(settings, "INPUTS", None) or {}


def in_on(name: str) -> bool:
    """One of the inputs switches."""
    return bool(in_cfg().get(name))


def in_host(url: str) -> str:
    """stream.binance.com, from wss://stream.binance.com:9443/ws/btcusdt@trade"""
    return str(url).split("//", 1)[-1].split("/", 1)[0].split(":", 1)[0]


def in_first(endpoints: List[str]) -> List[str]:
    """inputs.binance_first: the list with that address first (the other one stays as the fallback). As it comes ("")
    the list is handed back as it is."""
    want = str(in_cfg().get("binance_first") or "")
    if want not in IN_HOSTS:
        return endpoints
    return sorted(endpoints, key=lambda u: 0 if in_host(u) == want else 1)


class InSpan:
    """What one feed's delays added up to since the last summary line: how many messages, the fastest and the slowest,
    and a sample of them to read the typical one from (the whole run is never kept), the same per kind of message."""
    KEEP, KEEP_KIND = 2000, 400

    def __init__(self):
        self.n = 0
        self.lo = self.hi = None
        self.some: List[float] = []
        self.kinds: Dict[str, list] = {}       # kind of message -> [how many, a sample of their delays]

    def add(self, ms: float, kind: str = "") -> None:
        self.n += 1
        if self.lo is None or ms < self.lo:
            self.lo = ms
        if self.hi is None or ms > self.hi:
            self.hi = ms
        if len(self.some) < self.KEEP:
            self.some.append(ms)
        else:
            j = int(random.random() * self.n)
            if j < self.KEEP:
                self.some[j] = ms
        if kind:
            k = self.kinds.get(kind)
            if k is None:
                k = self.kinds[kind] = [0, []]
            k[0] += 1
            if len(k[1]) < self.KEEP_KIND:
                k[1].append(ms)
            else:
                j = int(random.random() * k[0])
                if j < self.KEEP_KIND:
                    k[1][j] = ms


class InMeter:
    """How late one feed's messages arrive. A message's delay is the time it got here (this computer's clock) minus
    the time its sender stamped on it, in ms. Kept: the last message, tenth-of-a-second buckets for the last two
    minutes ([which tenth, how many messages, the sum of their delays, the slowest]) and the sums for the summary."""

    def __init__(self):
        self.last = None                       # (when it arrived, its delay in ms)
        self.buckets = collections.deque(maxlen=1300)
        self.span = InSpan()

    def note(self, arrived: float, ms: float, kind: str = "") -> None:
        self.last = (arrived, ms)
        k = int(arrived * 10.0)
        b = self.buckets[-1] if self.buckets else None
        if b is None or b[0] != k:
            b = [k, 0, 0.0, ms]
            self.buckets.append(b)
        b[1] += 1
        b[2] += ms
        if ms > b[3]:
            b[3] = ms
        self.span.add(ms, kind)

    def mark(self):
        """Where the meter stands right now: (the last message, the newest bucket as it is). Taken at a moment, it
        lets around() tell what came before that moment from what came after it."""
        b = self.buckets[-1] if self.buckets else None
        return self.last, (tuple(b) if b is not None else None)

    def around(self, mark, t0: float, before_s: float = 5.0, after_s: float = 1.0, typical_s: float = 60.0) -> Dict:
        """For the moment t0, whose mark() was taken then: the last message before it (its delay, and how long before
        t0 it arrived), the slowest message of the before_s seconds before it and of the after_s seconds after it, and
        the typical delay of the typical_s seconds before it (the middle one of the tenth-of-a-second averages).
        None for whatever had no message."""
        last, at = mark if mark else (None, None)
        k0 = int(t0 * 10.0)
        rows = [tuple(b) for b in list(self.buckets)]
        before = [b[3] for b in rows if int((t0 - before_s) * 10.0) <= b[0] < k0 and b[1] > 0]
        after = [b[3] for b in rows if k0 < b[0] <= int((t0 + after_s) * 10.0) and b[1] > 0]
        same = at is not None and at[0] == k0 and at[1] > 0          # messages of t0's own tenth that were there at t0
        if same:
            before.append(at[3])
        now_b = next((b for b in rows if b[0] == k0), None)
        if now_b is not None and now_b[1] > (at[1] if same else 0) and (not same or now_b[3] > at[3]):
            after.append(now_b[3])                                   # ... and a slower one that came in it afterwards
        avgs = sorted(b[2] / b[1] for b in rows if b[1] > 0 and int((t0 - typical_s) * 10.0) <= b[0] <= k0)
        return {"last": last[1] if last else None, "last_age": (t0 - last[0]) if last else None,
                "before": max(before) if before else None, "after": max(after) if after else None,
                "typical": avgs[len(avgs) // 2] if avgs else None}

    def take_span(self) -> InSpan:
        """The sums since this was last asked; they start again from nothing."""
        old, self.span = self.span, InSpan()
        return old


class InRace:
    """With two connections to one feed: which one had each message first, and by how much, for the summary line.
    Only messages that came down both are counted."""
    KEEP, KEEP_LEADS = 8192, 2000

    def __init__(self):
        self.first: Dict = {}                  # message -> (which connection had it first, when)
        self.order = collections.deque()
        self.wins: Dict[str, int] = {}
        self.leads: List[float] = []           # ms, a sample
        self.n = 0

    def see(self, key, who: str, now: float) -> None:
        f = self.first.get(key)
        if f is None:
            self.first[key] = (who, now)
            self.order.append(key)
            if len(self.order) > self.KEEP:
                self.first.pop(self.order.popleft(), None)
        elif f[0] != who:
            self.first.pop(key, None)
            self.wins[f[0]] = self.wins.get(f[0], 0) + 1
            self.n += 1
            ms = (now - f[1]) * 1000.0
            if len(self.leads) < self.KEEP_LEADS:
                self.leads.append(ms)
            else:
                j = int(random.random() * self.n)
                if j < self.KEEP_LEADS:
                    self.leads[j] = ms

    def take(self):
        """(how often each connection was first, a sample of the leads in ms, how many messages) since last asked."""
        out = (self.wins, self.leads, self.n)
        self.wins, self.leads, self.n = {}, [], 0
        return out


class _InBinance:
    """TOGGLE: inputs — what the inputs switches add to BinanceTradeStream, which is built on this class."""

    def _in_setup(self) -> None:
        self._in_last = {"book": [None, 0.0], "trade": [None, 0.0]}   # both addresses: the newest message used per stream: [its number, when]
        self._in_meter = InMeter()       # delay_log: the trades the bot used
        self._in_meters: Dict[str, InMeter] = {}   # ... and every copy of them, by the address it came from
        self._in_race = InRace()         # ... and which address had a message first
        self._in_mode = ""               # what is running: "one:<address tried first>" or "both"

    def _in_urls(self):
        """The two streams and, for each, Binance's two addresses (the same four as in the two workers below)."""
        s = self.symbol
        return (("book", [f"wss://stream.binance.com:9443/ws/{s}@bookTicker", f"wss://data-stream.binance.vision/ws/{s}@bookTicker"]),
                ("trade", [f"wss://data-stream.binance.vision/ws/{s}@trade", f"wss://stream.binance.com:9443/ws/{s}@trade"]))

    async def start(self):
        """What the bot starts. As it comes it runs _in_plain(), which is start() as it was: one connection per stream,
        on the second address only after the first failed. inputs.binance_both: one connection per address for each
        stream instead (_in_pinned). A change of that switch, or of inputs.binance_first, is seen within half a second:
        the connections are closed and opened again the new way."""
        tasks, mode = [], None
        try:
            while not self.closed:
                try:
                    want = "both" if in_on("binance_both") else "one:" + str(in_cfg().get("binance_first") or "")
                    if want != mode or any(t.done() for t in tasks):     # (a connection's task that ended on its own is started again)
                        for t in tasks:
                            t.cancel()
                        self._in_last = {"book": [None, 0.0], "trade": [None, 0.0]}
                        if want == "both":
                            tasks = [asyncio.ensure_future(self._in_pinned(kind, url)) for kind, urls in self._in_urls() for url in urls]
                        else:
                            tasks = [asyncio.ensure_future(self._in_plain())]
                        mode = self._in_mode = want
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"Binance Spot WS: the connections could not be arranged as set ({type(e).__name__}: {e}); trying again")
                    mode = None
                await asyncio.sleep(0.5)
        finally:
            for t in tasks:
                t.cancel()

    async def _in_pinned(self, kind: str, url: str):
        """inputs.binance_both: one connection that stays on one address (kind: "book" for the bid/ask stream, "trade"
        for the trades). If the address can't be reached it is tried again every 2 seconds; the other address's
        connection carries on meanwhile."""
        while not self.closed:
            opened, failed = None, False
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=20.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=15.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Spot {'Book ' if kind == 'book' else ''}WS ({url}), one of both addresses")
                        opened = time.monotonic()
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._in_take(kind, url, fast_loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except asyncio.CancelledError:
                raise
            except Exception:
                failed = True
            # closed by Binance after a while (it does so once a day): opened again at once, as before. A failure, or
            # a connection that lasted under a second: again in 2 seconds.
            if not self.closed and (failed or opened is None or time.monotonic() - opened < 1.0):
                await asyncio.sleep(2)

    def _in_take(self, kind: str, url: str, d) -> None:
        """Both addresses: one message from one of them. The first copy of a message is used; a later copy of it, and
        anything older than what was used, is dropped. Binance numbers its bid/ask messages ("u") and its trades ("t")."""
        if not isinstance(d, dict):
            return
        num = d.get("u") if kind == "book" else d.get("t")
        now = time.time()
        last = self._in_last[kind]
        try:
            fresh = (num is None or last[0] is None or num > last[0]
                     or (now - last[1] > 5.0 and num != last[0]))    # nothing newer for 5 s: the numbers began again
        except TypeError:
            fresh = True
        log = in_on("delay_log")
        if log and num is not None:
            self._in_race.see((kind, num), in_host(url), now)
        if log and kind == "trade":
            self._in_stamp(d, url, used=fresh)
        if not fresh:
            return
        if num is not None:
            last[0], last[1] = num, now
        try:
            if kind == "book":
                if "b" in d and "a" in d:
                    bid, ask = float(d["b"]), float(d["a"])
                    if bid > 0 and ask > 0:
                        self._process_trade((bid + ask) / 2.0, source="binance_spot_book")
            elif "p" in d:
                p = float(d["p"])
                if p > 0:
                    self._process_trade(p, source="binance_spot_trade")
        except (TypeError, ValueError):
            pass

    def _in_stamp(self, d, url: str, used: bool = True) -> None:
        """delay_log: one trade message's delay: the time it got here minus Binance's own time for the trade ("T",
        else the message's "E"). Kept by the address it came from, and, for a message the bot used, for the stream."""
        try:
            stamp = float(d.get("T") or d.get("E") or 0.0)
            if stamp <= 0:
                return
            now = time.time()
            ms = now * 1000.0 - stamp
            host = in_host(url)
            m = self._in_meters.get(host)
            if m is None:
                m = self._in_meters[host] = InMeter()
            m.note(now, ms, "trade")
            if used:
                self._in_meter.note(now, ms, "trade")
        except (TypeError, ValueError, AttributeError):
            pass


class _InBook:
    """TOGGLE: inputs — what the inputs switches add to PolymarketClobMarketStream, which is built on this class."""

    def _in_setup(self) -> None:
        self._in_lock = threading.RLock()    # one message, or one read of a book, at a time
        self._in_dirty: set = set()          # book_light: tokens whose sorted sides, as handed out, are behind their levels ...
        self._in_work: Dict[str, list] = {}  # ... and, per token, the sorted sides kept up change by change (see _in_light)
        self._in_twin = None                 # book_both: the second connection (an object like this one) ...
        self._in_of = None                   # ... and, on that second one, the first
        self._in_pair = False                # two connections are running
        self._in_at: Dict[str, tuple] = {}   # then, per token: (Polymarket's stamp on the newest message applied, how many carried it)
        self._in_threaded = False            # book_thread: the feed runs in its own thread ...
        self._in_loop = None                 # ... on this loop ...
        self._in_thread = None               # ... which this thread runs
        self._in_thread_retry = 0.0          # a thread that could not be started is not tried again before this (time.monotonic())
        self._in_home = None                 # the bot's own loop
        self._in_cb = None                   # the bot's "a book changed" call, kept here while the feed is in its thread
        self._in_hop_waiting = False
        self._in_jobs: Dict = {}             # the connections that are running: "one" and "two"
        self._in_mode = None                 # what is running: (in a thread?, two connections?, top-of-book messages?)
        self._in_meter = InMeter()           # delay_log: how late this connection's messages arrive
        self._in_tops: Dict[str, int] = {}   # ... how often the top of a book changed, by the kind of message that brought it
        self._in_race = InRace()             # ... and which connection had a message first

    # ── what the bot starts ──
    async def start(self):
        """What the bot starts. It keeps _in_connect() running, which is start() as it was, and with it what the
        switches ask for: the feed in a thread of its own (book_thread), a second connection (book_both), and the
        connections opened again when top_of_book changes (the subscription is sent when a connection opens). A change
        of a switch is seen within half a second."""
        self._in_home = asyncio.get_running_loop()
        self._in_jobs, self._in_mode = {}, None
        try:
            while not self.closed:
                try:
                    await self._in_arrange()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"Polymarket CLOB Market WS: the feed could not be arranged as set ({type(e).__name__}: {e}); trying again")
                await asyncio.sleep(0.5)
        finally:
            for j in list(self._in_jobs.values()):
                j.cancel()
            self._in_jobs.clear()
            self._in_drop_twin()
            self._in_leave_thread()

    async def _in_arrange(self) -> None:
        """One look at the switches: whatever differs from what is running is closed and opened again the new way.
        self._in_mode is what is running: (in a thread?, two connections?, top-of-book messages?)."""
        jobs, mode = self._in_jobs, self._in_mode
        want = (in_on("book_thread") and time.monotonic() >= self._in_thread_retry, in_on("book_both"), in_on("top_of_book"))
        if self._in_thread is not None and not self._in_thread.is_alive():      # the feed's thread ended on its own: everything from the start
            jobs.clear()
            self._in_drop_twin()
            self._in_leave_thread()
            mode = None
        if mode is None or want[0] != mode[0]:
            self._in_mode = None
            await self._in_end(jobs, "two")
            await self._in_end(jobs, "one")
            self._in_drop_twin()
            try:
                await self._in_set_thread(want[0])
            except Exception as e:               # no thread to be had: the feed stays in the bot's own loop, and the thread is tried again in a minute
                print(f"Polymarket CLOB Market WS: its own thread could not be started ({type(e).__name__}: {e}); the feed stays in the bot's loop")
                self._in_thread_retry = time.monotonic() + 60.0
                self._in_leave_thread()
            self._in_begin(jobs, "one", self)
            mode = self._in_mode = (self._in_thread is not None, False, want[2])
        if want[1] != mode[1]:
            if want[1]:
                self._in_begin(jobs, "two", self._in_make_twin())
            else:
                await self._in_end(jobs, "two")
                self._in_drop_twin()
            mode = self._in_mode = (mode[0], want[1], mode[2])
        if want[2] != mode[2]:
            if "two" in jobs:                    # the second one first, so that one book stays live throughout
                since, was = time.time(), self._in_twin._ws
                await self._in_end(jobs, "two")
                self._in_begin(jobs, "two", self._in_twin)
                for _ in range(60):                                  # (up to 3 seconds) until its new connection is open and has had a message
                    if self._in_twin._ws is not was and self._in_twin.last_ts > since:
                        break
                    await asyncio.sleep(0.05)
            await self._in_end(jobs, "one")
            self._in_begin(jobs, "one", self)
            mode = self._in_mode = (mode[0], mode[1], want[2])
        for name, stream in (("one", self), ("two", self._in_twin)):
            if stream is not None and not stream.closed and (name not in jobs or jobs[name].done()):
                self._in_begin(jobs, name, stream)           # a connection that is not running (its task ended on its own) is started again

    def _in_leave_thread(self) -> None:
        """book_thread: lets go of the feed's thread (it is told to stop; nothing waits for it)."""
        loop = self._in_loop
        self._in_threaded, self._in_loop, self._in_thread = False, None, None
        if self.on_update == self._in_hop:
            self.on_update, self._in_cb = self._in_cb, None
        if loop is not None:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass

    def _in_begin(self, jobs: Dict, name: str, stream) -> None:
        """Opens one connection: on the feed thread's loop while book_thread is on, else on the bot's own."""
        coro = stream._in_connect()
        try:
            if self._in_loop is not None:
                jobs[name] = asyncio.run_coroutine_threadsafe(coro, self._in_loop)
            else:
                jobs[name] = asyncio.ensure_future(coro)
        except BaseException:
            coro.close()
            raise

    async def _in_end(self, jobs: Dict, name: str) -> None:
        """Closes one connection. In the bot's own loop this waits (up to 2 seconds) until it is closed; with the feed
        in its own thread the connection is told to close there and stops taking messages at once."""
        j = jobs.pop(name, None)
        if j is None:
            return
        j.cancel()
        for _ in range(40):
            if j.done():
                break
            await asyncio.sleep(0.05)

    async def _in_set_thread(self, on: bool) -> None:
        """book_thread: starts the feed's thread and loop, or ends them. Its connections have been closed, or told to
        close, before this is called."""
        if on and self._in_thread is None:
            loop = asyncio.new_event_loop()
            try:
                th = threading.Thread(target=self._in_thread_run, args=(loop,), name="polymarket-book-feed", daemon=True)
                th.start()
            except BaseException:
                loop.close()
                raise
            self._in_loop, self._in_thread = loop, th
            self._in_cb, self.on_update = self.on_update, self._in_hop    # "a book changed" is handed over to the bot's loop from now on
            self._in_threaded = True
        elif not on and self._in_thread is not None:
            th, loop = self._in_thread, self._in_loop
            try:
                loop.call_soon_threadsafe(loop.stop)                 # the thread closes what is left on its loop and ends
            except RuntimeError:
                pass
            for _ in range(60):                                      # (up to 3 seconds)
                if not th.is_alive():
                    break
                await asyncio.sleep(0.05)
            self._in_leave_thread()                                  # only now do reads, and the bot's "a book changed" call, go back to the bot's loop alone

    def _in_thread_run(self, loop) -> None:
        """book_thread: the feed's thread. It runs the feed's loop until that is stopped, then closes what is left."""
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            try:
                left = [t for t in asyncio.all_tasks(loop) if not t.done()]
                for t in left:
                    t.cancel()
                if left:
                    loop.run_until_complete(asyncio.gather(*left, return_exceptions=True))
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

    def _in_hop(self, data=None) -> None:
        """book_thread: the feed's "a book changed" call, made in the feed's thread. It is handed to the bot's own
        loop, once for a burst of messages (the bot's call looks at the books itself; it takes no message)."""
        if self._in_hop_waiting:
            return
        self._in_hop_waiting = True
        try:
            self._in_home.call_soon_threadsafe(self._in_hop_run)
        except (RuntimeError, AttributeError):                      # the bot's loop is closed: it is stopping
            self._in_hop_waiting = False

    def _in_hop_run(self) -> None:
        self._in_hop_waiting = False
        cb = self._in_cb
        if cb:
            try:
                cb(None)
            except Exception:
                pass

    def _in_spawn(self, coro) -> None:
        """Starts a small job of this connection (sending a subscription) on the loop the connection lives on."""
        loop = self._in_loop
        if loop is None:
            asyncio.create_task(coro)
            return
        try:
            asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError:                                         # the feed's loop is closed (its thread ended): the connection
            coro.close()                                             # that is opened next subscribes with the tokens as they are then

    # ── the second connection ──
    def _in_make_twin(self):
        """book_both: the second connection: an object like this one with a book of its own, kept by the same code
        from its own copy of the feed. It shares this one's lock, loop and tokens."""
        tw = type(self)(ws_url=self.ws_url, on_update=self._in_from_twin)
        tw._in_of, tw._in_lock = self, self._in_lock
        tw._in_loop, tw._in_threaded = self._in_loop, self._in_threaded
        tw.asset_ids = list(self.asset_ids)
        tw._in_pair = self._in_pair = True
        self._in_at = {}
        self._in_twin = tw
        return tw

    def _in_drop_twin(self) -> None:
        tw, self._in_twin = self._in_twin, None
        self._in_pair = False
        if tw is not None:
            tw.closed = True

    def _in_from_twin(self, data=None) -> None:
        """The second connection's "a book changed" call: the same call as the first one's."""
        cb = self.on_update
        if cb:
            cb(data)

    def _in_note_at(self, data) -> None:
        """Two connections: which message each token's book is at, by Polymarket's own time stamp on the message
        (and how many messages carried that stamp), so that _in_book can tell which of the two books is further on."""
        try:
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        self._in_note_at(item)
                return
            if not isinstance(data, dict):
                return
            ts = float(data.get("timestamp") or 0.0)
            if 0.0 < ts < 1e11:
                ts *= 1000.0                                         # a stamp in seconds: in ms like the rest
            aids = {str(data.get("asset_id") or "")}
            for pc in (data.get("price_changes") or data.get("changes") or ()):
                if isinstance(pc, dict):
                    aids.add(str(pc.get("asset_id") or ""))
            for aid in aids:
                if aid:
                    s = self._in_at.get(aid)
                    if s is None or ts > s[0]:
                        self._in_at[aid] = (ts, 1)
                    elif ts == s[0]:
                        self._in_at[aid] = (ts, s[1] + 1)
            if len(self._in_at) > 400:                               # tokens of windows long over
                keep = set(self.asset_ids)
                self._in_at = {a: v for a, v in self._in_at.items() if a in keep}
        except (TypeError, ValueError, AttributeError):
            pass

    # ── one message ──
    def _process_msg(self, data):
        """One message from the feed. It is applied by _in_apply(), which is _process_msg() as it was. Here: one at a
        time (the lock matters with the feed in its own thread), and what the switches note about it."""
        with self._in_lock:
            if self._in_threaded and isinstance(data, dict):         # a book already handed to the bot's loop is not changed under it:
                aid = str(data.get("asset_id") or "")                # a message that changes one in place gets a copy to change
                held = self.books.get(aid) if aid else None
                if held is not None:
                    self.books[aid] = dict(held)
            if in_on("delay_log"):
                self._in_measured(data)
            else:
                self._in_apply(data)
            if self._in_pair:
                self._in_note_at(data)

    def _in_measured(self, data) -> None:
        """delay_log: the message applied, and noted: how late it is by Polymarket's own time stamp on it, whether it
        changed the top of a book, and (two connections) which connection had it first."""
        now = time.time()
        kind, tops = "", None
        if isinstance(data, dict):
            kind = str(data.get("event_type") or ("book" if ("bids" in data or "asks" in data) else "other"))
            try:
                ts = float(data.get("timestamp") or 0.0)
                if ts > 0:
                    self._in_meter.note(now, now * 1000.0 - (ts if ts > 1e11 else ts * 1000.0), kind)
            except (TypeError, ValueError):
                pass
            tops = [(a, (self.books.get(a) or {}).get("best_bid"), (self.books.get(a) or {}).get("best_ask")) for a in list(self.asset_ids)]
            first = self._in_of or self
            if first._in_pair:
                pcs = data.get("price_changes") or data.get("changes")
                p = pcs[0] if isinstance(pcs, list) and pcs and isinstance(pcs[0], dict) else {}
                key = (kind, data.get("timestamp"), data.get("asset_id"), data.get("hash"), data.get("price"), data.get("size"),
                       data.get("best_bid"), data.get("best_ask"), len(pcs) if isinstance(pcs, list) else 0,
                       p.get("asset_id"), p.get("price"), p.get("size"), p.get("side"))
                first._in_race.see(key, "the second connection" if self._in_of else "the first connection", now)
        self._in_apply(data)
        if tops:
            n = sum(1 for a, bb, ba in tops
                    if ((self.books.get(a) or {}).get("best_bid"), (self.books.get(a) or {}).get("best_ask")) != (bb, ba))
            if n:
                self._in_tops[kind] = self._in_tops.get(kind, 0) + n

    def _in_light(self, aid: str, pc: Dict) -> bool:
        """inputs.book_light, for one price change whose level _in_apply has just set in the token's levels.
        As before, every price change rebuilds the token's whole book: both sides sorted again, a new entry made for
        every level. Here the change is put into sorted sides that are kept up to date (one level set, added or taken
        out), and the top of the book is taken from the message (Polymarket sends its best bid and ask with every
        change). The bot is handed these sides when it next reads the book.
        True: done. False: the switch is off, the token has no book yet, or the message is not one this can take (no
        best bid and ask, or something that can't be read); the book is then rebuilt now, as before."""
        if not in_on("book_light"):
            if self._in_work:
                self._in_work.clear()                                # switched off: the sorted sides are not kept up any more
            return False
        held, levels = self.books.get(aid), self.book_levels.get(aid)
        try:
            if held is None or levels is None:
                raise KeyError(aid)
            bb, ba = float(pc["best_bid"]), float(pc["best_ask"])
            side = str(pc.get("side", "")).upper()
            p, s = float(pc["price"]), float(pc.get("size", 0))
            if p != p:
                raise ValueError(p)
        except (KeyError, TypeError, ValueError):
            self._in_work.pop(aid, None)                             # rebuilt in full now, by the lines that follow the call
            self._in_dirty.discard(aid)
            return False
        w = self._in_work.get(aid)
        if w is None or w[0] is not levels["bids"] or w[1] is not levels["asks"]:
            # No sorted sides are kept for these levels yet (the first change, or the first after a full book arrived).
            if aid in self._in_dirty:
                # ... and the ones handed out are behind (they were left to be brought up, and then the switch went off
                # and on): one full rebuild, which has this change in it already.
                self._in_dirty.discard(aid)
                self._sync_book_entry(aid)
                held = self.books[aid]
                held["best_bid"], held["best_ask"] = bb, ba
                self._in_keep(aid, levels, held)
                return True
            w = self._in_keep(aid, levels, held)                     # the sides of the last full rebuild: the levels as they were before this change
        keys, rows, k = (w[2], w[3], -p) if side == "BUY" else ((w[4], w[5], p) if side == "SELL" else (None, None, None))
        if keys is not None:
            i = bisect.bisect_left(keys, k)
            there = i < len(keys) and keys[i] == k
            if s > 0:
                row = {"price": f"{p:.4f}", "size": s, "px": p}      # a new entry: one already handed out stays as it was
                if there:
                    rows[i] = row
                else:
                    keys.insert(i, k)
                    rows.insert(i, row)
            elif there:
                del keys[i]
                del rows[i]
        self.books[aid] = {**held, "best_bid": bb, "best_ask": ba, "updated_at": self.last_ts}
        self._in_dirty.add(aid)
        if len(self._in_dirty) > 400 or len(self._in_work) > 400:    # tokens of windows long over
            live = set(self.asset_ids) | {aid}
            self._in_dirty &= live
            self._in_work = {a: v for a, v in self._in_work.items() if a in live}
        return True

    def _in_keep(self, aid: str, levels: Dict, book: Dict) -> list:
        """book_light: starts keeping a token's sorted sides, from a book whose sides are its levels as they stand:
        [the bids' levels, the asks' levels, the bids' prices (negated, so that they sort like the bids: highest
        first), the bids' entries, the asks' prices, the asks' entries]. The two levels objects tell whether the
        sides still belong to the token's levels (a full book from the feed replaces them)."""
        bids, asks = book.get("bids") or [], book.get("asks") or []
        w = self._in_work[aid] = [levels["bids"], levels["asks"], [-r["px"] for r in bids], list(bids), [r["px"] for r in asks], list(asks)]
        return w

    # ── reading a book ──
    def _in_fresh(self, aid: str):
        """This connection's book of one token, with its sorted sides brought up first if book_light left them behind:
        a copy of the sides it keeps, or a full rebuild if it keeps none for these levels. (Under the lock.)"""
        if aid in self._in_dirty:
            self._in_dirty.discard(aid)
            held = self.books.get(aid)
            if held is not None:
                w, levels = self._in_work.get(aid), self.book_levels.get(aid) or {}
                if w is not None and w[0] is levels.get("bids") and w[1] is levels.get("asks"):
                    self.books[aid] = {**held, "bids": list(w[3]), "asks": list(w[5])}
                else:
                    keep = (held.get("best_bid"), held.get("best_ask"), held.get("updated_at"))
                    self._sync_book_entry(aid)
                    new = self.books[aid]
                    new["best_bid"], new["best_ask"], new["updated_at"] = keep
        return self.books.get(aid)

    def _in_book(self, aid: str):
        """The book of one token as the bot reads it, or None. As it comes: self.books.get(aid), as before.
        book_light: with its sorted sides brought up first. book_both: of the two connections' books, the one
        that is further on by Polymarket's own time stamps (the first connection's when they are level).
        book_thread: read between two messages, never in the middle of one."""
        tw = self._in_twin
        if tw is None and not self._in_dirty and not self._in_threaded:
            return self.books.get(aid)
        with self._in_lock:
            book = self._in_fresh(aid)
            if tw is not None:
                other = tw._in_fresh(aid)
                if other is not None and (book is None or tw._in_at.get(aid, (0.0, 0)) > self._in_at.get(aid, (0.0, 0))):
                    return other
            return book


class BinanceTradeStream(_InBinance):   # TOGGLE: inputs — _InBinance, above, holds what the inputs switches add to this class
    """Ultra-fast spot price feed from Binance Spot index.
    Listens to Binance Spot (bookTicker and trade). Computes real-time mid-price
    from best bid and ask on every order book change with zero basis skew."""
    def __init__(self, symbol: str, on_update: Optional[Callable] = None):
        self.symbol = symbol.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_spot_price = None
        self.last_ts = None
        self.last_source = None
        self._price_history = collections.deque()
        self._in_setup()   # TOGGLE: inputs — what the inputs switches keep on this object (_InBinance, above)
        self.closed = False

    async def _spot_book_worker(self):
        """Binance Spot feed (bookTicker) - fast mid-price on Spot index.
        The mid-price of best bid and ask updates on every book change with zero basis skew."""
        endpoints = [
            f"wss://stream.binance.com:9443/ws/{self.symbol}@bookTicker",
            f"wss://data-stream.binance.vision/ws/{self.symbol}@bookTicker"
        ]
        endpoints = in_first(endpoints)   # TOGGLE: inputs — inputs.binance_first: that address first (as it comes: the list as it is)
        endpoint_idx = 0
        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=20.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=15.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Spot Book WS ({url}): {self.symbol}@bookTicker")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                if "b" in data_msg and "a" in data_msg:
                                    try:
                                        bid = float(data_msg["b"])
                                        ask = float(data_msg["a"])
                                        if bid > 0 and ask > 0:
                                            mid = (bid + ask) / 2.0
                                            self._process_trade(mid, source="binance_spot_book")
                                    except (TypeError, ValueError):
                                        pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                if not self.closed:
                    await asyncio.sleep(2)

    async def _spot_worker(self):
        """Binance Spot feed (trade) - robust fallback and spot anchor."""
        endpoints = [
            f"wss://data-stream.binance.vision/ws/{self.symbol}@trade",
            f"wss://stream.binance.com:9443/ws/{self.symbol}@trade"
        ]
        endpoints = in_first(endpoints)   # TOGGLE: inputs — inputs.binance_first: that address first (as it comes: the list as it is)
        endpoint_idx = 0
        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=20.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=15.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Spot WS ({url}): {self.symbol}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                if in_on("delay_log"):   # TOGGLE: inputs — the feed delay log: how long this trade took to get here
                                    self._in_stamp(data_msg, url)
                                if "p" in data_msg:
                                    try:
                                        p = float(data_msg["p"])
                                        if p > 0:
                                            self._process_trade(p, source="binance_spot_trade")
                                    except (TypeError, ValueError):
                                        pass
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                if not self.closed:
                    await asyncio.sleep(2)

    async def _in_plain(self):   # TOGGLE: inputs — this was start(); _InBinance.start() runs it while inputs.binance_both is off
        await asyncio.gather(
            self._spot_book_worker(),
            self._spot_worker(),
            return_exceptions=True
        )

    def _process_trade(self, p: float, source: str = "binance_spot"):
        now_t = time.time()
        self.last_price = p
        self.last_spot_price = p
        self.last_ts = now_t
        self.last_source = source

        if not hasattr(self, "_price_history"):
            self._price_history = collections.deque()
        self._price_history.append((now_t, p))
        cutoff = now_t - 60.0
        while self._price_history and self._price_history[0][0] < cutoff:
            self._price_history.popleft()
        while len(self._price_history) > 6000:
            self._price_history.popleft()

        if self.on_update:
            try:
                res = self.on_update({"price": p, "spot_price": p, "ts": self.last_ts, "source": source, "is_spot": True})
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception:
                pass

    def get_momentum(self, lookback_s: float = 10.0) -> Optional[float]:
        """Price change over the last `lookback_s` seconds (current - past).
        Positive = spot moving up. Negative = spot moving down."""
        if not hasattr(self, "_price_history") or not self._price_history or self.last_price is None:
            return None
        now_ts = time.time()
        target_ts = now_ts - float(lookback_s or 10.0)
        past_price = None
        for t, pr in self._price_history:
            if t <= target_ts:
                past_price = pr
            else:
                if past_price is None:
                    past_price = pr
                break
        if past_price is None:
            past_price = self._price_history[0][1]
        return float(self.last_price) - float(past_price)

    def get_impulse(self, lookback_s: float = 8.0) -> Optional[float]:
        """Exact Binance spot price change over the last `lookback_s` seconds (current - past).
        Returns None until at least 1.0s of recent tick history is available."""
        if not hasattr(self, "_price_history") or not self._price_history or self.last_price is None or self.last_ts is None:
            return None
        now_ts = time.time()
        if now_ts - self.last_ts > 5.0:
            return None
        if now_ts - self._price_history[0][0] < min(1.0, float(lookback_s) * 0.5):
            return None
        target_ts = now_ts - float(lookback_s)
        past_price = None
        for t, pr in self._price_history:
            if t <= target_ts:
                past_price = pr
            else:
                if past_price is None:
                    past_price = pr
                break
        if past_price is None:
            past_price = self._price_history[0][1]
        return float(self.last_price) - float(past_price)

    def get_last(self):
        return {"price": self.last_spot_price or self.last_price, "ts": self.last_ts, "source": self.last_source}

    def close(self):
        self.closed = True


class BinanceKlineStream:
    """Binance Kline stream for 1m and 5m candles with sliding window buffer.
    Guarantees strict chronological ordering, gap detection and auto-backfill,
    and automatic reconnection with heartbeat to avoid hanging dead sockets."""
    def __init__(self, symbol: str, interval: str, limit: int = 240):
        self.symbol = symbol.lower()
        self.interval = interval
        self.limit = limit
        self.candles = []
        self.closed = False
        self._lock = asyncio.Lock()
        self._step_ms = 60_000 if interval == "1m" else 300_000
        self._backfilling = False

    async def start(self):
        endpoints = [
            f"wss://data-stream.binance.vision/ws/{self.symbol}@kline_{self.interval}",
            f"wss://stream.binance.com:9443/ws/{self.symbol}@kline_{self.interval}"
        ]
        endpoint_idx = 0

        # Proactively backfill if candles are missing or insufficient
        if len(self.candles) < 20:
            asyncio.create_task(self._backfill_gap())

        while not self.closed:
            url = endpoints[endpoint_idx % len(endpoints)]
            endpoint_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_read=30.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Binance Kline WS ({url}): {self.symbol} {self.interval}")
                        while not self.closed:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data_msg = fast_loads(msg.data)
                                k = data_msg.get("k", {})
                                if k.get("t") is not None:
                                    candle = {
                                        "openTime": int(k.get("t")),
                                        "open": float(k.get("o")),
                                        "high": float(k.get("h")),
                                        "low": float(k.get("l")),
                                        "close": float(k.get("c")),
                                        "volume": float(k.get("v")),
                                        "closeTime": int(k.get("T")),
                                        "isClosed": bool(k.get("x"))
                                    }
                                    async with self._lock:
                                        self._update_candle(candle)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                break
            except Exception as e:
                if not self.closed:
                    print(f"Binance Kline WS reconnecting ({url}): {e}")
                    await asyncio.sleep(2)

    def _update_candle(self, candle: Dict):
        if not self.candles:
            self.candles.append(candle)
            asyncio.create_task(self._backfill_gap())
            return

        last = self.candles[-1]
        c_open = candle["openTime"]
        l_open = last["openTime"]

        if c_open == l_open:
            self.candles[-1] = candle
        elif c_open > l_open:
            if (c_open - l_open) > (self._step_ms * 1.5) or len(self.candles) < 15:
                asyncio.create_task(self._backfill_gap())
            self.candles.append(candle)
            if len(self.candles) > self.limit:
                self.candles.pop(0)
        else:
            for i, c in enumerate(self.candles):
                if c["openTime"] == c_open:
                    self.candles[i] = candle
                    return
            self.candles.append(candle)
            self.candles.sort(key=lambda x: x["openTime"])
            if len(self.candles) > self.limit:
                self.candles = self.candles[-self.limit:]

    async def _backfill_gap(self):
        if getattr(self, "_backfilling", False):
            return
        self._backfilling = True
        try:
            from . import data
            klines = await data.fetch_klines(self.symbol.upper(), self.interval, self.limit)
            if klines and len(klines) >= 15:
                async with self._lock:
                    self.set_candles(klines)
        except Exception:
            pass
        finally:
            self._backfilling = False

    def set_candles(self, candles: List[Dict]):
        if not candles:
            return
        seen = set()
        deduped = []
        for c in sorted(candles, key=lambda x: x["openTime"]):
            if c["openTime"] not in seen:
                seen.add(c["openTime"])
                deduped.append(c)
        self.candles = deduped[-self.limit:]

    def get_candles(self) -> List[Dict]:
        return list(self.candles)

    def close(self):
        self.closed = True


class PolymarketChainlinkStream:
    def __init__(self, ws_url: str, symbol_includes: str = "btc", on_update: Optional[Callable] = None):
        self.ws_url = ws_url
        self.symbol_includes = symbol_includes.lower()
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False

    async def start(self):
        if not self.ws_url:
            return

        async def ping_loop(ws):
            while not self.closed:
                try:
                    await ws.send_str("PING")
                    await asyncio.sleep(5)
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                }
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=25.0)
                async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
                    print(f"Connecting to Polymarket WS: {self.ws_url}")
                    async with session.ws_connect(self.ws_url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Polymarket WS. Subscribing to topics...")

                        topics = ["crypto_prices_chainlink", "price_chainlink", "settlement_prices"]
                        for t in topics:
                            await ws.send_json({"action": "subscribe", "topic": t})

                        ping_task = asyncio.create_task(ping_loop(ws))
                        try:
                            while not self.closed:
                                msg = await ws.receive(timeout=30.0)
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_text = msg.data
                                    if data_text in ("PONG", "OK", "PONG\n"):
                                        continue

                                    try:
                                        data_msg = fast_loads(data_text)
                                    except Exception:
                                        continue

                                    topic = data_msg.get("topic")
                                    if topic not in ("crypto_prices_chainlink", "price_chainlink", "settlement_prices"):
                                        continue

                                    payload = data_msg.get("payload", {})
                                    if isinstance(payload, str):
                                        try:
                                            payload = fast_loads(payload)
                                        except:
                                            continue

                                    updates = payload if isinstance(payload, list) else [payload]

                                    for update in updates:
                                        if not isinstance(update, dict): continue

                                        sym = str(update.get("symbol") or update.get("pair") or update.get("ticker") or update.get("asset") or "").lower()
                                        if self.symbol_includes:
                                            target = self.symbol_includes.lower()
                                            if target not in sym and not (target == "btc" and "bitcoin" in sym):
                                                continue

                                        try:
                                            price_val = update.get("price") or update.get("value") or update.get("current")
                                            if price_val is not None:
                                                self.last_price = float(price_val)
                                                ts_val = update.get("timestamp") or update.get("updated_at") or time.time()
                                                updated_at = float(ts_val)
                                                if updated_at < 10000000000: updated_at *= 1000
                                                self.last_updated_at = updated_at
                                                self.last_received_at = time.time() * 1000.0

                                                if self.on_update:
                                                    res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "receivedAt": self.last_received_at, "source": "polymarket_ws"})
                                                    if asyncio.iscoroutine(res):
                                                        asyncio.create_task(res)
                                        except:
                                            continue

                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                    break
                        finally:
                            ping_task.cancel()
            except Exception as e:
                print(f"WS Error (Polymarket): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {
            "price": self.last_price,
            "updatedAt": self.last_updated_at,
            "receivedAt": getattr(self, "last_received_at", None),
            "source": "polymarket_ws",
        }

    def close(self):
        self.closed = True


class PolymarketClobMarketStream(_InBook):   # TOGGLE: inputs — _InBook, above BinanceTradeStream, holds what the inputs switches add to this class
    """Real-time orderbook and price feed for active Polymarket tokens over CLOB Market WS."""
    def __init__(self, ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market", on_update=None):
        self.ws_url = ws_url
        self.on_update = on_update
        self.asset_ids: List[str] = []
        self.books: Dict[str, Dict] = {} # asset_id -> {"bids": [...], "asks": [...], "best_bid": float, "best_ask": float}
        self.book_levels: Dict[str, Dict[str, Dict[float, float]]] = {} # asset_id -> {"bids": {px: sz}, "asks": {px: sz}}
        # PATCH: longshot maker — recent trades per asset_id: (received, price, size, Polymarket's timestamp)
        self.trades: Dict[str, "collections.deque"] = {}
        self.last_ts: float = 0
        self.closed = False
        self._ws = None
        self._in_setup()   # TOGGLE: inputs — what the inputs switches keep on this object (_InBook)

    def reset_books(self):
        self.books.clear()
        self.book_levels.clear()

    def update_assets(self, asset_ids: List[str]):
        new_ids = sorted(list(set(asset_ids)))
        if new_ids != self.asset_ids:
            self.asset_ids = new_ids
            if self._in_twin is not None:   # TOGGLE: inputs — inputs.book_both: the second connection follows the same tokens
                self._in_twin.update_assets(asset_ids)
            if self._ws and not self._ws.closed and self.asset_ids:
                self._in_spawn(self._subscribe(self._ws))   # TOGGLE: inputs — on the loop this connection lives on (as it comes: asyncio.create_task, as before)

    async def _subscribe(self, ws):
        if not self.asset_ids:
            return
        sub_msg = {
            "assets_ids": self.asset_ids,
            "type": "market"
        }
        if in_on("top_of_book"):   # TOGGLE: inputs — inputs.top_of_book: Polymarket then also sends a best_bid_ask message when the top of a book changes
            sub_msg["custom_feature_enabled"] = True
        try:
            await ws.send_json(sub_msg)
            print(f"Subscribed to Polymarket CLOB Market WS for tokens: {self.asset_ids}")
        except Exception as e:
            print(f"Failed to subscribe to CLOB market WS: {e}")

    async def _in_connect(self):   # TOGGLE: inputs — this was start(); _InBook.start() keeps it running
        async def ping_loop(ws):
            while not self.closed:
                try:
                    await asyncio.sleep(10)
                    if not ws.closed:
                        await ws.send_str("PING")
                except:
                    break

        while not self.closed:
            try:
                proxy = get_proxy_url_for(self.ws_url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=25.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(self.ws_url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        self._ws = ws
                        print(f"Connected to Polymarket CLOB Market WS: {self.ws_url}")
                        await self._subscribe(ws)
                        ping_task = asyncio.create_task(ping_loop(ws))

                        try:
                            while not self.closed:
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_text = msg.data.strip() if msg.data else ""
                                    if not data_text or data_text in ("PONG", "PING", "OK", "PONG\n"):
                                        continue
                                    try:
                                        data = fast_loads(data_text)
                                        self._process_msg(data)
                                    except Exception:
                                        continue
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                    break
                        finally:
                            ping_task.cancel()
            except Exception as e:
                print(f"Polymarket CLOB Market WS error: {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def _sync_book_entry(self, aid: str):
        levels = self.book_levels.get(aid, {"bids": {}, "asks": {}})
        bids_map = levels.get("bids", {})
        asks_map = levels.get("asks", {})

        # Bids: descending by price (highest bid first)
        sorted_bids = sorted(
            [{"price": f"{p:.4f}", "size": s, "px": p} for p, s in bids_map.items() if s > 0],
            key=lambda x: x["px"],
            reverse=True
        )
        # Asks: ascending by price (lowest ask first)
        sorted_asks = sorted(
            [{"price": f"{p:.4f}", "size": s, "px": p} for p, s in asks_map.items() if s > 0],
            key=lambda x: x["px"]
        )

        best_bid = sorted_bids[0]["px"] if sorted_bids else None
        best_ask = sorted_asks[0]["px"] if sorted_asks else None

        self.books[aid] = {
            "bids": sorted_bids,
            "asks": sorted_asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "updated_at": self.last_ts
        }

    def _in_apply(self, data):   # TOGGLE: inputs — this was _process_msg(); _InBook._process_msg() calls it for every message
        self.last_ts = time.time()
        # Single book dict snapshot
        if isinstance(data, dict) and ("bids" in data or "asks" in data or data.get("event_type") in ("book", "market")):
            aid = str(data.get("asset_id") or data.get("market") or "")
            if aid:
                levels = self.book_levels.setdefault(aid, {"bids": {}, "asks": {}})
                if "bids" in data:
                    levels["bids"] = {}
                    for b in data.get("bids", []):
                        try:
                            levels["bids"][float(b["price"])] = float(b.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                if "asks" in data:
                    levels["asks"] = {}
                    for a in data.get("asks", []):
                        try:
                            levels["asks"][float(a["price"])] = float(a.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                self._sync_book_entry(aid)
                if data.get("best_bid") is not None:
                    try: self.books[aid]["best_bid"] = float(data["best_bid"])
                    except: pass
                if data.get("best_ask") is not None:
                    try: self.books[aid]["best_ask"] = float(data["best_ask"])
                    except: pass

        # Full book snapshot (list)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("event_type") == "last_trade_price":
                    self._note_trade(item)   # PATCH: longshot maker — a trade, not a book snapshot
                    continue
                if isinstance(item, dict) and "asset_id" in item:
                    aid = str(item["asset_id"])
                    levels = self.book_levels.setdefault(aid, {"bids": {}, "asks": {}})
                    levels["bids"] = {}
                    levels["asks"] = {}
                    for b in item.get("bids", []):
                        try:
                            levels["bids"][float(b["price"])] = float(b.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                    for a in item.get("asks", []):
                        try:
                            levels["asks"][float(a["price"])] = float(a.get("size", 0))
                        except (TypeError, ValueError, KeyError):
                            pass
                    self._sync_book_entry(aid)
                    if item.get("best_bid") is not None:
                        try: self.books[aid]["best_bid"] = float(item["best_bid"])
                        except: pass
                    if item.get("best_ask") is not None:
                        try: self.books[aid]["best_ask"] = float(item["best_ask"])
                        except: pass

        # Price / book delta updates
        elif isinstance(data, dict):
            price_changes = data.get("price_changes") or data.get("changes") or []
            if isinstance(price_changes, list):
                for pc in price_changes:
                    if not isinstance(pc, dict): continue
                    aid = str(pc.get("asset_id") or data.get("asset_id") or "")
                    if not aid: continue
                    levels = self.book_levels.setdefault(aid, {"bids": {}, "asks": {}})
                    side = str(pc.get("side", "")).upper()
                    try:
                        p = float(pc["price"])
                        s = float(pc.get("size", 0))
                        if side == "BUY":
                            if s > 0: levels["bids"][p] = s
                            else: levels["bids"].pop(p, None)
                        elif side == "SELL":
                            if s > 0: levels["asks"][p] = s
                            else: levels["asks"].pop(p, None)
                    except (TypeError, ValueError, KeyError):
                        pass

                    if self._in_light(aid, pc):   # TOGGLE: inputs — inputs.book_light: the change put into the sorted book where it belongs, instead of the rebuild below
                        continue
                    self._sync_book_entry(aid)
                    if pc.get("best_bid") is not None:
                        try: self.books[aid]["best_bid"] = float(pc["best_bid"])
                        except (TypeError, ValueError): pass
                    if pc.get("best_ask") is not None:
                        try: self.books[aid]["best_ask"] = float(pc["best_ask"])
                        except (TypeError, ValueError): pass

            aid = str(data.get("asset_id") or "")
            if aid:
                if aid in self.books:
                    self.books[aid]["updated_at"] = self.last_ts
                    if data.get("best_bid") is not None:
                        try: self.books[aid]["best_bid"] = float(data["best_bid"])
                        except (TypeError, ValueError): pass
                    if data.get("best_ask") is not None:
                        try: self.books[aid]["best_ask"] = float(data["best_ask"])
                        except (TypeError, ValueError): pass

            if data.get("event_type") == "last_trade_price":   # PATCH: longshot maker
                self._note_trade(data)

        if self.on_update:
            try:
                self.on_update(data)
            except Exception:
                pass

    def _note_trade(self, d: Dict) -> None:
        """PATCH: longshot maker — a last_trade_price event (a match on this token)."""
        aid = str(d.get("asset_id") or "")
        try:
            px, sz = float(d["price"]), float(d.get("size") or 0.0)
        except (TypeError, ValueError, KeyError):
            return
        if not aid or px <= 0 or sz <= 0:
            return
        q = self.trades.get(aid)
        if q is None:
            q = self.trades[aid] = collections.deque(maxlen=200)
        q.append((time.time(), px, sz, str(d.get("timestamp") or "")))

    def recent_trades(self, asset_id: str, since_ts: float) -> List[tuple]:
        """PATCH: longshot maker — trades on `asset_id` received after since_ts (unix seconds)."""
        return [t for t in list(self.trades.get(str(asset_id), ())) if t[0] > since_ts]   # TOGGLE: inputs — list(): a copy to read, since the feed (in its own thread) may be adding to it

    def get_token_market(self, asset_id: str) -> Dict:
        return self._in_book(str(asset_id)) or {}   # TOGGLE: inputs — as it comes this is self.books.get(...), as before

    def get_summary(self, asset_id: str, max_age_s: float = 5.0) -> Optional[Dict]:
        """Return best bid/ask, spread, liquidity and bids list for `asset_id` if fresh."""
        aid = str(asset_id)
        book = self._in_book(aid)   # TOGGLE: inputs — as it comes this is self.books.get(aid), as before
        if not book:
            return None
        updated_at = book.get("updated_at", 0)
        if max_age_s > 0 and (time.time() - updated_at) > max_age_s:
            return None
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        best_bid = book.get("best_bid")
        best_ask = book.get("best_ask")

        if best_bid is None and bids:
            try:
                best_bid = float(bids[0].get("px", bids[0]["price"]))
            except:
                pass
        if best_ask is None and asks:
            try:
                best_ask = float(asks[0].get("px", asks[0]["price"]))
            except:
                pass

        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        bid_liq = 0.0
        for b in bids[:5]:
            try:
                bid_liq += float(b.get("size", 0))
            except:
                pass

        ask_liq = 0.0
        for a in asks[:5]:
            try:
                ask_liq += float(a.get("size", 0))
            except:
                pass

        return {
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "spread": spread,
            "bidLiquidity": bid_liq,
            "askLiquidity": ask_liq,
            "bids": bids,
            "asks": asks,
            "updated_at": updated_at
        }

    def close(self):
        self.closed = True


class ChainlinkPriceStream:
    def __init__(self, aggregator: str, decimals: int = 8, on_update: Optional[Callable] = None):
        self.aggregator = aggregator
        self.decimals = decimals
        self.on_update = on_update
        self.last_price = None
        self.last_updated_at = None
        self.closed = False
        self.wss_urls = settings.POLYGON_WSS_URLS + ([settings.POLYGON_WSS_URL] if settings.POLYGON_WSS_URL else [])

    async def start(self):
        if not self.wss_urls or not self.aggregator:
            return

        url_idx = 0
        call_msg = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "eth_call",
            "params": [
                {"to": self.aggregator, "data": "0xfeaf968c"},
                "latest"
            ]
        }

        while not self.closed:
            url = self.wss_urls[url_idx % len(self.wss_urls)]
            url_idx += 1
            try:
                proxy = get_proxy_url_for(url)
                timeout = aiohttp.ClientTimeout(total=None, sock_connect=8.0, sock_read=30.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(url, heartbeat=20.0, proxy=proxy if proxy else None) as ws:
                        print(f"Connected to Chainlink RPC WS: {url}")
                        # 1. Subscribe to on-chain AnswerUpdated logs
                        sub_msg = {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "eth_subscribe",
                            "params": [
                                "logs",
                                {
                                    "address": self.aggregator,
                                    "topics": ["0x05598845ccd9c46647361c770d3023029a3514781ca1029c91d84f2913e79435"] # AnswerUpdated topic
                                }
                            ]
                        }
                        await ws.send_json(sub_msg)

                        # 2. Query initial latestRoundData() immediately over WS
                        await ws.send_json(call_msg)

                        # 3. Periodic refresh loop over WS to keep sync and serve as keepalive
                        async def _sync_loop():
                            while not self.closed:
                                try:
                                    await asyncio.sleep(5)
                                    if not ws.closed:
                                        await ws.send_json(call_msg)
                                except Exception:
                                    break

                        sync_task = asyncio.create_task(_sync_loop())

                        try:
                            while not self.closed:
                                msg = await ws.receive()
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_res = fast_loads(msg.data)
                                    # Handle AnswerUpdated event logs
                                    if data_res.get("method") == "eth_subscription":
                                        log = data_res.get("params", {}).get("result", {})
                                        topics = log.get("topics", [])
                                        if len(topics) >= 2:
                                            answer = int(topics[1], 16)
                                            if answer >= 2**255:
                                                answer -= 2**256

                                            self.last_price = answer / (10 ** self.decimals)
                                            data_hex = log.get("data", "0x")
                                            if len(data_hex) >= 66:
                                                self.last_updated_at = int(data_hex[2:66], 16) * 1000
                                            else:
                                                self.last_updated_at = int(time.time() * 1000)

                                            if self.on_update:
                                                res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                                                if asyncio.iscoroutine(res):
                                                    asyncio.create_task(res)

                                    # Handle latestRoundData() eth_call response
                                    elif data_res.get("id") == 2 or (isinstance(data_res.get("result"), str) and len(data_res.get("result", "")) >= 130):
                                        res_hex = data_res.get("result", "")
                                        if isinstance(res_hex, str) and len(res_hex) >= 130 and res_hex.startswith("0x"):
                                            try:
                                                hex_body = res_hex[2:]
                                                answer = int(hex_body[64:128], 16)
                                                if answer >= 2**255:
                                                    answer -= 2**256
                                                updated_at = int(hex_body[192:256], 16) * 1000
                                                self.last_price = answer / (10 ** self.decimals)
                                                self.last_updated_at = updated_at

                                                if self.on_update:
                                                    res = self.on_update({"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"})
                                                    if asyncio.iscoroutine(res):
                                                        asyncio.create_task(res)
                                            except Exception:
                                                pass

                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                                    break
                        finally:
                            sync_task.cancel()
            except Exception as e:
                print(f"WS Error (Chainlink RPC): {e}")
                if not self.closed:
                    await asyncio.sleep(2)

    def get_last(self):
        return {"price": self.last_price, "updatedAt": self.last_updated_at, "source": "chainlink_ws"}

    def close(self):
        self.closed = True