#!/usr/bin/env python3
"""Slow, straightforward reference implementation of rules.md, written
independently of the Rust pipeline, for cross-checking on small datasets.
Usage: reference.py DATA_DIR OUT_DIR  (writes user_daily.csv, campaign_daily.csv)"""
import bisect
import calendar
import datetime as dt
import glob
import json
import os
import re
import sys

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000


def read_lines(path):
    with open(path, 'rb') as f:
        data = f.read()
    parts = data.split(b'\n')
    if data.endswith(b'\n'):
        parts = parts[:-1]
    return [p.decode('utf-8', 'surrogateescape') for p in parts[1:]]  # skip header


def parse_line(line, n):
    if line == '':
        return None
    fields = line.split(',')
    if len(fields) != n:
        return None
    out = []
    for f in fields:
        if '"' in f:
            if len(f) >= 2 and f[0] == '"' and f[-1] == '"' and '"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f if f != '' else None)
    return out


DIG = set('0123456789')
F1 = re.compile(r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$', re.ASCII)
F2 = re.compile(r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})([+-])(\d{2}):(\d{2})$', re.ASCII)


def civil_ms(y, mo, d, h, mi, s):
    if not (1 <= mo <= 12):
        return None
    if y < 1:
        return None  # python datetime limitation; never inside a window anyway
    if not (1 <= d <= calendar.monthrange(y, mo)[1]):
        return None
    if h > 23 or mi > 59 or s > 59:
        return None
    days = (dt.date(y, mo, d) - dt.date(1970, 1, 1)).days
    return ((days * 24 + h) * 60 + mi) * 60 * 1000 + s * 1000


def parse_ts(v):
    if v is None:
        return None
    m = F1.match(v)
    if m:
        return civil_ms(*map(int, m.groups()))
    m = F2.match(v)
    if m:
        y, mo, d, h, mi, s = map(int, m.groups()[:6])
        sign, oh, om = m.group(7), int(m.group(8)), int(m.group(9))
        if oh > 23 or om > 59:
            return None
        t = civil_ms(y, mo, d, h, mi, s)
        if t is None:
            return None
        off = (oh * 60 + om) * 60 * 1000
        return t - off if sign == '+' else t + off
    if v and all(c in DIG for c in v):
        x = int(v)
        if x >= 2**63:
            return None
        return x
    return None


def iso_ms(v):
    return civil_ms(*map(int, F1.match(v).groups()))


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, 'manifest.json')))
    ws, we = iso_ms(man['window_start']), iso_ms(man['window_end'])

    products = {}
    for line in read_lines(os.path.join(data, 'products.csv')):
        r = parse_line(line, 2)
        if r and r[0] is not None:
            products[r[0]] = r[1] or ''
    campaigns = {}
    for line in read_lines(os.path.join(data, 'campaigns.csv')):
        r = parse_line(line, 2)
        if r and r[0] is not None:
            campaigns[r[0]] = r[1] or ''

    # links
    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    for p in sorted(glob.glob(os.path.join(data, 'links', '*.csv'))):
        for line in read_lines(p):
            r = parse_line(line, 2)
            if not r or r[0] is None or r[1] is None:
                continue
            a, b = r
            parent.setdefault(a, a)
            parent.setdefault(b, b)
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb
    groups = {}
    for x in list(parent):
        groups.setdefault(find(x), []).append(x)
    name = {}
    for root, members in groups.items():
        nm = min(members, key=lambda s: s.encode('utf-8', 'surrogateescape'))
        for m in members:
            name[m] = nm

    def person(u):
        return name.get(u, u)

    # events
    best = {}
    for p in sorted(glob.glob(os.path.join(data, 'events', '*.csv'))):
        for line in read_lines(p):
            r = parse_line(line, 7)
            if not r or r[0] is None:
                continue
            ing = iso_ms(r[3])
            cur = best.get(r[0])
            if cur is None or ing < cur[0]:
                best[r[0]] = (ing, r)
    per = {}
    for eid, (ing, r) in best.items():
        _, uid, ts, _, et, pid, price = r
        if uid is None or pid is None:
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we):
            continue
        if et not in ('view', 'cart', 'purchase'):
            continue
        pr = 0
        if et == 'purchase':
            if price is None or not all(c in DIG for c in price):
                continue
            pr = int(price)
            if not (1 <= pr <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        cat = products.get(pid, 'unknown')
        per.setdefault(person(uid), []).append((t, et, pr, cat))

    # clicks
    clicks = {}
    seen = set()
    for p in sorted(glob.glob(os.path.join(data, 'clicks', '*.csv'))):
        for line in read_lines(p):
            r = parse_line(line, 4)
            if not r or r[0] is None:
                continue
            if r[0] in seen:
                continue
            seen.add(r[0])
            cid, uid, camp, ts = r
            if uid is None:
                continue
            t = parse_ts(ts)
            if t is None or not (ws - WEEK <= t < we):
                continue
            if camp not in campaigns:
                continue
            clicks.setdefault(person(uid), []).append((t, cid.encode('utf-8', 'surrogateescape'), camp))

    ud = {}
    cd = {}

    def cdrow(c, day):
        return cd.setdefault((c, day), [0, 0, 0, 0])

    for pk, evs in per.items():
        evs.sort(key=lambda e: e[0])
        cl = sorted(clicks.get(pk, []))
        cts = [c[0] for c in cl]
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            row = ud.setdefault((pk, day), [0, 0, 0, {}])
            row[0] += 1
            row[1] += len(s)
            rev = 0
            purch = False
            for (_, et, pr, cat) in s:
                if et == 'purchase':
                    rev += pr
                    purch = True
                    row[3][cat] = row[3].get(cat, 0) + pr
            row[2] += rev
            # credit
            hi = bisect.bisect_right(cts, start)
            cands = [c for c in cl[:hi] if start - c[0] <= WEEK]
            if cands:
                tmax = max(c[0] for c in cands)
                win = min((c for c in cands if c[0] == tmax), key=lambda c: c[1])
                camp = win[2]
            else:
                camp = 'organic'
            x = cdrow(camp, day)
            x[0] += 1
            x[1] += 1 if purch else 0
            x[2] += rev
            for c in set(c[2] for c in cands):
                cdrow(c, day)[3] += 1

    def dstr(day):
        return (dt.date(1970, 1, 1) + dt.timedelta(days=day)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, 'user_daily.csv'), 'w', encoding='utf-8', errors='surrogateescape') as f:
        f.write('user_id,day,sessions,events,revenue_cents,top_category\n')
        for (pk, day), (s, n, rev, cats) in sorted(ud.items()):
            top = ''
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0]
            f.write(f'{pk},{dstr(day)},{s},{n},{rev},{top}\n')
    with open(os.path.join(out, 'campaign_daily.csv'), 'w') as f:
        f.write('campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n')
        for (c, day), (s, ps, rev, a) in sorted(cd.items()):
            ch = 'organic' if c == 'organic' else campaigns[c]
            f.write(f'{c},{ch},{dstr(day)},{s},{ps},{rev},{a}\n')


if __name__ == '__main__':
    main()
