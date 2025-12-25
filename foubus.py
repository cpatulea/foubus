#!venv/bin/python3 -u
import curses
import datetime
import glob
import http.server
import io
import itertools
import os
import os.path
import pickle
import shutil
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse

import gtfs_kit
import pandas as pd
import urllib3
from google.protobuf import text_format
from google.transit import gtfs_realtime_pb2
from loguru import logger

# Configure loguru
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="{time:YYYY-MM-DD HH:mm:ss} [{file}:{line}] [{name}] [{thread.name}] {level}: {message}",
    level="INFO",
)

http_pool = urllib3.PoolManager()
revalidated = datetime.datetime.min

STOPS = {
    # https://openbusmap.org/#-73.5873;45.4784;17
    # stop_name : Google Maps walking time from Foulab
    "Saint-Antoine / Saint-Ferdinand": 3,
    "Saint-Ferdinand / Saint-Antoine": 4,
    "Station Place-Saint-Henri": 7,
    "Station Place-Saint-Henri / Saint-Ferdinand": 7,
    "Notre-Dame / Place Saint-Henri": 8,
}


def download():
    global revalidated

    if datetime.datetime.now() >= (revalidated + datetime.timedelta(hours=24)).replace(
        hour=3
    ):
        logger.info("Revalidating (last at %s)", revalidated)

        url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"

        try:
            mtime = os.path.getmtime(os.path.basename(url))
        except FileNotFoundError:
            headers = {}
        else:
            headers = {
                "If-Modified-Since": time.strftime(
                    "%a, %d %b %Y %H:%M:%S GMT", time.gmtime(mtime)
                )
            }
        logger.info("If-Modified-Since: %s", headers.get("If-Modified-Since"))

        resp = http_pool.request(
            "GET", url, headers=headers, timeout=3600.0, preload_content=False
        )
        logger.info(
            "Response: %s %s (headers: %s)", resp.status, resp.reason, resp.headers
        )

        if resp.status == 304:
            revalidated = datetime.datetime.now()
            return
        elif resp.status == 200:
            last_modified = time.mktime(
                time.strptime(
                    resp.headers["Last-Modified"], "%a, %d %b %Y %H:%M:%S GMT"
                )
            )

            with tempfile.NamedTemporaryFile(
                dir=".", prefix=os.path.basename(url) + "-", delete=False
            ) as f:
                logger.info("Downloading to %s", f.name)
                while chunk := resp.read(1024 * 1024):
                    f.write(chunk)

            resp.release_conn()

            os.utime(f.name, (last_modified, last_modified))
            os.rename(f.name, os.path.basename(url))
            logger.info("Saved to %s", os.path.basename(url))

            revalidated = datetime.datetime.now()
        else:
            raise ValueError(f"Unexpected status: {resp.status}")


def build_stop_timetable(date):
    """Run at 6am"""
    logger.info("Reading feed...")
    feed = gtfs_kit.read_feed("gtfs_stm.zip", dist_units="m")
    logger.info("Feed loaded")

    stops = feed.stops[feed.stops["stop_name"].isin(STOPS)]
    feed.stop_times = feed.stop_times[feed.stop_times["stop_id"].isin(stops["stop_id"])]

    with tempfile.TemporaryDirectory(dir=".", prefix="stop_timetable-") as d:
        for stop_id, stop_name in zip(stops["stop_id"], stops["stop_name"]):
            tt = feed.build_stop_timetable(stop_id, [date.strftime("%Y%m%d")])
            tt["stop_name"] = stop_name
            with open(f"{d}/stop-{stop_id}.txt", "w") as f:
                f.write(str(tt))
            tt.to_csv(f"{d}/stop-{stop_id}.csv")
            tt.to_json(f"{d}/stop-{stop_id}.json")
            tt.to_pickle(f"{d}/stop-{stop_id}.pickle")
            tt.to_html(f"{d}/stop-{stop_id}.html")
            logger.info("Built stop %s (%s)", stop_id, stop_name)
        try:
            shutil.rmtree("stop_timetable/")
        except FileNotFoundError:
            pass
        os.rename(d, "stop_timetable")


def load_pickle():
    tts = []
    for path in glob.glob("stop_timetable/*.pickle"):
        with open(path, "rb") as p:
            tts.append(pickle.load(p))
    tt = pd.concat(tts).reset_index()
    return tt


def decorate_timetable(tt, now):
    # exclude 17 Nord at stop 51986 (Station Place-Saint-Henri / Saint-Ferdinand),
    # there's a closer stop at 51916
    tt = tt[
        ~(
            (tt["route_id"] == "17")
            & (tt["trip_headsign"] == "Nord")
            & (tt["stop_id"] == "51986")
        )
    ]

    # Avoid future SettingWithCopyWarning
    tt = tt.copy()

    tt["route_id_int"] = tt["route_id"].astype(int)

    # Map special headsigns to simplified labels
    headsign_map = {
        "Station Henri-Bourassa": "Montmorency",
        "Station Montmorency -Zone B": "Montmorency",
        "Station Côte-Vertu": "Côte-Vertu",
    }

    # Using map with fillna to create trip_label
    tt["trip_label"] = (
        tt["trip_headsign"]
        .map(headsign_map)
        .fillna(tt["route_id"] + " " + tt["trip_headsign"])
    )

    def _departure_time_dt(row):
        isodate = row["date"][0:4] + "-" + row["date"][4:6] + "-" + row["date"][6:8]
        noon = datetime.datetime.combine(
            datetime.date.fromisoformat(isodate), datetime.time(12, 0, 0)
        )
        dep = row["departure_time"]
        h, m, s = map(int, dep.split(":"))
        return (
            noon
            - datetime.timedelta(hours=12)
            + datetime.timedelta(hours=h, minutes=m, seconds=s)
        )

    tt["departure_time_dt"] = tt.apply(_departure_time_dt, axis=1)

    def _leave_in(row):
        return row["departure_time_dt"] - now

    tt["leave_in"] = tt.apply(_leave_in, axis=1)

    routes = tt[["route_id", "route_id_int", "trip_label"]].value_counts()
    routes = pd.DataFrame(routes).sort_values(["route_id_int", "trip_label"])

    return routes, tt


def apply_realtime(
    tt, now, url="https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
):
    resp = http_pool.request(
        "GET",
        url,
        headers={"Apikey": open("stm-apikey.txt").read().strip()},
        timeout=10.0,
    )
    logger.info(
        "Response: %s %s (headers: %s, size: %d)",
        resp.status,
        resp.reason,
        resp.headers,
        len(resp.data),
    )
    if resp.status != 200:
        logger.warning("Response error: %r", resp.data.decode("utf-8", "replace"))
        raise ValueError(str(resp.status))
    fm = gtfs_realtime_pb2.FeedMessage.FromString(resp.data)
    with open("tripUpdates.textproto", "w") as f:
        f.write(str(fm))
    logger.info(
        "TripUpdates header: %s (timestamp %s, age %d seconds)",
        text_format.MessageToString(fm.header, as_one_line=True),
        datetime.datetime.fromtimestamp(fm.header.timestamp),
        (
            datetime.datetime.now()
            - datetime.datetime.fromtimestamp(fm.header.timestamp)
        ).total_seconds(),
    )
    logger.info(
        "TripUpdates: %d entity, %d stop_time_update",
        len(fm.entity),
        sum(len(e.trip_update.stop_time_update) for e in fm.entity),
    )

    updates = 0
    for entity in fm.entity:
        assert entity.trip_update.trip.trip_id, str(entity)
        if (tt["trip_id"] == entity.trip_update.trip.trip_id).any():
            logger.info(
                "trip_update for %s: %s: %d stop_time_update",
                entity.trip_update.trip.trip_id,
                text_format.MessageToString(entity.trip_update.trip, as_one_line=True),
                len(entity.trip_update.stop_time_update),
            )
            last_stop_sequence = None
            for stu in entity.trip_update.stop_time_update:
                # TODO: implement delay propagation
                # https://gtfs.org/documentation/realtime/feed-entities/trip-updates/#:~:text=If%20one%20or%20more%20stops%20are%20missing
                assert (
                    last_stop_sequence is None
                    or last_stop_sequence + 1 == stu.stop_sequence
                ), text_format.MessageToString(stu, as_one_line=True)

                if (
                    stu.schedule_relationship
                    != gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED
                ):
                    continue

                row = tt[
                    (tt["trip_id"] == entity.trip_update.trip.trip_id)
                    & (tt["date"] == entity.trip_update.trip.start_date)
                    & (tt["stop_sequence"] == stu.stop_sequence)
                    & (tt["stop_id"] == stu.stop_id)
                ]
                if not row.empty:
                    assert len(row) == 1, row
                    # print(row)
                    # print(stu)
                    if not stu.departure.time:
                        logger.warning(
                            "No departure time: trip: %s stop_time_update: %s",
                            text_format.MessageToString(
                                entity.trip_update.trip, as_one_line=True
                            ),
                            text_format.MessageToString(stu, as_one_line=True),
                        )
                    else:
                        # row.loc[:,'realtime'] = stu.departure.time
                        tt.loc[
                            (tt["trip_id"] == entity.trip_update.trip.trip_id)
                            & (tt["date"] == entity.trip_update.trip.start_date)
                            & (tt["stop_sequence"] == stu.stop_sequence)
                            & (tt["stop_id"] == stu.stop_id),
                            ["realtime", "leave_in"],
                        ] = [
                            True,
                            datetime.datetime.fromtimestamp(stu.departure.time) - now,
                        ]
                        print(row)
                        updates += 1

    logger.info("TripUpdates for us: %d", updates)
    return tt


def next_trips(routes, tt, now):
    tt["next"] = len(tt) * [False]
    tt["last"] = len(tt) * [False]

    # add walking time before picking next (might be too late)
    def _add_walking_time(row):
        return row["leave_in"] - pd.Timedelta(minutes=STOPS[row["stop_name"]])

    tt["leave_in"] = tt.apply(_add_walking_time, axis=1)
    for (route_id, _, trip_label), _ in routes.iterrows():
        logger.info("= %s =", trip_label)
        trips = list(
            tt[
                (tt["trip_label"] == trip_label)
                & (tt["leave_in"].apply(pd.Timedelta.total_seconds) >= 0)
            ][:2].itertuples()
        )
        logger.info("Trips: %s", trips)
        if len(trips) == 0:
            pass
        elif len(trips) == 1:
            tt.loc[pd.Index([trips[0].Index]), "next"] = True
            tt.loc[pd.Index([trips[0].Index]), "last"] = True
        elif len(trips) >= 2:
            logger.info("Trip 2+ at index: %s", pd.Index([trips[0].Index]))
            tt.loc[pd.Index([trips[0].Index]), "next"] = True
    tt = tt[tt["next"]]
    tt["leave_in"] = tt["leave_in"].apply(
        lambda dt: dt - pd.Timedelta(seconds=dt.seconds % 60)
    )
    logger.info("Next trips leave: %s", tt)
    logger.info("Next trips next: %s", tt["next"])
    return tt


def render(html, term, routes, nexts, now, warnings):
    # html.write('<link rel="stylesheet" href="style.css" />\n')
    # inline eliminates load flicker
    html.write("<style>\n")
    html.write(open("style.css").read())
    html.write("</style>\n")
    term.write(curses.tparm(curses.tigetstr("cup"), 0, 0))
    term.write(curses.tparm(curses.tigetstr("ed"), 2))

    def term_write(s):
        term.write(s.encode("utf-8"))

    print(routes)
    evenodd = itertools.cycle(["even", "odd"])
    trip_index = 0
    for (route_id, _, trip_label), _ in routes.iterrows():
        print(f"= {trip_label} =")
        rt = nexts[
            (nexts["trip_label"] == trip_label) & (nexts["departure_time_dt"] >= now)
        ][:2]
        rt = list(rt.itertuples())
        classes = ["route"]
        if len(rt) == 0:
            classes.append("finished")
            term.write(curses.tparm(curses.tigetstr("setab"), curses.COLOR_WHITE))
        if route_id == "2":
            classes.append("orange-line")
            # https://en.wikipedia.org/wiki/ANSI_escape_code#8-bit
            term.write(curses.tparm(curses.tigetstr("setab"), 214))
            term.write(curses.tparm(curses.tigetstr("setaf"), curses.COLOR_BLACK))
        classes.append(next(evenodd))
        if rt and route_id != "2":
            bg = curses.COLOR_BLUE if "even" in classes else 87
            term.write(curses.tparm(curses.tigetstr("setab"), bg))
            fg = curses.COLOR_WHITE if "even" in classes else curses.COLOR_BLACK
            term.write(curses.tparm(curses.tigetstr("setaf"), fg))
        html.write(f'<div class="{" ".join(classes)}">\n')
        strikethrough = ""
        if not rt:
            strikethrough = 'style="text-decoration: line-through;"'
        html.write(f'  <div class="label" {strikethrough}>{trip_label}</div>\n')
        term_write(f"{trip_label:15.15} ")
        print(rt)
        # The following code includes creative contributions from Claude, a generative AI system.
        # https://declare-ai.org/1.0.0/total.html
        if rt:
            (r,) = rt  # assert len 1
            total_seconds = int(r.leave_in.total_seconds())
            if total_seconds < 60:
                delta_display = "Now"
                term_display = "Now"
            elif total_seconds < 3600:
                delta_minutes = total_seconds // 60
                delta_display = f"{delta_minutes} min"
                term_display = f"{delta_minutes:4} min"
            else:
                delta_hours = total_seconds // 3600
                delta_minutes = (total_seconds % 3600) // 60
                delta_display = f"{delta_hours} hr {delta_minutes} min"
                term_display = f"{delta_hours} hr {delta_minutes} min"
            html.write(f"<!-- {r} -->\n")
            html.write(
                f'  <div class="trip"><span class="countdown" data-trip-index="{trip_index}" data-trip-seconds="{total_seconds}">{delta_display}</span>'
            )
            term_write(term_display + " ")
            if r.realtime:
                html.write('    <img class="realtime" src="realtime.png"/>')
            term_write(f'{"📡" if r.realtime else "  "} ')
            if r.last:
                html.write('    <span class="last">LAST</span>')
                term_write('{"LAST" if r.last else "":4} ')
            html.write("  </div>\n")
            trip_index += 1
        else:
            html.write('<div class="trip"></div>\n')
        html.write("</div>\n")

        term.write(curses.tparm(curses.tigetstr("setab"), 0))
        term.write(curses.tparm(curses.tigetstr("sgr"), 0))
        term_write("\n")
    html.write("<div>Times include walking time to the stop.</div>\n")
    html.write(
        f'<div>Last updated: <span class="last-updated-time">{now.strftime("%x %H:%M")}</span><br/><span class="last-updated-relative">00:00 ago</span></div>\n'
    )
    # end of partially AI generated code.
    term_write(f"Last updated: {now}\n")
    html.write("<div>Warnings: ")
    term_write("Warnings: ")
    if not warnings:
        html.write("none")
        term_write("none")
    else:
        html.write(" ".join(warnings))
        term_write(" ".join(warnings))
    html.write("</div>")
    term_write("\n")


# https://stackoverflow.com/a/65656371/2793863
def sleepUntil(hour, minute):
    t = datetime.datetime.today()
    future = datetime.datetime(t.year, t.month, t.day, hour, minute)
    if t.timestamp() > future.timestamp():
        future += datetime.timedelta(days=1)
    time.sleep((future - t).total_seconds())


if __name__ == "__main__":
    curses.setupterm(term="xterm-256color")

    g_lock = threading.Lock()

    download()
    build_stop_timetable((datetime.datetime.now() - datetime.timedelta(hours=5)).date())
    g_tt = load_pickle()

    def _build_thread():
        global g_tt
        try:
            while True:
                sleepUntil(6, 0)
                download()
                build_stop_timetable(
                    (datetime.datetime.now() - datetime.timedelta(hours=5)).date()
                )
                with g_lock:
                    g_tt = load_pickle()
        except Exception:
            traceback.print_exc()
            os.abort()

    th = threading.Thread(target=_build_thread, name="build thread")
    th.daemon = True
    th.start()

    class RequestHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path in ["/", "/realtime.png"]:
                self.send_response(200)
                path = path.lstrip("/")
                path = path if path else "index.html"
                with open(path, "rb") as f:
                    data = f.read()
                self.send_header("Connection", "keep-alive")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            elif path in ["/loading.html"]:
                self.send_response(200)
                data = "Loading...".encode("utf-8")
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Connection", "keep-alive")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            elif path in ["/schedule.html", "/schedule.txt"]:
                self.send_response(200)
                now = datetime.datetime.now()
                warnings = []
                with g_lock:
                    routes, tt = decorate_timetable(g_tt, now)
                tt["realtime"] = False
                try:
                    tt = apply_realtime(tt, now)
                except Exception as e:
                    warnings.append("Error applying realtime: " + str(e))
                nexts = next_trips(routes, tt, now)
                html = io.StringIO()
                term = io.BytesIO()
                render(html, term, routes, nexts, now, warnings)
                if path.endswith(".html"):
                    data = html.getvalue().encode("utf-8")
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Connection", "keep-alive")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                elif path.endswith(".txt"):
                    data = term.getvalue()
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.send_header("Connection", "keep-alive")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                else:
                    assert False
            else:
                self.send_response(404)
                self.send_header("Connection", "keep-alive")
                self.send_header("Content-Length", "0")
                self.end_headers()

    server = http.server.ThreadingHTTPServer(("", 8000), RequestHandler)
    logger.info("Server started")
    server.serve_forever()
