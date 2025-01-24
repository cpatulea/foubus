#!venv/bin/python3
"""
A script that downloads STM GTFS data, builds a stop timetable,
applies real-time updates from STM, and serves an HTML page
showing next departure times (including walking times).
"""

import datetime
import glob
import http.server
import io
import itertools
import logging
import os
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
from urllib3.exceptions import HTTPError
from google.protobuf import text_format
from google.transit import gtfs_realtime_pb2


LOG_FORMAT = (
    "%(asctime)s [%(filename)s:%(lineno)d] [%(name)s] "
    "[%(threadName)s] %(levelname)s: %(message)s"
)
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=LOG_FORMAT)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

httppool = urllib3.PoolManager()

# This will be updated once a day after a successful revalidation of the GTFS feed.
revalidated = datetime.datetime.min

# Dictionary of stops and their walking times (in minutes) from Foulab
STOPS = {
    # https://openbusmap.org/#-73.5873;45.4784;17
    "Saint-Antoine / Saint-Ferdinand": 3,
    "Saint-Ferdinand / Saint-Antoine": 4,
    "Station Place-Saint-Henri": 7,
    "Station Place-Saint-Henri / Saint-Ferdinand": 7,
    "Notre-Dame / Place Saint-Henri": 8,
}


def download():
    """
    Download the latest STM GTFS feed if 24 hours have passed since the last
    download. Only re-download if it has changed (HTTP 200). If not, skip (HTTP 304).
    """
    global revalidated

    # Check if at least 24 hours have passed since last revalidation, then also
    # only revalidate after 3 AM of that next day.
    revalidation_time = (revalidated + datetime.timedelta(hours=24)).replace(hour=3)
    if datetime.datetime.now() < revalidation_time:
        return

    logging.info("Revalidating (last at %s)", revalidated)
    url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"

    # Build headers for conditional GET requests
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

    logging.info("If-Modified-Since: %s", headers.get("If-Modified-Since"))

    try:
        resp = httppool.request(
            "GET", url, headers=headers, timeout=3600.0, preload_content=False
        )
    except HTTPError as exc:
        logging.error("Download failed: %s", exc)
        return

    logging.info(
        "Response: %s %s (headers: %s)",
        resp.status,
        resp.reason,
        resp.headers,
    )

    # Not modified
    if resp.status == 304:
        revalidated = datetime.datetime.now()
        resp.release_conn()
        return

    # New file available
    if resp.status == 200:
        last_modified = time.mktime(
            time.strptime(resp.headers["Last-Modified"], "%a, %d %b %Y %H:%M:%S GMT")
        )

        with tempfile.NamedTemporaryFile(
            dir=".", prefix=os.path.basename(url) + "-", delete=False
        ) as tmp_file:
            logging.info("Downloading to %s", tmp_file.name)
            while chunk := resp.read(1024 * 1024):
                tmp_file.write(chunk)

        resp.release_conn()

        # Update mtime of new file
        os.utime(tmp_file.name, (last_modified, last_modified))
        os.rename(tmp_file.name, os.path.basename(url))
        logging.info("Saved to %s", os.path.basename(url))

        revalidated = datetime.datetime.now()
    else:
        resp.release_conn()
        raise ValueError(f"Unexpected status: {resp.status}")


def build_stop_timetable(req_date: datetime.date) -> None:
    """
    Builds the stop timetable for all stops listed in STOPS for the given date.
    The result is saved to a 'stop_timetable/' directory.
    """
    logging.info("Reading feed...")
    feed = gtfs_kit.read_feed("gtfs_stm.zip", dist_units="m")
    logging.info("Feed loaded")

    with tempfile.TemporaryDirectory(dir=".", prefix="stop_timetable-") as temp_dir:
        stops_df = feed.stops
        target_stops_df = stops_df[stops_df["stop_name"].isin(STOPS)]

        for _, stop_row in target_stops_df.iterrows():
            stop_id = stop_row["stop_id"]
            stop_name = stop_row["stop_name"]

            # Build the timetable for the requested date (format: YYYYMMDD)
            timetable = feed.build_stop_timetable(stop_id, [req_date.strftime("%Y%m%d")])
            timetable["stop_name"] = [stop_name] * len(timetable)

            # Save in multiple formats
            base_path = os.path.join(temp_dir, f"stop-{stop_id}")
            with open(f"{base_path}.txt", "w") as f_txt:
                f_txt.write(str(timetable))
            timetable.to_csv(f"{base_path}.csv")
            timetable.to_json(f"{base_path}.json")
            timetable.to_pickle(f"{base_path}.pickle")
            timetable.to_html(f"{base_path}.html")

            logging.info("Built stop %s (%s)", stop_id, stop_name)

        # Replace old directory with the newly built timetable
        try:
            shutil.rmtree("stop_timetable/")
        except FileNotFoundError:
            pass

        os.rename(temp_dir, "stop_timetable")


def load_pickle() -> pd.DataFrame:
    """
    Load all pickle files from the 'stop_timetable/' directory and
    concatenate them into a single DataFrame.
    """
    timetable_dataframes = []
    for path in glob.glob("stop_timetable/*.pickle"):
        with open(path, "rb") as pickled:
            timetable_dataframes.append(pickle.load(pickled))

    return pd.concat(timetable_dataframes).reset_index(drop=True)


def decorate_timetable(tt: pd.DataFrame, now: datetime.datetime):
    """
    Perform post-processing on the timetable:
      - Exclude certain stops (e.g., special filtering for route 17).
      - Convert route_id to int for sorting.
      - Create a human-readable 'trip_label'.
      - Calculate departure_time_dt as a datetime.
      - Calculate leave_in as a timedelta from 'now'.

    Returns:
      routes: Unique routes sorted by route_id_int, plus the trip_label.
      tt: Updated timetable with new columns (trip_label, departure_time_dt, leave_in).
    """
    # Exclude 17 Nord at stop 51986 in favor of a closer stop
    tt_filtered = tt[
        ~(
            (tt["route_id"] == "17")
            & (tt["trip_headsign"] == "Nord")
            & (tt["stop_id"] == "51986")
        )
    ].copy()

    tt_filtered["route_id_int"] = tt_filtered["route_id"].apply(int)

    def _trip_label(row):
        headsign = row["trip_headsign"]
        if headsign in ("Station Henri-Bourassa", "Station Montmorency -Zone B"):
            return "Montmorency"
        elif headsign == "Station Côte-Vertu":
            return "Côte-Vertu"
        else:
            return f"{row['route_id']} {row['trip_headsign']}"

    tt_filtered["trip_label"] = tt_filtered.apply(_trip_label, axis=1)

    def _departure_time_dt(row):
        isodate = f"{row['date'][0:4]}-{row['date'][4:6]}-{row['date'][6:8]}"
        noon = datetime.datetime.combine(
            datetime.date.fromisoformat(isodate), datetime.time(12, 0, 0)
        )
        h, m, s = map(int, row["departure_time"].split(":"))
        return noon - datetime.timedelta(hours=12) + datetime.timedelta(
            hours=h, minutes=m, seconds=s
        )

    tt_filtered["departure_time_dt"] = tt_filtered.apply(_departure_time_dt, axis=1)
    tt_filtered["leave_in"] = tt_filtered["departure_time_dt"] - now

    routes = (
        tt_filtered[["route_id", "route_id_int", "trip_label"]]
        .value_counts()
        .to_frame(name="count")
        .sort_values(["route_id_int", "trip_label"])
    )

    return routes, tt_filtered


def apply_realtime(tt: pd.DataFrame, now: datetime.datetime,
                   url: str = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
                   ) -> pd.DataFrame:
    """
    Apply real-time trip updates to the timetable DataFrame.
    If a trip is found in the real-time feed, update 'leave_in' based
    on the real-time departure time. Mark such rows as 'realtime=True'.

    Returns the updated timetable.
    """
    try:
        with open("stm-apikey.txt") as key_file:
            api_key = key_file.read().strip()
    except FileNotFoundError:
        logging.warning("stm-apikey.txt not found. Realtime will not be applied.")
        return tt

    try:
        resp = httppool.request(
            "GET",
            url,
            headers={"Apikey": api_key},
            timeout=10.0,
        )
    except HTTPError as exc:
        logging.error("Error fetching realtime data: %s", exc)
        return tt

    logging.info(
        "Response: %s %s (headers: %s, size: %d)",
        resp.status,
        resp.reason,
        resp.headers,
        len(resp.data),
    )

    fm = gtfs_realtime_pb2.FeedMessage.FromString(resp.data)
    with open("tripUpdates.textproto", "w") as f:
        f.write(str(fm))

    feed_age = (datetime.datetime.now() - datetime.datetime.fromtimestamp(fm.header.timestamp)).total_seconds()
    logging.info(
        "TripUpdates header: %s (timestamp %s, age %d seconds)",
        text_format.MessageToString(fm.header, as_one_line=True),
        datetime.datetime.fromtimestamp(fm.header.timestamp),
        feed_age,
    )
    logging.info(
        "TripUpdates: %d entity, %d stop_time_update",
        len(fm.entity),
        sum(len(e.trip_update.stop_time_update) for e in fm.entity),
    )

    tt["realtime"] = False
    updates_count = 0

    for entity in fm.entity:
        if not entity.trip_update.trip.trip_id:
            continue

        trip_id = entity.trip_update.trip.trip_id
        trip_date = entity.trip_update.trip.start_date

        if (tt["trip_id"] == trip_id).any():
            logging.info(
                "trip_update for %s: %s: %d stop_time_update",
                trip_id,
                text_format.MessageToString(entity.trip_update.trip, as_one_line=True),
                len(entity.trip_update.stop_time_update),
            )
            last_stop_sequence = None

            for stu in entity.trip_update.stop_time_update:
                # Validate consecutive stop sequences
                if last_stop_sequence is not None and last_stop_sequence + 1 != stu.stop_sequence:
                    logging.warning(
                        "Non-consecutive stop sequence: %s",
                        text_format.MessageToString(stu, as_one_line=True),
                    )
                last_stop_sequence = stu.stop_sequence

                # Only apply updates for scheduled stops
                if stu.schedule_relationship != stu.ScheduleRelationship.SCHEDULED:
                    continue

                mask = (
                    (tt["trip_id"] == trip_id)
                    & (tt["date"] == trip_date)
                    & (tt["stop_sequence"] == stu.stop_sequence)
                    & (tt["stop_id"] == stu.stop_id)
                )
                row_df = tt[mask]
                if row_df.empty:
                    continue

                if not stu.departure.time:
                    logging.warning(
                        "No departure time for trip: %s, stop_time_update: %s",
                        text_format.MessageToString(entity.trip_update.trip, as_one_line=True),
                        text_format.MessageToString(stu, as_one_line=True),
                    )
                    continue

                departure_dt = datetime.datetime.fromtimestamp(stu.departure.time)
                tt.loc[mask, ["realtime", "leave_in"]] = [
                    True,
                    departure_dt - now,
                ]
                updates_count += 1

    logging.info("TripUpdates applied to %d rows", updates_count)
    return tt


def next_trips(routes: pd.DataFrame, tt: pd.DataFrame,
               now: datetime.datetime) -> pd.DataFrame:
    """
    For each route in 'routes', identify the next upcoming trip(s) from the
    timetable 'tt' that departs after 'now'. Mark those in the DataFrame as 'next = True'.
    Also round 'leave_in' down to the nearest minute.

    Returns a DataFrame subset containing only the upcoming 'next' trips.
    """
    tt["next"] = False
    tt["last"] = False

    # Apply walking time: if 'leave_in' < walking time, we can't catch it.
    def _add_walking_time(row):
        walking_minutes = STOPS[row["stop_name"]]
        return row["leave_in"] - pd.Timedelta(minutes=walking_minutes)

    tt["leave_in"] = tt.apply(_add_walking_time, axis=1)

    # For each route, pick up to two upcoming trips
    for (route_id, _, trip_label), _ in routes.iterrows():
        route_filter = (tt["trip_label"] == trip_label) & (tt["leave_in"] >= pd.Timedelta(0))
        upcoming = tt[route_filter].nlargest(2, "departure_time_dt")

        # Because we used nlargest, we need to reverse so the earliest is first
        upcoming_sorted = upcoming.sort_values("departure_time_dt", ascending=True)
        upcoming_trips = list(upcoming_sorted.itertuples())

        if not upcoming_trips:
            continue

        # Mark first trip as 'next'. If there's only one, it's also 'last'.
        first_trip_index = upcoming_trips[0].Index
        tt.at[first_trip_index, "next"] = True
        if len(upcoming_trips) == 1:
            tt.at[first_trip_index, "last"] = True

    # Keep only 'next' trips
    next_df = tt[tt["next"]].copy()

    # Round 'leave_in' to full minutes
    next_df["leave_in"] = next_df["leave_in"].apply(
        lambda x: x - pd.Timedelta(seconds=x.seconds % 60)
    )

    logging.info("Next trips:\n%s", next_df)
    return next_df


def render(output: io.StringIO,
           routes: pd.DataFrame,
           nexts: pd.DataFrame,
           now: datetime.datetime,
           warnings: list) -> None:
    """
    Render an HTML schedule of the next trips into the 'output' stream.
    Include walking time, real-time indicators, and warnings if any.
    """
    # Inline style eliminates flicker when loading external CSS
    output.write("<style>\n")
    with open("style.css", "r") as css_file:
        output.write(css_file.read())
    output.write("</style>\n")

    even_odd = itertools.cycle(["even", "odd"])

    for (route_id, _, trip_label), _ in routes.iterrows():
        # Get up to two upcoming departures
        route_trips = nexts[
            (nexts["trip_label"] == trip_label) & (nexts["departure_time_dt"] >= now)
        ][:2]
        route_trips = list(route_trips.itertuples())

        classes = ["route"]
        if not route_trips:
            classes.append("finished")
        if route_id == "2":
            classes.append("orange-line")
        classes.append(next(even_odd))

        output.write(f'<div class="{" ".join(classes)}">\n')
        output.write(f'  <div class="label">{trip_label}</div>\n')

        if route_trips:
            # We only display the very first upcoming trip
            current_trip = route_trips[0]
            delta_minutes = int(current_trip.leave_in.total_seconds()) // 60

            output.write(f"  <!-- {current_trip} -->\n")
            output.write(f'  <div class="trip">{delta_minutes} min')

            if current_trip.realtime:
                output.write('    <img class="realtime" src="realtime.png"/>')
            if current_trip.last:
                output.write('    <span class="last">LAST</span>')

            output.write("  </div>\n")
        else:
            output.write('  <div class="trip"></div>\n')

        output.write("</div>\n")

    output.write("<div>Times include walking time to the stop.</div>\n")
    output.write(f"<div>Last updated: {now}</div>\n")

    output.write("<div>Warnings: ")
    if not warnings:
        output.write("none")
    else:
        output.write(" ".join(warnings))
    output.write("</div>")


def sleep_until(hour: int, minute: int) -> None:
    """
    Block the current thread until the specified hour and minute (local time).
    If that time has already passed today, wait until tomorrow at that time.
    """
    now = datetime.datetime.today()
    target = datetime.datetime(now.year, now.month, now.day, hour, minute)
    if now.timestamp() > target.timestamp():
        target += datetime.timedelta(days=1)
    time.sleep((target - now).total_seconds())


def main():
    """
    Main entry point:
      - Download the GTFS feed if needed.
      - Build the stop timetable for "today minus 5 hours" to account for late-night schedules.
      - Start a background thread to rebuild the timetable daily at 6am.
      - Start an HTTP server to serve schedule data.
    """

    timetable_lock = threading.Lock()

    # Initial setup
    download()
    today_minus_5 = (datetime.datetime.now() - datetime.timedelta(hours=5)).date()
    build_stop_timetable(today_minus_5)
    global_tt = load_pickle()

    def background_build_thread():
        """
        A loop that, every day at 06:00, re-downloads the feed and rebuilds the stop timetable.
        """
        nonlocal global_tt
        try:
            while True:
                sleep_until(6, 0)
                download()
                today_minus_5 = (datetime.datetime.now() - datetime.timedelta(hours=5)).date()
                build_stop_timetable(today_minus_5)
                with timetable_lock:
                    global_tt = load_pickle()
        except Exception:
            logging.exception("Build thread encountered an error:")
            # In case of error, abort the process (or handle gracefully)
            os._exit(1)

    th = threading.Thread(target=background_build_thread, name="BuildThread", daemon=True)
    th.start()

    class RequestHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path

            if path in ["/", "/realtime.png"]:
                # Serve index.html or realtime.png from the current dir
                self.serve_static(path.lstrip("/") or "index.html")

            elif path == "/loading.html":
                self.send_response(200)
                data = "Loading...".encode("utf-8")
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Connection", "keep-alive")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            elif path == "/schedule.html":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Connection", "keep-alive")

                now = datetime.datetime.now()
                local_warnings = []

                with timetable_lock:
                    routes, tt_dec = decorate_timetable(global_tt, now)

                try:
                    tt_rt = apply_realtime(tt_dec, now)
                except Exception as exc:
                    local_warnings = [f"Error applying realtime: {str(exc)}"]
                    tt_rt = tt_dec

                next_departures = next_trips(routes, tt_rt, now)
                buffer = io.StringIO()
                render(buffer, routes, next_departures, now, local_warnings)

                data = buffer.getvalue().encode("utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            else:
                self.send_response(404)
                self.send_header("Connection", "keep-alive")
                self.send_header("Content-Length", "0")
                self.end_headers()

        def serve_static(self, filename: str):
            """
            Serve a static file from the current directory.
            """
            if not os.path.exists(filename):
                self.send_error(404, "File not found.")
                return

            self.send_response(200)
            if filename.endswith(".png"):
                self.send_header("Content-Type", "image/png")
            elif filename.endswith(".html"):
                self.send_header("Content-Type", "text/html; charset=utf-8")
            else:
                self.send_header("Content-Type", "application/octet-stream")

            with open(filename, "rb") as f:
                data = f.read()
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = http.server.ThreadingHTTPServer(("", 8000), RequestHandler)
    logging.info("Server started on port 8000")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down server.")
        server.server_close()


if __name__ == "__main__":
    main()
