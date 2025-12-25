#!venv/bin/python3
import curses
import datetime
import glob
import http.server
import io
import logging
import os
import pickle
import shutil
import tempfile
import threading
import unittest

import pandas as pd

import foubus

pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)


class FoubusTest(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp(prefix="foubus_test-")
        self.addCleanup(lambda d=d: shutil.rmtree(d))
        os.symlink(os.path.relpath("testdata/", d), f"{d}/testdata")
        os.symlink(os.path.relpath("style.css", d), f"{d}/style.css")
        os.chdir(d)
        # Exercise the download code, but then immediately replace with a
        # snapshot which matches the dates hardcoded in this test.
        foubus.download()
        shutil.copy('testdata/gtfs_stm-2024-12-12.zip', 'gtfs_stm.zip')
        foubus.build_stop_timetable(datetime.date(2025, 1, 18))
        with open("stm-apikey.txt", "w"):
            pass
        curses.setupterm(term='xterm-256color')

    def testRealtime(self):
        """
        Collect realtime test data:
          for _ in $(seq 1 100); do curl -o tripUpdates-$(date +%Y%m%d-%H%M%S).pb https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates -H 'Apikey: '$(cat stm-apikey.txt)' ; sleep 1h; done

        Inspect:
          protoc --decode_raw < tripUpdates-20250118-162839.pb | grep '^      5: "' | egrep '"(17|35|36|190|371)"'
        """
        now = datetime.datetime.fromisoformat("2025-01-18T17:10:00")
        tt = foubus.load_pickle()
        isodate = (
            tt.iloc[0]["date"][0:4]
            + "-"
            + tt.iloc[0]["date"][4:6]
            + "-"
            + tt.iloc[0]["date"][6:8]
        )
        self.assertEqual(now.date(), datetime.date.fromisoformat(isodate))

        routes, tt = foubus.decorate_timetable(tt, now)

        class TestDataHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, request, client_address, server):
                super().__init__(request, client_address, server, directory="testdata/")

        hs = http.server.HTTPServer(("localhost", 9191), TestDataHTTPRequestHandler)
        hst = threading.Thread(name="HTTPServer", target=hs.serve_forever)
        hst.start()

        def _shutdown():
            hs.shutdown()
            hst.join()
            hs.server_close()

        self.addCleanup(_shutdown)

        tt["realtime"] = False
        tt = foubus.apply_realtime(
            tt, now, url="http://localhost:9191/tripUpdates-20250118-162839.pb"
        )
        tt = foubus.next_trips(routes, tt, now)
        logging.info("Nexts: %s", tt)

        warnings = []
        term = io.BytesIO()
        with open("schedule.html", "w") as f:
            foubus.render(f, term, routes, tt, now, warnings)
        self.assertEqual([], warnings)

        (t,) = tt[tt["trip_label"] == "Côte-Vertu"].itertuples()
        self.assertEqual(datetime.timedelta(minutes=6), t.leave_in)
        self.assertEqual(False, t.realtime)
        self.assertEqual(False, t.last)

        (t,) = tt[tt["trip_label"] == "35 Est"].itertuples()
        self.assertEqual(datetime.timedelta(minutes=1), t.leave_in)
        self.assertEqual(True, t.realtime)
        self.assertEqual(False, t.last)

        (t,) = tt[tt["trip_label"] == "35 Ouest"].itertuples()
        self.assertEqual(datetime.timedelta(minutes=10), t.leave_in)
        self.assertEqual(True, t.realtime)
        self.assertEqual(False, t.last)

        (t,) = tt[tt["trip_label"] == "36 Ouest"].itertuples()
        self.assertEqual(datetime.timedelta(minutes=14), t.leave_in)
        self.assertEqual(True, t.realtime)
        self.assertEqual(False, t.last)

        (t,) = tt[tt["trip_label"] == "371 Sud"].itertuples()
        self.assertEqual(datetime.timedelta(minutes=532), t.leave_in)
        self.assertEqual(False, t.realtime)
        self.assertEqual(False, t.last)


if __name__ == "__main__":
    unittest.main()
