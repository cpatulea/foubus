#!venv/bin/python3
import datetime, urllib3, urllib3.exceptions, time, os.path, logging, sys, tempfile, gtfs_kit, shutil, glob, pickle, itertools, http.server, io
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.protobuf import text_format
import urllib.parse, threading, traceback, os

LOG_FORMAT = '%(asctime)s [%(filename)s:%(lineno)d] [%(name)s] [%(threadName)s] %(levelname)s: %(message)s'
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format=LOG_FORMAT)
logging.getLogger("urllib3").setLevel(logging.DEBUG)

httppool = urllib3.PoolManager()
revalidated = datetime.datetime.min

STOPS = {
  # https://openbusmap.org/#-73.5873;45.4784;17
  # stop_name : Google Maps walking time from Foulab
  'Saint-Antoine / Saint-Ferdinand': 3,
  'Saint-Ferdinand / Saint-Antoine': 4,
  'Station Place-Saint-Henri': 7,
  'Station Place-Saint-Henri / Saint-Ferdinand': 7,
  'Notre-Dame / Place Saint-Henri': 8,
}

def download():
  global revalidated

  if datetime.datetime.now() >= (revalidated + datetime.timedelta(hours=24)).replace(hour=3):
    logging.info('Revalidating (last at %s)', revalidated)

    url = 'https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip'

    try:
      mtime = os.path.getmtime(os.path.basename(url))
    except FileNotFoundError:
      headers = {}
    else:
      headers = {
        'If-Modified-Since': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(mtime))
      }
    logging.info('If-Modified-Since: %s', headers.get('If-Modified-Since'))

    resp = httppool.request('GET', url, headers=headers, timeout=3600.0, preload_content=False)
    logging.info('Response: %s %s (headers: %s)', resp.status, resp.reason, resp.headers)

    if resp.status == 304:
      revalidated = datetime.datetime.now()
      return
    elif resp.status == 200:
      last_modified = time.mktime(time.strptime(resp.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S GMT'))

      with tempfile.NamedTemporaryFile(dir='.', prefix=os.path.basename(url) + '-', delete=False) as f:
        logging.info('Downloading to %s', f.name)
        while chunk := resp.read(1024 * 1024):
          f.write(chunk)
      
      resp.release_conn()
    
      os.utime(f.name, (last_modified, last_modified))
      os.rename(f.name, os.path.basename(url))
      logging.info('Saved to %s', os.path.basename(url))

      revalidated = datetime.datetime.now()
    else:
      raise ValueError(f'Unexpected status: {resp.status}')

def build_stop_timetable(date):
  """Run at 6am"""
  logging.info('Reading feed...')
  feed = gtfs_kit.read_feed('gtfs_stm.zip', dist_units='m')
  logging.info('Feed loaded')

  with tempfile.TemporaryDirectory(dir='.', prefix='stop_timetable-') as d:
    df = feed.stops
    for _, s in df[df['stop_name'].isin(STOPS)].iterrows():
        tt = feed.build_stop_timetable(s['stop_id'], [date.strftime('%Y%m%d')])
        tt['stop_name'] = [s['stop_name']] * len(tt)
        with open(f'{d}/stop-{s["stop_id"]}.txt', 'w') as f:
          f.write(str(tt))
        tt.to_csv(f'{d}/stop-{s["stop_id"]}.csv')
        tt.to_json(f'{d}/stop-{s["stop_id"]}.json')
        tt.to_pickle(f'{d}/stop-{s["stop_id"]}.pickle')
        tt.to_html(f'{d}/stop-{s["stop_id"]}.html')
        logging.info('Built stop %s (%s)', s["stop_id"], s["stop_name"])
    try:
      shutil.rmtree('stop_timetable/')
    except FileNotFoundError:
      pass
    os.rename(d, 'stop_timetable')

def load_pickle():
  tts = []
  for path in glob.glob('stop_timetable/*.pickle'):
    with open(path, 'rb') as p:
      tts.append(pickle.load(p))
  tt = pd.concat(tts).reset_index()
  return tt

def decorate_timetable(tt, now):
  # exclude 17 Nord at stop 51986 (Station Place-Saint-Henri / Saint-Ferdinand),
  # there's a closer stop at 51916
  tt = tt[~((tt['route_id'] == '17') & (tt['trip_headsign'] == 'Nord') & (tt['stop_id'] == '51986'))]

  # Avoid future SettingWithCopyWarning
  tt = tt.copy()

  tt['route_id_int'] = tt['route_id'].apply(int)
  def _trip_label(row):
    headsign = row['trip_headsign']
    if headsign == 'Station Henri-Bourassa':
      return 'Montmorency'
    elif headsign == 'Station Montmorency -Zone B':
      return 'Montmorency'
    elif headsign == 'Station Côte-Vertu':
      return 'Côte-Vertu'
    else:
      return row['route_id'] + ' ' + row['trip_headsign']
  tt['trip_label'] = tt.apply(_trip_label, axis=1)
  def _departure_time_dt(row):
    isodate = row['date'][0:4] + '-' + row['date'][4:6] + '-' + row['date'][6:8]
    noon = datetime.datetime.combine(datetime.date.fromisoformat(isodate), datetime.time(12, 0, 0))
    dep = row['departure_time']
    h, m, s = map(int, dep.split(':'))
    return noon - datetime.timedelta(hours=12) + datetime.timedelta(hours=h, minutes=m, seconds=s)
  tt['departure_time_dt'] = tt.apply(_departure_time_dt, axis=1)
  def _leave_in(row):
    return row['departure_time_dt'] - now
  tt['leave_in'] = tt.apply(_leave_in, axis=1)

  routes = tt[['route_id', 'route_id_int', 'trip_label']].value_counts()
  routes = pd.DataFrame(routes).sort_values(['route_id_int', 'trip_label'])

  return routes, tt

def apply_realtime(tt, now, url='https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates'):
  resp = httppool.request(
    'GET', url,
    headers={'Apikey': open('stm-apikey.txt').read().strip()},
    timeout=10.0)
  logging.info('Response: %s %s (headers: %s, size: %d)', resp.status, resp.reason, resp.headers, len(resp.data))
  fm = gtfs_realtime_pb2.FeedMessage.FromString(resp.data)
  with open('tripUpdates.textproto', 'w') as f:
    f.write(str(fm))
  logging.info('TripUpdates header: %s (timestamp %s, age %d seconds)',
               text_format.MessageToString(fm.header, as_one_line=True),
               datetime.datetime.fromtimestamp(fm.header.timestamp),
               (datetime.datetime.now() - datetime.datetime.fromtimestamp(fm.header.timestamp)).total_seconds())
  logging.info('TripUpdates: %d entity, %d stop_time_update',
               len(fm.entity),
               sum(len(e.trip_update.stop_time_update) for e in fm.entity))
  
  tt['realtime'] = [False] * len(tt)

  updates = 0
  for entity in fm.entity:
    assert entity.trip_update.trip.trip_id, str(entity)
    if (tt['trip_id'] == entity.trip_update.trip.trip_id).any():
      logging.info('trip_update for %s: %s: %d stop_time_update',
                   entity.trip_update.trip.trip_id,
                   text_format.MessageToString(entity.trip_update.trip, as_one_line=True),
                   len(entity.trip_update.stop_time_update))
      last_stop_sequence = None
      for stu in entity.trip_update.stop_time_update:
        # TODO: implement delay propagation
        # https://gtfs.org/documentation/realtime/feed-entities/trip-updates/#:~:text=If%20one%20or%20more%20stops%20are%20missing
        assert last_stop_sequence is None or last_stop_sequence + 1 == stu.stop_sequence, text_format.MessageToString(stu, as_one_line=True)

        if stu.schedule_relationship != gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.SCHEDULED:
          continue

        row = tt[(tt['trip_id'] == entity.trip_update.trip.trip_id) &
                 (tt['date'] == entity.trip_update.trip.start_date) &
                 (tt['stop_sequence'] == stu.stop_sequence) &
                 (tt['stop_id'] == stu.stop_id)]
        if not row.empty:
          assert len(row) == 1, row
          # print(row)
          # print(stu)
          if not stu.departure.time:
            logging.warning('No departure time: trip: %s stop_time_update: %s',
                        text_format.MessageToString(entity.trip_update.trip, as_one_line=True),
                        text_format.MessageToString(stu, as_one_line=True))
          else:
            # row.loc[:,'realtime'] = stu.departure.time
            tt.loc[(tt['trip_id'] == entity.trip_update.trip.trip_id) &
                 (tt['date'] == entity.trip_update.trip.start_date) &
                 (tt['stop_sequence'] == stu.stop_sequence) &
                 (tt['stop_id'] == stu.stop_id),
                 ['realtime', 'leave_in']] = [True, datetime.datetime.fromtimestamp(stu.departure.time) - now]
            print(row)
            updates += 1

  logging.info('TripUpdates for us: %d', updates)
  return tt

def next_trips(routes, tt, now):
  tt['next'] = len(tt) * [False]
  tt['last'] = len(tt) * [False]
  # add walking time before picking next (might be too late)
  def _add_walking_time(row):
    return row['leave_in'] - pd.Timedelta(minutes=STOPS[row['stop_name']])
  tt['leave_in'] = tt.apply(_add_walking_time, axis=1)
  for (route_id, _, trip_label), _ in routes.iterrows():
    logging.info('= %s =', trip_label)
    trips = list(tt[(tt['trip_label'] == trip_label) & (tt['leave_in'].apply(pd.Timedelta.total_seconds) >= 0)][:2].itertuples())
    logging.info('Trips: %s', trips)
    if len(trips) == 0:
      pass
    elif len(trips) == 1:
      tt.loc[pd.Index([trips[0].Index]), 'next'] = True
      tt.loc[pd.Index([trips[0].Index]), 'last'] = True
    elif len(trips) >= 2:
      logging.info('Trip 2+ at index: %s', pd.Index([trips[0].Index]))
      tt.loc[pd.Index([trips[0].Index]), 'next'] = True
  tt = tt[tt['next']]
  tt['leave_in'] = tt['leave_in'].apply(lambda dt: dt - pd.Timedelta(seconds=dt.seconds % 60))
  logging.info('Next trips leave: %s', tt)
  logging.info('Next trips next: %s', tt['next'])
  return tt

def render(f, routes, nexts, now, warnings):
  # f.write('<link rel="stylesheet" href="style.css" />\n')
  # inline eliminates load flicker
  f.write('<style>\n')
  f.write(open('style.css').read())
  f.write('</style>\n')
  print(routes)
  evenodd = itertools.cycle(['even', 'odd'])
  for (route_id, _, trip_label), _ in routes.iterrows():
    print(f'= {trip_label} =')
    rt = nexts[(nexts['trip_label'] == trip_label) & (nexts['departure_time_dt'] >= now)][:2]
    rt = list(rt.itertuples())
    classes = ['route']
    if len(rt) == 0:
      classes.append('finished')
    if route_id == '2':
      classes.append('orange-line')
    classes.append(next(evenodd))
    f.write(f'<div class="{" ".join(classes)}">\n')
    f.write(f'  <div class="label">{trip_label}</div>\n')
    print(rt)
    if rt:
      r, = rt  # assert len 1
      delta = int(r.leave_in.total_seconds())//60
      f.write(f'<!-- {r} -->\n')
      f.write(f'  <div class="trip">{delta} min')
      if r.realtime:
        f.write(f'    <img class="realtime" src="realtime.png"/>')
      if r.last:
        f.write(f'    <span class="last">LAST</span>')
      f.write(f'  </div>\n')
    else:
      f.write('<div class="trip"></div>\n')
    f.write('</div>\n')
  f.write(f'<div>Times include walking time to the stop.</div>\n')
  f.write(f'<div>Last updated: {now}</div>\n')
  f.write(f'<div>Warnings: ')
  if not warnings:
    f.write('none')
  else:
    f.write(' '.join(warnings))
  f.write('</div>')

# https://stackoverflow.com/a/65656371/2793863
def sleepUntil(hour, minute):
    t = datetime.datetime.today()
    future = datetime.datetime(t.year, t.month, t.day, hour, minute)
    if t.timestamp() > future.timestamp():
        future += datetime.timedelta(days=1)
    time.sleep((future-t).total_seconds())

if __name__ == '__main__':
  g_lock = threading.Lock()

  download()
  build_stop_timetable((datetime.datetime.now() - datetime.timedelta(hours=5)).date())
  g_tt = load_pickle()

  def _build_thread():
    try:
      while True:
        sleepUntil(6, 0)
        download()
        build_stop_timetable((datetime.datetime.now() - datetime.timedelta(hours=5)).date())
        with g_lock:
          g_tt = load_pickle()
    except:
      traceback.print_exc()
      os.abort()

  th = threading.Thread(target=_build_thread, name='build thread')
  th.daemon = True
  th.start()

  class RequestHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def do_GET(self):
      path = urllib.parse.urlparse(self.path).path
      if path in ['/', '/realtime.png']:
        self.send_response(200)
        path = path.lstrip('/')
        path = path if path else 'index.html'
        with open(path, 'rb') as f:
          data = f.read()
        self.send_header('Connection', 'keep-alive')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
      elif path in ['/loading.html']:
        self.send_response(200)
        data = 'Loading...'.encode('utf-8')
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
      elif path == '/schedule.html':
        self.send_response(200)
        now = datetime.datetime.now()
        warnings = []
        with g_lock:
          routes, tt = decorate_timetable(g_tt, now)
        try:
          tt = apply_realtime(tt, now)
        except Exception as e:
          warnings = ['Error applying realtime: ' + str(e)]
        nexts = next_trips(routes, tt, now)
        buffer = io.StringIO()
        render(buffer, routes, nexts, now, warnings)
        data = buffer.getvalue().encode('utf-8')
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
      else:
        self.send_response(404)
        self.send_header('Connection', 'keep-alive')
        self.send_header('Content-Length', '0')
        self.end_headers()

  server = http.server.ThreadingHTTPServer(('', 8000), RequestHandler)
  logging.info('Server started')
  server.serve_forever()
