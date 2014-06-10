import datetime
import json
import psycopg2

class postgeo(object):
  def __init__(self):
    self.conn = None
    self.cur = None

  def connect_pg(self, str_conn):
    try:
      self.conn = psycopg2.connect(str_conn)
      self.cur = self.conn.cursor()
    except:
      print "Unable to connect to the database. Please check your options and try again."
      return False
    return True

  def close_pg(self):
    if self.cur is not None:
      self.cur.close()

    if self.conn is not None:
      self.conn.close()

  # return type dict
  # input type: dict,file handle, filename
  def geojson2topojson(self, geojson):
    from topojson import topojson
    return topojson(geojson, None, quantization=1e6, simplify=0.0001)

  # return type dict
  # input type: dict,file handle, filename
  def topojson2geojson(self, topojson):
    from geojson import geojson
    geojson(topojson, input_name=None, out_geojson=None)

  # return type string
  def dumpjson(self, json, pretty=True):
    if pretty:
      jsonstr = json.dumps(json, indent=2)
    else:
      jsonstr = json.dumps(json)
    return jsonstr

  # return type dict
  def query(self, table, geom_field, where, fields=None, format='geojson'):
    if self.cur is not None:
      cur = self.cur
    else:
      return None

    query = "SELECT "

    if isinstance(fields, list):
      for field in fields:
        query += field + ", "
    else:
      query += "*, "

    query += "ST_AsGeoJSON(" + geom_field + ") AS geometry FROM " + table

    if where is not None:
      query += " WHERE " + where + ";"
    else:
      query += ";"

    # Execute the query
    try:
      cur.execute(query)
    except Exception as exc:
      print "Unable to execute query. Error was {0}".format(str(exc))
      return None

    # Retrieve the results of the query
    rows = cur.fetchall()

    # Get the column names returned
    colnames = [desc[0] for desc in cur.description]

    # Find the index of the column that holds the geometry
    geomIndex = colnames.index("geometry")
    feature_collection = {'type': 'FeatureCollection', 'features': []}

    for row in rows:
      feature = {
        'type': 'Feature',
        'geometry': json.loads(row[geomIndex]),
        'properties': {},
      }

      for index, colname in enumerate(colnames):
        if colname not in ('geometry', geom_field):
          if isinstance(row[index], datetime.datetime):
            # datetimes are not JSON.dumpable, manually stringify these.
            value = str(row[index])
          else:
            value = row[index]
          feature['properties'][colname] = value

      feature_collection['features'].append(feature)

    if format == 'topojson':
      return self.geojson2topjson(feature_collection)
    else:
      return feature_collection
