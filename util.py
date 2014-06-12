import argparse
from postgeo import PostGeo

parser = argparse.ArgumentParser(
  description="Create a GeoJSON from a PostGIS query.",
  epilog="Example usage: python postgis2geojson.py -d awesomeData -h localhost -u user -p securePassword -t table -f id name geom -w 'columnA = columnB' -o myData --topojson")

parser.add_argument("-d", "--database", dest="database",
  default="postgis", type=str,
  help="The database to connect to")

parser.add_argument("-H", "--host", dest="host",
  default="127.0.0.1", type=str,
  help="Database host. Defaults to 'localhost'")

parser.add_argument("-P", "--port", dest="port",
  default="5432", type=str,
  help="Database port. Defaults to 5432")

parser.add_argument("-u", "--user", dest="user",
  default="_postgres", type=str,
  help="Database user. Defaults to 'postgres'")

parser.add_argument("-p", "--password", dest="password",
  default="_postgres", type=str,
  help="Password for the database user")

parser.add_argument("-t", "--table", dest="table",
  default="panorama_line", type=str,
  help="Database table to query")

parser.add_argument("-f", "--fields", dest="fields",
  nargs="+",
  help="Fields to return separated by a single space. Defaults to *")

parser.add_argument("-g", "--geometry", dest="geometry",
  default="geom", type=str,
  help="Name of the geometry column. Defaults to 'geom'")

parser.add_argument("-w", "--where", dest="where",
  type=str,
  help="Optional WHERE clause to add to the SQL query")

parser.add_argument("-o", "--output", dest="file",
  default="data", type=str,
  help="Output file name without extension. Defaults to 'data.geojson'")

parser.add_argument("--topojson", dest="topojson",
  default=False, action="store_true",
  help="Use if you would also like a copy of the data as TopoJSON")

parser.add_argument("--pretty", dest="pretty",
  default=False, action="store_true",
  help="Pretty print the output (indent).")

arguments = parser.parse_args()

def main():

  postgeo = PostGeo()

  connection = "dbname=" + arguments.database + " host=" + arguments.host + " port=" + arguments.port + " user=" + arguments.user + " password="+ arguments.password
  connection = "dbname='postgis' host='127.0.0.1' port='5432' user='_postgres' password='_postgres'"
  try:
    postgeo.connect(connection)
  except:
    print "Unable to connect to the database. Please check your options and try again."
    return

  json = postgeo.query(arguments.table, arguments.geometry, arguments.where, arguments.fields)

  if arguments.topojson:
    json = postgeo.geojson2topojson(json)

  postgeo.dumpjson(json, arguments.file+'.geojson', pretty=arguments.pretty)

  postgeo.close()

if __name__ == '__main__':
  main()
