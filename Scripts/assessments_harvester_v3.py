#import required modules
import exceptions
import urllib2, json
from cartodb import CartoDBAPIKey, CartoDBException

#cartodb variables
api_key = 'ebd93f0d0daf2ab7d2b31e2449e307cfe0744252'
cartodb_domain = 'troy'
cl = CartoDBAPIKey(api_key, cartodb_domain)

# function for inserting into cartodb
def insert_into_cartodb(sql_query):
    try:
       print cl.sql(sql_query)
    except CartoDBException as e:
       print ("some error ocurred", e)
       
#def get_max_id():
#    return cl.sql('SELECT MAX(id) FROM humanitarian_response')['rows'][0]['max']

#the main function
def main():
  # define a variable to hold the source URL
  urlData = "http://www.humanitarianresponse.info/api/v1.0/assessments"
  
  # Open the URL and read the data
  webUrl = urllib2.urlopen(urlData)
  if (webUrl.getcode() == 200):
    data = webUrl.read()
    
    #Use the json module to load the string data into a dictionary
    api_url = json.loads(data)
  
  for i in api_url["data"]:
    #defining the variables that will be pulled into CartoDB
    id = i["id"]
    label = i["label"]
    cluster_id = i["bundles"][0]["id"]
    cluster_label = i["bundles"][0]["label"]
    subject = i["subject"]
    methodology = i["methodology"]
    key_findings = i["key_findings"]
    other_location = i["other_location"]
    status = i["status"]
    report_id = i["report"]["file"]["fid"]
    
    #variablea for nested locations JSON
    location_api = i["locations"][0]["self"]
    location_id = i["locations"][0]["id"]
    location_label = i["locations"][0]["label"]
    
    #checks connection and loads the json
    openlocations = urllib2.urlopen(location_api)
    if (openlocations.getcode() == 200):
        location_data = openlocations.read()
        load_locations = json.loads(location_data)
        
        #defining the variable to be inserted into cartodb from the nested JSON
        geoid = load_locations["data"][0]["id"]
        geo_pcode = load_locations["data"][0]["pcode"]
        geo_iso_code = load_locations["data"][0]["iso3"]
        lat = load_locations["data"][0]["geolocation"]["lat"]
        long = load_locations["data"][0]["geolocation"]["lon"]
    #prints error mesage if connection fails   
    else:
        print "Received an error from the server and cannot retrieve the results" + str(openlocations.getcode())
    
    sql_query = "Report ID:" + str(report_id) + "\t" + "Regular ID:" + str(id) + "\t" + str(label) + "\t" + str(cluster_id) + "\t" + str(cluster_label) + "\t" + str(location_id) + "\t" + str(location_label) + "\t" + str(lat) + "\t" + str(long)  + "\t" + str(geoid) + "\t" + str(geo_pcode) + "\t" + str(geo_iso_code) + "\t" + str(subject)
    print str(sql_query)
    try:
        sql_query = "INSERT INTO humanitarian_response (the_geom, id, label, cluster_id, cluster_label, location_id, location_label, geoid, geo_pcode, geo_iso_code, subject) VALUES ("
        sql_query = sql_query + "'SRID=4326; POINT (%f %f)', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'" % (float(str(long)), float(str(lat)), id, label, cluster_id, cluster_label, location_id, location_label, geoid, geo_pcode, geo_iso_code, subject)
        sql_query = sql_query + ")"
        print str(sql_query)
    except ValueError,e:
        print ("some error ocurred", e)
    
    #Call insert_into_cartodb()
    insert_into_cartodb(sql_query)
  else:
    print "Received an error from server, cannot retrieve results " + str(webUrl.getcode())

if __name__ == "__main__":
  main()
  try:
    cl.sql('DELETE FROM humanitarian_response WHERE cartodb_id NOT IN (SELECT MIN(cartodb_id) FROM humanitarian_response GROUP BY id)')
  except CartoDBException as e:
    print ("some error ocurred", e)