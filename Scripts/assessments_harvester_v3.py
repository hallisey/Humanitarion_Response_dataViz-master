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
        #defining the main level variables that will be pulled into CartoDB
        id = i["id"]
        label = i["label"]
        
        #variables for nested bundles JSON
        cluster_api = i["bundles"][0]["self"]
        cluster_id = i["bundles"][0]["id"]
        cluster_label = i["bundles"][0]["label"]
        #checks connection and loads the json
        openbundles = urllib2.urlopen(cluster_api)
        if (openbundles.getcode() == 200):
            bundles_data = openbundles.read()
            load_bundles = json.loads(bundles_data)
            
            #defining the variables to be inserted into cartodb from the nested JSON
            bundles_email = load_bundles["data"][0]["email"]
            bundles_global_cluster_id = load_bundles["data"][0]["global_cluster"]["id"]
            bundles_global_cluster_label = load_bundles["data"][0]["global_cluster"]["label"]
            bundles_leadagency_id = load_bundles["data"][0]["lead_agencies"][0]["id"]
            bundles_leadagency_label = load_bundles["data"][0]["lead_agencies"][0]["label"]
            bundles_partners_id = load_bundles["data"][0]["partners"][0]["id"]
            bundles_partners_label = load_bundles["data"][0]["partners"][0]["label"]
            cluster_coordinators = load_bundles["data"][0]["cluster_coordinators"]
            hid_access = load_bundles["data"][0]["hid_access"]
        
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
            
        other_location = i["other_location"]
        subject = i["subject"]
        methodology = i["methodology"]
        key_findings = i["key_findings"]
       
        date = i["date"]
        date_start = i["date"]["from"]
        date_end = i["date"]["to"]
        date_timezone = i["date"]["timezone"]
        
        frequency = i["frequency"]
        status = i["status"]
        
        report = i["report"]
        report_access = i["report"]["accessibility"]
        if "file" in report:
            report_id = report["file"]["fid"]
            report_filename = report["file"]["filename"]
            report_filesize = report["file"]["filesize"]
            report_url = report["file"]["url"]
        else:
            report_id = "null"
            report_filename = "null"
            report_url = "null"
        #report_instructions = i["report"]["instructions"]
        
        questionnaire = i["questionnaire"]
        questionnaire_access = i["questionnaire"]["accessibility"]
        if "file" in questionnaire:
            questionnaire_id = questionnaire["file"]["fid"]
            questionnaire_filename = questionnaire["file"]["filename"]
            questionnaire_filesize = questionnaire["file"]["filesize"]
            questionaire_url = questionnaire["file"]["url"]
        else:
            questionnaire_id = "null"
            questionnaire_filename = "null"
            questionnaire_filesize = "null"
            questionaire_url = "null"
        #questionnaire_instructions = i["questionnaire"]["instructions"]
        
        data_upload = i["data"]
        data_upload_access = i["data"]["accessibility"]
        if "file" in data_upload:
            data_upload_id = data_upload["file"]["fid"]
            data_upload_filename = data_upload["file"]["filename"]
            data_upload_filesize = data_upload["file"]["filesize"]
            data_upload_url = data_upload["file"]["url"]
        else:
            data_upload_id = "null"
            data_upload_filename = "null"
            data_upload_filesize = "null"
            data_upload_url = "null"
        #data_upload_instructions = i["data"]["instructions"]
        
        themes = i["themes"]
        disasters = i["disasters"]
            
        
        
        
        sql_query = "ID: " + str(id) + " Label: " + str(label) + " ClusterID: " + str(cluster_id) + " ClusterLabel: " + str(cluster_label) + " ClusterCoordinators: " + str(cluster_coordinators) + " ClusterEmail: " + str(bundles_email) + " Global ClusterID: " + str(bundles_global_cluster_id) + " Global Cluster Label: " + str(bundles_global_cluster_label) + " Cluster Lead Agency ID: " + str(bundles_leadagency_id) + " Cluster Lead Agency Label: " + str(bundles_leadagency_label)  + " HID Access: " + str(hid_access) + " DateStart: " + str(date_start) + " DateEnd: " + str(date_end) + " DateTimeZone: " + str(date_timezone)
        print str(sql_query)
#        try:
#            sql_query = "INSERT INTO humanitarian_response (the_geom, id, label, cluster_id, cluster_label, location_id, location_label, geoid, geo_pcode, geo_iso_code, subject) VALUES ("
#            sql_query = sql_query + "'SRID=4326; POINT (%f %f)', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'" % (float(str(long)), float(str(lat)), id, label, cluster_id, cluster_label, location_id, location_label, geoid, geo_pcode, geo_iso_code, subject)
#            sql_query = sql_query + ")"
#            print str(sql_query)
#        except ValueError,e:
#            print ("some error ocurred", e)
        
        #Call insert_into_cartodb()
#        insert_into_cartodb(sql_query)
    else:
        print "Received an error from server, cannot retrieve results " + str(webUrl.getcode())

if __name__ == "__main__":
  main()
  try:
    cl.sql('DELETE FROM humanitarian_response WHERE cartodb_id NOT IN (SELECT MIN(cartodb_id) FROM humanitarian_response GROUP BY id)')
  except CartoDBException as e:
    print ("some error ocurred", e)